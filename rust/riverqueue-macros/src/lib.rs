#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Fields, Lit, LitBool, LitInt, LitStr, Meta, Token, ext::IdentExt as _,
    parenthesized, parse_macro_input, punctuated::Punctuated,
};

/// Derives `riverqueue::JobArgs`.
///
/// The type must declare `#[river(kind = "...")]`. It may also declare
/// `aliases("old_kind")`, `queue = "..."`, `max_attempts = N`, `priority = N`,
/// and `pending = true`. Fields marked `#[river(unique)]` are used by
/// argument-scoped unique jobs. Their serialized names follow Serde's
/// serialization-side `rename` and `rename_all` settings. A unique field may
/// be conditionally omitted with `skip_serializing_if`, matching River Go, but
/// cannot be flattened or unconditionally skipped during serialization.
#[proc_macro_derive(JobArgs, attributes(river, serde))]
pub fn derive_job_args(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_job_args(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[allow(clippy::too_many_lines)]
fn expand_job_args(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let mut kind = None;
    let mut aliases = Vec::new();
    let mut max_attempts = None;
    let mut pending = None;
    let mut priority = None;
    let mut queue = None;
    let mut unique_paths = Vec::new();
    for attribute in &input.attrs {
        if attribute.path().is_ident("river") {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("kind") {
                    kind = Some(meta.value()?.parse::<LitStr>()?);
                    return Ok(());
                }
                if meta.path.is_ident("aliases") {
                    let content;
                    parenthesized!(content in meta.input);
                    aliases = Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?
                        .into_iter()
                        .collect();
                    return Ok(());
                }
                if meta.path.is_ident("max_attempts") {
                    max_attempts = Some(meta.value()?.parse::<LitInt>()?);
                    return Ok(());
                }
                if meta.path.is_ident("pending") {
                    pending = Some(meta.value()?.parse::<LitBool>()?);
                    return Ok(());
                }
                if meta.path.is_ident("priority") {
                    priority = Some(meta.value()?.parse::<LitInt>()?);
                    return Ok(());
                }
                if meta.path.is_ident("queue") {
                    queue = Some(meta.value()?.parse::<LitStr>()?);
                    return Ok(());
                }
                if meta.path.is_ident("unique") {
                    let content;
                    parenthesized!(content in meta.input);
                    unique_paths
                        .extend(Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?);
                    return Ok(());
                }
                Err(meta.error("unsupported river type attribute"))
            })?;
        }
    }
    let kind = kind.ok_or_else(|| {
        syn::Error::new_spanned(&input.ident, "JobArgs requires #[river(kind = \"...\")]")
    })?;
    validate_kind(&kind)?;
    for alias in &aliases {
        validate_kind(alias)?;
        if alias.value() == kind.value() {
            return Err(syn::Error::new_spanned(
                alias,
                "a kind alias cannot equal the primary kind",
            ));
        }
    }
    let mut alias_values = aliases.iter().map(LitStr::value).collect::<Vec<_>>();
    alias_values.sort_unstable();
    if alias_values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "kind aliases must be unique",
        ));
    }
    if let Some(max_attempts) = &max_attempts
        && !(1..=i16::MAX as u64).contains(&max_attempts.base10_parse::<u64>()?)
    {
        return Err(syn::Error::new_spanned(
            max_attempts,
            "max_attempts must be between 1 and 32767",
        ));
    }
    if let Some(priority) = &priority
        && !(1..=4).contains(&priority.base10_parse::<u8>()?)
    {
        return Err(syn::Error::new_spanned(
            priority,
            "priority must be between 1 and 4",
        ));
    }
    if let Some(queue) = &queue {
        validate_queue(queue)?;
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "JobArgs can only be derived for a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "JobArgs can only be derived for structs",
            ));
        }
    };

    let rename_all = serde_serialize_rename_all(&input.attrs)?;
    let mut available_json_fields = Vec::new();
    let mut unique_fields = Vec::new();
    for field in fields {
        let mut unique = false;
        for attribute in &field.attrs {
            if attribute.path().is_ident("river") {
                attribute.parse_nested_meta(|meta| {
                    if meta.path.is_ident("unique") {
                        unique = true;
                        return Ok(());
                    }
                    Err(meta.error("unsupported river field attribute"))
                })?;
            }
        }
        let serde = serde_field_attributes(&field.attrs)?;
        if unique && (serde.flatten || serde.skip || serde.skip_serializing) {
            return Err(syn::Error::new_spanned(
                field,
                "#[river(unique)] cannot be combined with #[serde(flatten)], #[serde(skip)], or #[serde(skip_serializing)]",
            ));
        }
        let rust_name = field.ident.as_ref().unwrap().unraw().to_string();
        let mut json_name = rename_all.as_ref().map_or_else(
            || Ok(rust_name.clone()),
            |rule| rename_field(&rust_name, rule),
        )?;
        if let Some(rename) = serde.rename {
            json_name = rename.value();
        }
        if !serde.flatten && !serde.skip && !serde.skip_serializing {
            available_json_fields.push(json_name.clone());
        }
        if unique {
            unique_fields.push(LitStr::new(
                &json_name,
                field.ident.as_ref().unwrap().span(),
            ));
        }
    }
    for path in unique_paths {
        let value = path.value();
        let mut segments = value.split('.');
        let first = segments.next().unwrap_or_default();
        if first.is_empty()
            || segments.any(str::is_empty)
            || !available_json_fields.iter().any(|field| field == first)
        {
            return Err(syn::Error::new_spanned(
                path,
                "unique JSON path must start with a serialized field name and contain no empty segments",
            ));
        }
        unique_fields.push(path);
    }

    let name = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let set_max_attempts = max_attempts.map(|value| quote!(.with_max_attempts(#value)));
    let set_pending = pending.map(|value| quote!(.with_pending(#value)));
    let set_priority = priority.map(|value| quote!(.with_priority(#value)));
    let set_queue = queue.map(|value| quote!(.with_queue(#value)));
    Ok(quote! {
        impl #impl_generics ::riverqueue::JobArgs for #name #type_generics #where_clause {
            const KIND: &'static str = #kind;

            fn kind_aliases() -> &'static [&'static str] {
                &[#(#aliases),*]
            }

            fn default_insert_opts() -> ::riverqueue::InsertOpts {
                ::riverqueue::InsertOpts::default()
                    #set_max_attempts
                    #set_pending
                    #set_priority
                    #set_queue
            }

            fn unique_fields() -> &'static [&'static str] {
                &[#(#unique_fields),*]
            }
        }
    })
}

#[derive(Default)]
struct SerdeFieldAttributes {
    flatten: bool,
    rename: Option<LitStr>,
    skip: bool,
    skip_serializing: bool,
}

fn serde_field_attributes(attributes: &[syn::Attribute]) -> syn::Result<SerdeFieldAttributes> {
    let mut parsed = SerdeFieldAttributes::default();
    for attribute in attributes {
        if !attribute.path().is_ident("serde") {
            continue;
        }
        for meta in serde_metas(attribute)? {
            match meta {
                Meta::Path(path) if path.is_ident("flatten") => parsed.flatten = true,
                Meta::Path(path) if path.is_ident("skip") => parsed.skip = true,
                Meta::Path(path) if path.is_ident("skip_serializing") => {
                    parsed.skip_serializing = true;
                }
                Meta::NameValue(meta) if meta.path.is_ident("rename") => {
                    parsed.rename = Some(meta_lit_str(&meta)?);
                }
                Meta::List(meta) if meta.path.is_ident("rename") => {
                    if let Some(rename) = serialize_name_from_list(&meta)? {
                        parsed.rename = Some(rename);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(parsed)
}

fn serde_serialize_rename_all(attributes: &[syn::Attribute]) -> syn::Result<Option<LitStr>> {
    let mut rename_all = None;
    for attribute in attributes {
        if !attribute.path().is_ident("serde") {
            continue;
        }
        for meta in serde_metas(attribute)? {
            match meta {
                Meta::NameValue(meta) if meta.path.is_ident("rename_all") => {
                    rename_all = Some(meta_lit_str(&meta)?);
                }
                Meta::List(meta) if meta.path.is_ident("rename_all") => {
                    if let Some(rename) = serialize_name_from_list(&meta)? {
                        rename_all = Some(rename);
                    }
                }
                _ => {}
            }
        }
    }
    Ok(rename_all)
}

fn serde_metas(attribute: &syn::Attribute) -> syn::Result<Punctuated<Meta, Token![,]>> {
    attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
}

fn serialize_name_from_list(meta: &syn::MetaList) -> syn::Result<Option<LitStr>> {
    let nested = meta.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
    nested
        .iter()
        .find_map(|meta| match meta {
            Meta::NameValue(meta) if meta.path.is_ident("serialize") => Some(meta_lit_str(meta)),
            _ => None,
        })
        .transpose()
}

fn meta_lit_str(meta: &syn::MetaNameValue) -> syn::Result<LitStr> {
    match &meta.value {
        syn::Expr::Lit(expression) => match &expression.lit {
            Lit::Str(value) => Ok(value.clone()),
            _ => Err(syn::Error::new_spanned(
                &meta.value,
                "expected string literal",
            )),
        },
        _ => Err(syn::Error::new_spanned(
            &meta.value,
            "expected string literal",
        )),
    }
}

fn rename_field(field: &str, rule: &LitStr) -> syn::Result<String> {
    let renamed = match rule.value().as_str() {
        "lowercase" | "snake_case" => field.to_owned(),
        "UPPERCASE" | "SCREAMING_SNAKE_CASE" => field.to_ascii_uppercase(),
        "PascalCase" => rename_field_pascal_case(field),
        "camelCase" => {
            let pascal = rename_field_pascal_case(field);
            let mut characters = pascal.chars();
            characters.next().map_or_else(String::new, |first| {
                first.to_ascii_lowercase().to_string() + characters.as_str()
            })
        }
        "kebab-case" => field.replace('_', "-"),
        "SCREAMING-KEBAB-CASE" => field.to_ascii_uppercase().replace('_', "-"),
        unsupported => {
            return Err(syn::Error::new_spanned(
                rule,
                format!("unsupported serde rename rule {unsupported:?}"),
            ));
        }
    };
    Ok(renamed)
}

fn rename_field_pascal_case(field: &str) -> String {
    let mut renamed = String::new();
    let mut capitalize = true;
    for character in field.chars() {
        if character == '_' {
            capitalize = true;
        } else if capitalize {
            renamed.push(character.to_ascii_uppercase());
            capitalize = false;
        } else {
            renamed.push(character);
        }
    }
    renamed
}

fn validate_kind(kind: &LitStr) -> syn::Result<()> {
    let value = kind.value();
    let mut characters = value.chars();
    if value.len() < 2
        || value.len() >= 128
        || !characters
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphanumeric())
        || !characters.all(|character| {
            character.is_ascii_alphanumeric()
                || matches!(
                    character,
                    '_' | '-' | '[' | ']' | '<' | '>' | '/' | '.' | '·' | ':' | '+'
                )
        })
    {
        return Err(syn::Error::new_spanned(kind, "invalid River job kind"));
    }
    Ok(())
}

fn validate_queue(queue: &LitStr) -> syn::Result<()> {
    let value = queue.value();
    let mut characters = value.chars();
    let valid = value.len() <= 64
        && characters
            .next()
            .is_some_and(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
        && value.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '_' | '-')
        })
        && !value.contains("__")
        && !value.contains("--")
        && !value.contains("_-")
        && !value.contains("-_")
        && value
            .chars()
            .next_back()
            .is_some_and(|character| character.is_ascii_lowercase() || character.is_ascii_digit());
    if !valid {
        return Err(syn::Error::new_spanned(queue, "invalid River queue name"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn derive(source: &str) -> syn::Result<proc_macro2::TokenStream> {
        expand_job_args(&syn::parse_str(source).expect("valid Rust syntax"))
    }

    #[test]
    fn accepts_complete_job_args_configuration() {
        let expanded = derive(
            r#"
            #[derive(serde::Serialize)]
            #[serde(rename_all = "camelCase")]
            #[river(
                kind = "email.send",
                aliases("email_send_v1"),
                max_attempts = 8,
                pending = true,
                priority = 2,
                queue = "email-critical",
                unique("account.id")
            )]
            struct EmailArgs {
                account: Account,
                #[river(unique)]
                message_id: String,
            }
            "#,
        )
        .unwrap()
        .to_string();

        assert!(expanded.contains("email.send"));
        assert!(expanded.contains("email_send_v1"));
        assert!(expanded.contains("messageId"));
        assert!(expanded.contains("account.id"));
    }

    #[test]
    fn follows_serde_serialization_names() {
        let expanded = derive(
            r#"
            #[serde(rename_all(serialize = "SCREAMING-KEBAB-CASE", deserialize = "camelCase"))]
            #[river(kind = "serde_names")]
            struct SerdeNames {
                #[river(unique)]
                first_value: String,
                #[river(unique)]
                #[serde(rename(serialize = "wire-name", deserialize = "inputName"))]
                second_value: String,
                #[river(unique)]
                r#type: String,
            }
            "#,
        )
        .unwrap()
        .to_string();

        assert!(expanded.contains("FIRST-VALUE"));
        assert!(expanded.contains("wire-name"));
        assert!(expanded.contains("TYPE"));
        assert!(!expanded.contains("inputName"));
    }

    #[test]
    fn permits_conditionally_omitted_unique_fields() {
        let expanded = derive(
            r#"
            #[river(kind = "optional_unique")]
            struct OptionalUnique {
                #[river(unique)]
                #[serde(skip_serializing_if = "Option::is_none")]
                optional: Option<String>,
            }
            "#,
        )
        .unwrap()
        .to_string();

        assert!(expanded.contains("optional"));
    }

    #[test]
    fn rejects_serde_attributes_that_hide_unique_fields() {
        for serde_attribute in ["flatten", "skip", "skip_serializing"] {
            let source = format!(
                r#"
                #[river(kind = "invalid_serde")]
                struct InvalidSerde {{
                    #[river(unique)]
                    #[serde({serde_attribute})]
                    value: String,
                }}
                "#,
            );
            let error = derive(&source).expect_err("attribute combination should be rejected");

            assert!(
                error
                    .to_string()
                    .contains("#[river(unique)] cannot be combined"),
                "unexpected error for {serde_attribute}: {error}"
            );
        }

        for serde_attribute in ["flatten", "skip", "skip_serializing"] {
            let source = format!(
                r#"
                #[river(kind = "invalid_serde_path", unique("value"))]
                struct InvalidSerdePath {{
                    #[serde({serde_attribute})]
                    value: String,
                }}
                "#,
            );
            let error = derive(&source).expect_err("hidden unique path should be rejected");

            assert!(
                error
                    .to_string()
                    .contains("unique JSON path must start with a serialized field name"),
                "unexpected path error for {serde_attribute}: {error}"
            );
        }
    }

    #[test]
    fn rejects_unsupported_serde_rename_rule() {
        let error = derive(
            r#"
            #[serde(rename_all(serialize = "Title Case"))]
            #[river(kind = "invalid_rename")]
            struct InvalidRename { value: String }
            "#,
        )
        .expect_err("rename rule should be rejected");

        assert!(
            error
                .to_string()
                .contains("unsupported serde rename rule \"Title Case\"")
        );
    }

    #[test]
    fn rejects_invalid_job_args_configuration() {
        let cases = [
            (
                "struct MissingKind { value: String }",
                "JobArgs requires #[river(kind = \"...\")]",
            ),
            (
                r#"#[river(kind = "x")] struct InvalidKind { value: String }"#,
                "invalid River job kind",
            ),
            (
                r#"#[river(kind = "valid", aliases("valid"))] struct DuplicateKind { value: String }"#,
                "a kind alias cannot equal the primary kind",
            ),
            (
                r#"#[river(kind = "valid", priority = 5)] struct InvalidPriority { value: String }"#,
                "priority must be between 1 and 4",
            ),
            (
                r#"#[river(kind = "valid", queue = "Invalid")] struct InvalidQueue { value: String }"#,
                "invalid River queue name",
            ),
            (
                r#"#[river(kind = "valid", unique("missing.id"))] struct InvalidPath { value: String }"#,
                "unique JSON path must start with a serialized field name",
            ),
        ];

        for (source, message) in cases {
            let error = derive(source).expect_err("configuration should be rejected");
            assert!(
                error.to_string().contains(message),
                "unexpected error for {source}: {error}"
            );
        }
    }
}
