//! Derive macros for River job arguments.

#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Fields, LitBool, LitInt, LitStr, Token, parenthesized, parse_macro_input,
    punctuated::Punctuated,
};

/// Derives `riverqueue::JobArgs`.
///
/// The type must declare `#[river(kind = "...")]`. It may also declare
/// `aliases("old_kind")`, `queue = "..."`, `max_attempts = N`, `priority = N`,
/// and `pending = true`. Fields marked `#[river(unique)]` are used by
/// argument-scoped unique jobs.
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

    let rename_all = serde_rename_all(&input.attrs)?;
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
        let rust_name = field.ident.as_ref().unwrap().to_string();
        let mut json_name = rename_all
            .as_deref()
            .map_or_else(|| rust_name.clone(), |rule| rename_field(&rust_name, rule));
        for attribute in &field.attrs {
            if attribute.path().is_ident("serde") {
                attribute.parse_nested_meta(|meta| {
                    if meta.path.is_ident("rename") {
                        json_name = meta.value()?.parse::<LitStr>()?.value();
                    }
                    Ok(())
                })?;
            }
        }
        available_json_fields.push(json_name.clone());
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
    let set_max_attempts = max_attempts.map(|value| quote!(opts.max_attempts = #value;));
    let set_pending = pending.map(|value| quote!(opts.pending = #value;));
    let set_priority = priority.map(|value| quote!(opts.priority = #value;));
    let set_queue = queue.map(|value| quote!(opts.queue = #value.to_owned();));
    Ok(quote! {
        impl #impl_generics ::riverqueue::JobArgs for #name #type_generics #where_clause {
            const KIND: &'static str = #kind;

            fn kind_aliases() -> &'static [&'static str] {
                &[#(#aliases),*]
            }

            fn default_insert_opts() -> ::riverqueue::InsertOpts {
                let mut opts = ::riverqueue::InsertOpts::default();
                #set_max_attempts
                #set_pending
                #set_priority
                #set_queue
                opts
            }

            fn unique_fields() -> &'static [&'static str] {
                &[#(#unique_fields),*]
            }
        }
    })
}

fn serde_rename_all(attributes: &[syn::Attribute]) -> syn::Result<Option<String>> {
    let mut rename_all = None;
    for attribute in attributes {
        if !attribute.path().is_ident("serde") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                rename_all = Some(meta.value()?.parse::<LitStr>()?.value());
            }
            Ok(())
        })?;
    }
    Ok(rename_all)
}

fn rename_field(field: &str, rule: &str) -> String {
    let words = field
        .split('_')
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    match rule {
        "camelCase" => words.first().map_or_else(String::new, |first| {
            first.to_lowercase()
                + &words
                    .iter()
                    .skip(1)
                    .map(|word| capitalize(word))
                    .collect::<String>()
        }),
        "PascalCase" => words.iter().map(|word| capitalize(word)).collect(),
        "kebab-case" => words.join("-").to_lowercase(),
        "SCREAMING-KEBAB-CASE" => words.join("-").to_uppercase(),
        "SCREAMING_SNAKE_CASE" => words.join("_").to_uppercase(),
        "lowercase" => words.concat().to_lowercase(),
        "snake_case" => words.join("_").to_lowercase(),
        "UPPERCASE" => words.concat().to_uppercase(),
        _ => field.to_owned(),
    }
}

fn capitalize(word: &str) -> String {
    let mut characters = word.chars();
    characters.next().map_or_else(String::new, |first| {
        first.to_uppercase().collect::<String>() + characters.as_str()
    })
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
