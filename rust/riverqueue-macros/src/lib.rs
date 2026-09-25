#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote, quote_spanned};
use syn::{
    Data, DeriveInput, Fields, Ident, Lit, LitBool, LitInt, LitStr, Meta, Token,
    ext::IdentExt as _, meta::ParseNestedMeta, parenthesized, parse_macro_input,
    punctuated::Punctuated, spanned::Spanned as _, token,
};

/// Derives `riverqueue::JobArgs`.
///
/// The type must declare `#[river(kind = "...")]`. Other type-level options:
///
/// * `aliases("old_kind", ...)`: former kinds handled by the same worker.
/// * `queue = "..."`, `max_attempts = N`, `priority = N`, `pending = true`,
///   and `tags("a", "b")`: default insertion options.
/// * `unique(...)`: makes the job unique by default. Options are `by_args`,
///   `by_args("nested.path", ...)`, `by_period = "1h"` (a Go duration of at
///   least one second), `by_queue`, `by_state(available, running, ...)`, and
///   `exclude_kind`.
/// * `insert_opts = path::to_fn`: a `fn() -> InsertOpts` whose options are
///   overlaid on the attribute defaults, like Go's `JobArgsWithInsertOpts`.
/// * `crate = "path"`: the path to `riverqueue` when it is renamed or
///   re-exported.
///
/// Fields marked `#[river(unique)]` are the arguments hashed by `unique(by_args)`,
/// together with any `by_args` paths; without either, every argument is
/// hashed. Marking a field requires `unique(by_args)` on the type. Field
/// names follow Serde's serialization-side `rename` and `rename_all`. A
/// unique field may be conditionally omitted with `skip_serializing_if`,
/// matching River Go, but cannot be flattened or unconditionally skipped.
/// `by_args` paths separate nested names with `.`; escape a literal dot or
/// backslash with a backslash (for example, `"user\\.id"` selects the single
/// JSON name `user.id`). Tagged fields use their whole serialized name as one
/// component, even when that name contains a dot.
#[proc_macro_derive(JobArgs, attributes(river))]
pub fn derive_job_args(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_job_args(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[derive(Default)]
struct TypeAttributes {
    aliases: Vec<LitStr>,
    krate: Option<syn::Path>,
    insert_opts: Option<syn::Path>,
    kind: Option<LitStr>,
    max_attempts: Option<LitInt>,
    pending: Option<LitBool>,
    priority: Option<LitInt>,
    queue: Option<LitStr>,
    tags: Vec<LitStr>,
    unique: Option<UniqueAttribute>,
}

struct UniqueAttribute {
    by_args: bool,
    by_args_paths: Vec<LitStr>,
    by_period: Option<(u64, u32)>,
    by_queue: bool,
    by_state: Option<Vec<Ident>>,
    exclude_kind: bool,
}

const JOB_STATES: [(&str, &str); 8] = [
    ("available", "Available"),
    ("cancelled", "Cancelled"),
    ("completed", "Completed"),
    ("discarded", "Discarded"),
    ("pending", "Pending"),
    ("retryable", "Retryable"),
    ("running", "Running"),
    ("scheduled", "Scheduled"),
];

const UNIQUE_REQUIRES_OPTION: &str =
    "unique(...) requires at least one of by_args, by_period, by_queue, by_state, or exclude_kind";

const JOB_STATES_REQUIRED_FOR_UNIQUE: [&str; 4] = ["available", "pending", "running", "scheduled"];

fn parse_type_attributes(input: &DeriveInput) -> syn::Result<TypeAttributes> {
    let mut parsed = TypeAttributes::default();
    for attribute in &input.attrs {
        if !attribute.path().is_ident("river") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("aliases") {
                parsed.aliases = parse_string_list(&meta)?;
            } else if meta.path.is_ident("crate") {
                parsed.krate = Some(meta.value()?.parse::<LitStr>()?.parse()?);
            } else if meta.path.is_ident("insert_opts") {
                parsed.insert_opts = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("kind") {
                parsed.kind = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("max_attempts") {
                parsed.max_attempts = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("pending") {
                parsed.pending = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("priority") {
                parsed.priority = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("queue") {
                parsed.queue = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("tags") {
                parsed.tags = parse_string_list(&meta)?;
            } else if meta.path.is_ident("unique") {
                parsed.unique = Some(parse_unique(&meta)?);
            } else {
                return Err(meta.error("unsupported river type attribute"));
            }
            Ok(())
        })?;
    }
    Ok(parsed)
}

fn parse_string_list(meta: &ParseNestedMeta<'_>) -> syn::Result<Vec<LitStr>> {
    let content;
    parenthesized!(content in meta.input);
    Ok(Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?
        .into_iter()
        .collect())
}

fn parse_unique(meta: &ParseNestedMeta<'_>) -> syn::Result<UniqueAttribute> {
    let span = meta
        .path
        .get_ident()
        .map_or_else(Span::call_site, Ident::span);
    {
        let lookahead = meta.input.fork();
        let content;
        parenthesized!(content in lookahead);
        if content.peek(LitStr) {
            return Err(content
                .error("declare unique argument paths with `unique(by_args(\"path\", ...))`"));
        }
        if content.is_empty() {
            return Err(syn::Error::new(span, UNIQUE_REQUIRES_OPTION));
        }
    }
    let mut unique = UniqueAttribute {
        by_args: false,
        by_args_paths: Vec::new(),
        by_period: None,
        by_queue: false,
        by_state: None,
        exclude_kind: false,
    };
    meta.parse_nested_meta(|option| {
        if option.path.is_ident("by_args") {
            unique.by_args = true;
            if option.input.peek(token::Paren) {
                unique.by_args_paths = parse_string_list(&option)?;
            }
        } else if option.path.is_ident("by_period") {
            let period = option.value()?.parse::<LitStr>()?;
            let nanos = parse_go_duration(&period.value())
                .map_err(|message| syn::Error::new_spanned(&period, message))?;
            if nanos < 1_000_000_000 {
                return Err(syn::Error::new_spanned(
                    &period,
                    "by_period must be at least one second",
                ));
            }
            let seconds = u64::try_from(nanos / 1_000_000_000)
                .map_err(|_| syn::Error::new_spanned(&period, "by_period is too large"))?;
            let nanos = u32::try_from(nanos % 1_000_000_000).unwrap_or_default();
            unique.by_period = Some((seconds, nanos));
        } else if option.path.is_ident("by_queue") {
            unique.by_queue = true;
        } else if option.path.is_ident("by_state") {
            let content;
            parenthesized!(content in option.input);
            let states = Punctuated::<Ident, Token![,]>::parse_terminated(&content)?
                .into_iter()
                .collect::<Vec<_>>();
            validate_unique_states(&states, &option)?;
            unique.by_state = Some(states);
        } else if option.path.is_ident("exclude_kind") {
            unique.exclude_kind = true;
        } else {
            return Err(option.error(
                "unsupported unique option; expected by_args, by_period, by_queue, by_state, or exclude_kind",
            ));
        }
        Ok(())
    })?;
    Ok(unique)
}

fn validate_unique_states(states: &[Ident], option: &ParseNestedMeta<'_>) -> syn::Result<()> {
    let mut seen = Vec::new();
    for state in states {
        let name = state.to_string();
        if !JOB_STATES.iter().any(|(known, _)| *known == name) {
            return Err(syn::Error::new_spanned(
                state,
                format!(
                    "unknown job state `{name}`; expected one of {}",
                    JOB_STATES
                        .iter()
                        .map(|(known, _)| *known)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
        if seen.contains(&name) {
            return Err(syn::Error::new_spanned(
                state,
                format!("duplicate job state `{name}`"),
            ));
        }
        seen.push(name);
    }
    let missing = JOB_STATES_REQUIRED_FOR_UNIQUE
        .iter()
        .filter(|required| !seen.iter().any(|state| state == *required))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(option.error(format!(
            "by_state must include available, pending, running, and scheduled; missing {}",
            missing.join(", ")
        )));
    }
    Ok(())
}

/// Parses a Go `time.ParseDuration` string without a sign, such as `1h30m`
/// or `1.5h`, into nanoseconds.
fn parse_go_duration(text: &str) -> Result<u128, String> {
    let invalid = || format!("invalid duration {text:?}; expected a Go duration such as \"1h\"");
    if text.is_empty() {
        return Err(invalid());
    }
    let mut rest = text;
    let mut total = 0_u128;
    while !rest.is_empty() {
        let number_end = rest
            .find(|character: char| !(character.is_ascii_digit() || character == '.'))
            .unwrap_or(rest.len());
        let (number, after_number) = rest.split_at(number_end);
        let unit_end = after_number
            .find(|character: char| character.is_ascii_digit() || character == '.')
            .unwrap_or(after_number.len());
        let (unit, after_unit) = after_number.split_at(unit_end);
        rest = after_unit;

        let unit_nanos: u128 = match unit {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(invalid()),
        };
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        if whole.is_empty() && fraction.is_empty() {
            return Err(invalid());
        }
        let whole = if whole.is_empty() {
            0
        } else {
            whole.parse::<u128>().map_err(|_| invalid())?
        };
        let mut value = whole.checked_mul(unit_nanos).ok_or_else(invalid)?;
        if !fraction.is_empty() {
            let scale = 10_u128
                .checked_pow(u32::try_from(fraction.len()).map_err(|_| invalid())?)
                .ok_or_else(invalid)?;
            let fraction = fraction.parse::<u128>().map_err(|_| invalid())?;
            value = value
                .checked_add(fraction.checked_mul(unit_nanos).ok_or_else(invalid)? / scale)
                .ok_or_else(invalid)?;
        }
        total = total.checked_add(value).ok_or_else(invalid)?;
    }
    Ok(total)
}

#[allow(clippy::too_many_lines)]
fn expand_job_args(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let attributes = parse_type_attributes(input)?;
    let kind = attributes.kind.clone().ok_or_else(|| {
        syn::Error::new_spanned(&input.ident, "JobArgs requires #[river(kind = \"...\")]")
    })?;
    validate_kind(&kind)?;
    for alias in &attributes.aliases {
        validate_kind(alias)?;
        if alias.value() == kind.value() {
            return Err(syn::Error::new_spanned(
                alias,
                "a kind alias cannot equal the primary kind",
            ));
        }
    }
    let mut alias_values = attributes
        .aliases
        .iter()
        .map(LitStr::value)
        .collect::<Vec<_>>();
    alias_values.sort_unstable();
    if alias_values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "kind aliases must be unique",
        ));
    }
    if let Some(max_attempts) = &attributes.max_attempts
        && !(1..=i16::MAX as u64).contains(&max_attempts.base10_parse::<u64>()?)
    {
        return Err(syn::Error::new_spanned(
            max_attempts,
            "max_attempts must be between 1 and 32767",
        ));
    }
    if let Some(priority) = &attributes.priority
        && !(1..=4).contains(&priority.base10_parse::<u8>()?)
    {
        return Err(syn::Error::new_spanned(
            priority,
            "priority must be between 1 and 4",
        ));
    }
    if let Some(queue) = &attributes.queue {
        validate_queue(queue)?;
    }
    for tag in &attributes.tags {
        validate_tag(tag)?;
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
        let field_ident = field.ident.as_ref().expect("named fields have identifiers");
        let rust_name = field_ident.unraw().to_string();
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
            if !attributes
                .unique
                .as_ref()
                .is_some_and(|unique| unique.by_args)
            {
                return Err(syn::Error::new_spanned(
                    field_ident,
                    "#[river(unique)] fields are only hashed with `unique(by_args)`; add it to the type's #[river(...)] attribute",
                ));
            }
            unique_fields.push(vec![LitStr::new(&json_name, field_ident.span())]);
        }
    }
    if let Some(unique) = &attributes.unique {
        for path in &unique.by_args_paths {
            let components = parse_unique_path(path)?;
            let first = &components[0];
            if !available_json_fields.iter().any(|field| field == first) {
                return Err(syn::Error::new_spanned(
                    path,
                    "unique JSON path must start with a serialized field name",
                ));
            }
            unique_fields.push(
                components
                    .into_iter()
                    .map(|component| LitStr::new(&component, path.span()))
                    .collect(),
            );
        }
    }
    validate_unique_paths(&unique_fields)?;
    let unique_fields = unique_fields.iter().map(|path| quote!(&[#(#path),*]));

    let krate = attributes
        .krate
        .clone()
        .unwrap_or_else(|| syn::parse_quote!(::riverqueue));
    let name = &input.ident;
    let aliases = &attributes.aliases;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    let set_max_attempts = attributes
        .max_attempts
        .as_ref()
        .map(|value| quote!(.with_max_attempts(#value)));
    let set_pending = attributes
        .pending
        .as_ref()
        .map(|value| quote!(.with_pending(#value)));
    let set_priority = attributes
        .priority
        .as_ref()
        .map(|value| quote!(.with_priority(#value)));
    let set_queue = attributes
        .queue
        .as_ref()
        .map(|value| quote!(.with_queue(#value)));
    let set_tags = (!attributes.tags.is_empty()).then(|| {
        let tags = &attributes.tags;
        quote!(.with_tags([#(#tags),*]))
    });
    let set_unique = attributes
        .unique
        .as_ref()
        .map(|unique| expand_unique_opts(&krate, unique));
    // Bind the function's result with the expected type so a mismatched
    // function is reported at its path.
    let overlay = attributes.insert_opts.as_ref().map(|function| {
        let call = quote_spanned!(function.span()=> #function());
        quote! {
            .overlay({
                let overrides: #krate::InsertOpts = #call;
                overrides
            })
        }
    });
    Ok(quote! {
        impl #impl_generics #krate::JobArgs for #name #type_generics #where_clause {
            const KIND: &'static str = #kind;

            fn kind_aliases() -> &'static [&'static str] {
                &[#(#aliases),*]
            }

            fn default_insert_opts() -> #krate::InsertOpts {
                #krate::InsertOpts::default()
                    #set_max_attempts
                    #set_pending
                    #set_priority
                    #set_queue
                    #set_tags
                    #set_unique
                    #overlay
            }

            fn unique_fields() -> &'static [&'static [&'static str]] {
                &[#(#unique_fields),*]
            }
        }
    })
}

fn expand_unique_opts(krate: &syn::Path, unique: &UniqueAttribute) -> proc_macro2::TokenStream {
    let by_args = unique.by_args.then(|| quote!(.by_args()));
    let by_period = unique
        .by_period
        .map(|(seconds, nanos)| quote!(.by_period(::core::time::Duration::new(#seconds, #nanos))));
    let by_queue = unique.by_queue.then(|| quote!(.by_queue()));
    let by_state = unique.by_state.as_ref().map(|states| {
        let variants = states.iter().map(|state| {
            let variant = JOB_STATES
                .iter()
                .find(|(name, _)| state == name)
                .map_or("Available", |(_, variant)| variant);
            let variant = Ident::new(variant, state.span());
            quote!(#krate::JobState::#variant)
        });
        quote!(.by_states([#(#variants),*]))
    });
    let exclude_kind = unique.exclude_kind.then(|| quote!(.without_kind()));
    quote! {
        .with_unique(
            #krate::UniqueOpts::new()
                #by_args
                #by_period
                #by_queue
                #by_state
                #exclude_kind
        )
    }
}

/// Decode the convenience dotted syntax into literal JSON field names.
fn parse_unique_path(path: &LitStr) -> syn::Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for character in path.value().chars() {
        if escaped {
            current.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '.' {
            parts.push(std::mem::take(&mut current));
        } else {
            current.push(character);
        }
    }
    if escaped {
        return Err(syn::Error::new_spanned(
            path,
            "unique JSON path ends in an escape",
        ));
    }
    parts.push(current);
    if parts.iter().any(String::is_empty) {
        return Err(syn::Error::new_spanned(
            path,
            "unique JSON path segments cannot be empty",
        ));
    }
    Ok(parts)
}

fn validate_unique_paths(paths: &[Vec<LitStr>]) -> syn::Result<()> {
    for path in paths {
        if path.iter().any(|segment| segment.value().is_empty()) {
            return Err(syn::Error::new_spanned(
                &path[0],
                "unique JSON path segments cannot be empty",
            ));
        }
        if let Some(segment) = path.iter().find(|segment| {
            let value = segment.value();
            value.bytes().all(|byte| byte.is_ascii_digit()) || value == "-1"
        }) {
            return Err(syn::Error::new_spanned(
                segment,
                "numeric unique JSON path segments require array semantics that are not yet supported",
            ));
        }
    }
    for path in paths {
        let value = path.iter().map(LitStr::value).collect::<Vec<_>>();
        if let Some(other) = paths.iter().find(|other| {
            other.len() > path.len()
                && other
                    .iter()
                    .zip(&value)
                    .all(|(segment, value)| segment.value() == *value)
        }) {
            return Err(syn::Error::new_spanned(
                &other[0],
                format!(
                    "unique JSON path {:?} is inside another unique path {value:?}",
                    other.iter().map(LitStr::value).collect::<Vec<_>>()
                ),
            ));
        }
    }
    Ok(())
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

/// Validates a tag with River's rules: 3 to 255 bytes of ASCII word
/// characters and `-`, starting and ending with a word character.
fn validate_tag(tag: &LitStr) -> syn::Result<()> {
    let value = tag.value();
    let is_word = |character: char| character == '_' || character.is_ascii_alphanumeric();
    let valid = (3..=255).contains(&value.len())
        && value.chars().next().is_some_and(is_word)
        && value.chars().next_back().is_some_and(is_word)
        && value
            .chars()
            .all(|character| is_word(character) || character == '-');
    if !valid {
        return Err(syn::Error::new_spanned(
            tag,
            "invalid River tag; tags contain 3 to 255 ASCII letters, digits, `_`, or `-`, and start and end with a letter, digit, or `_`",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn derive(source: &str) -> syn::Result<proc_macro2::TokenStream> {
        expand_job_args(&syn::parse_str(source).expect("valid Rust syntax"))
    }

    fn compact(tokens: &proc_macro2::TokenStream) -> String {
        tokens.to_string().replace(' ', "")
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
                tags("email", "outbound-mail"),
                unique(
                    by_args("account.id"),
                    by_period = "1h30m",
                    by_queue,
                    by_state(available, pending, running, scheduled, retryable),
                    exclude_kind
                ),
                insert_opts = email_insert_opts
            )]
            struct EmailArgs {
                account: Account,
                #[river(unique)]
                message_id: String,
            }
            "#,
        )
        .unwrap();
        let expanded = compact(&expanded);

        assert!(expanded.contains("\"email.send\""));
        assert!(expanded.contains("\"email_send_v1\""));
        assert!(expanded.contains(".with_tags([\"email\",\"outbound-mail\"])"));
        assert!(expanded.contains(".by_args()"));
        assert!(expanded.contains(".by_period(::core::time::Duration::new(5400u64,0u32))"));
        assert!(expanded.contains(".by_queue()"));
        assert!(expanded.contains("::riverqueue::JobState::Retryable"));
        assert!(expanded.contains(".without_kind()"));
        assert!(expanded.contains(
            ".overlay({letoverrides:::riverqueue::InsertOpts=email_insert_opts();overrides})"
        ));
        assert!(expanded.contains("&[&[\"messageId\"],&[\"account\",\"id\"]]"));
    }

    #[test]
    fn follows_serde_serialization_names() {
        let expanded = derive(
            r#"
            #[serde(rename_all(serialize = "SCREAMING-KEBAB-CASE", deserialize = "camelCase"))]
            #[river(kind = "serde_names", unique(by_args))]
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
    fn parses_go_durations() {
        for (text, nanos) in [
            ("1s", 1_000_000_000),
            ("1h", 3_600_000_000_000),
            ("1h30m", 5_400_000_000_000),
            ("1.5h", 5_400_000_000_000),
            ("90m", 5_400_000_000_000),
            ("1500ms", 1_500_000_000),
            ("2s500ms", 2_500_000_000),
            ("1\u{b5}s", 1_000),
            ("7ns", 7),
        ] {
            assert_eq!(parse_go_duration(text), Ok(nanos), "{text}");
        }
        for text in ["", "1", "h", "1x", "-1h", "1.h.", "."] {
            assert!(parse_go_duration(text).is_err(), "{text}");
        }
    }

    #[test]
    fn permits_conditionally_omitted_unique_fields() {
        let expanded = derive(
            r#"
            #[river(kind = "optional_unique", unique(by_args))]
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
                #[river(kind = "invalid_serde", unique(by_args))]
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
                #[river(kind = "invalid_serde_path", unique(by_args("value")))]
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
                r#"#[river(kind = "valid", tags("x"))] struct InvalidTag { value: String }"#,
                "invalid River tag",
            ),
            (
                r#"#[river(kind = "valid", unique(by_args("missing.id")))] struct InvalidPath { value: String }"#,
                "unique JSON path must start with a serialized field name",
            ),
            (
                r#"#[river(kind = "valid", unique("value"))] struct OldPathSyntax { value: String }"#,
                "declare unique argument paths with `unique(by_args(",
            ),
            (
                r#"#[river(kind = "valid", unique())] struct EmptyUnique { value: String }"#,
                "unique(...) requires at least one of",
            ),
            (
                r#"#[river(kind = "valid", unique(by_arg))] struct UnknownUnique { value: String }"#,
                "unsupported unique option",
            ),
            (
                r#"#[river(kind = "valid", unique(by_period = "500ms"))] struct ShortPeriod { value: String }"#,
                "by_period must be at least one second",
            ),
            (
                r#"#[river(kind = "valid", unique(by_period = "1 hour"))] struct BadPeriod { value: String }"#,
                "invalid duration",
            ),
            (
                r#"#[river(kind = "valid", unique(by_state(available, running)))] struct MissingStates { value: String }"#,
                "missing pending, scheduled",
            ),
            (
                r#"#[river(kind = "valid", unique(by_state(available, pending, running, scheduled, done)))] struct UnknownState { value: String }"#,
                "unknown job state `done`",
            ),
            (
                r#"#[river(kind = "valid", unique(by_state(available, available, pending, running, scheduled)))] struct DuplicateState { value: String }"#,
                "duplicate job state `available`",
            ),
            (
                r#"#[river(kind = "valid", unique(by_queue))] struct UniqueFieldWithoutArgs { #[river(unique)] value: String }"#,
                "#[river(unique)] fields are only hashed with `unique(by_args)`",
            ),
            (
                r#"#[river(kind = "valid")] struct UniqueFieldWithoutUnique { #[river(unique)] value: String }"#,
                "#[river(unique)] fields are only hashed with `unique(by_args)`",
            ),
            (
                r#"#[river(kind = "valid", unique(by_args("value.0")))] struct IndexPath { value: String }"#,
                "require array semantics",
            ),
            (
                r#"#[river(kind = "valid", unique(by_args("value", "value.id")))] struct NestedPath { value: String }"#,
                "is inside another unique path",
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

    #[test]
    fn uses_configured_crate_path() {
        let expanded = derive(
            r#"
            #[river(kind = "renamed", crate = "::my_app::river", unique(by_state(available, pending, running, scheduled)))]
            struct Renamed { value: String }
            "#,
        )
        .unwrap();
        let expanded = compact(&expanded);

        assert!(expanded.contains("impl::my_app::river::JobArgsforRenamed"));
        assert!(expanded.contains("::my_app::river::JobState::Available"));
        assert!(!expanded.contains("::riverqueue::"));
    }
}
