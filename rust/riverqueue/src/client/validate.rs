//! Validation of job insertion parameters.

#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) fn validate_insert_parts(
    kind: &str,
    opts: &InsertParams,
    allow_legacy_job_kinds: bool,
) -> Result<(), Error> {
    let mut kind_characters = kind.chars();
    if !allow_legacy_job_kinds
        && (kind.len() < 2
            || kind.len() >= 128
            || !kind_characters.next().is_some_and(is_word)
            || !kind_characters.all(valid_kind_character))
    {
        return Err(Error::invalid_job(format!("invalid job kind {kind:?}")));
    }
    if opts.max_attempts < 1 {
        return Err(Error::invalid_job(
            "max_attempts must be greater than zero".to_owned(),
        ));
    }
    if !(1..=4).contains(&opts.priority) {
        return Err(Error::invalid_job(
            "priority must be between one and four".to_owned(),
        ));
    }
    validate_queue(&opts.queue)?;
    for tag in &opts.tags {
        if tag.len() > 255 || tag.len() < 3 {
            return Err(Error::invalid_job(
                "tags must contain between 3 and 255 bytes".to_owned(),
            ));
        }
        let mut characters = tag.chars();
        let first = characters.next().unwrap();
        let last = tag.chars().next_back().unwrap();
        if !is_word(first)
            || !is_word(last)
            || !characters.all(|character| is_word(character) || character == '-')
        {
            return Err(Error::invalid_job(format!("invalid tag {tag:?}")));
        }
    }
    opts.unique.validate().map_err(Error::invalid_job)
}

pub(super) fn valid_kind_character(character: char) -> bool {
    character.is_ascii_alphanumeric()
        || matches!(
            character,
            '_' | '-' | '[' | ']' | '<' | '>' | '/' | '.' | '·' | ':' | '+'
        )
}

pub(crate) fn validate_queue(queue: &str) -> Result<(), Error> {
    if queue.is_empty() || queue.len() > 64 {
        return Err(Error::invalid_job(
            "queue name must contain between 1 and 64 bytes".to_owned(),
        ));
    }
    if !queue
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
    {
        return Err(Error::invalid_job(format!("invalid queue name {queue:?}")));
    }
    let mut previous_separator = false;
    for character in queue.chars() {
        let separator = matches!(character, '_' | '|' | '-');
        if !(character.is_ascii_lowercase() || character.is_ascii_digit() || separator)
            || (separator && previous_separator)
        {
            return Err(Error::invalid_job(format!("invalid queue name {queue:?}")));
        }
        previous_separator = separator;
    }
    if previous_separator {
        return Err(Error::invalid_job(format!("invalid queue name {queue:?}")));
    }
    Ok(())
}

#[cfg(feature = "postgres")]
pub(super) fn validate_identifier(identifier: &str, description: &str) -> Result<(), Error> {
    let mut characters = identifier.chars();
    if identifier.is_empty()
        || identifier.len() > 63
        || !characters
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(Error::invalid_job(format!(
            "invalid PostgreSQL {description} identifier {identifier:?}"
        )));
    }
    Ok(())
}

pub(super) fn is_word(character: char) -> bool {
    character == '_' || character.is_ascii_alphanumeric()
}

pub(super) fn validate_metadata_key(key: &str) -> Result<(), Error> {
    let mut characters = key.chars();
    if key.is_empty()
        || !characters
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(Error::configuration(format!(
            "invalid job cleaner metadata exclusion key {key:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_queue;

    #[test]
    fn queue_names_match_go_validation() {
        // Mirrors Go's `^(?:[a-z0-9])+(?:[_|\-]?[a-z0-9]+)*$` plus its
        // 64-byte limit.
        for valid in [
            "0",
            "a",
            "a-b",
            "a_b",
            "a|b",
            "default",
            "tenant|priority_emails-2",
            &"a".repeat(64),
        ] {
            assert!(validate_queue(valid).is_ok(), "{valid:?} should be valid");
        }
        for invalid in [
            "",
            "-a",
            "A",
            "_a",
            "a b",
            "a-",
            "a.b",
            "a__b",
            "a_|b",
            "a|",
            "|a",
            &"a".repeat(65),
        ] {
            assert!(
                validate_queue(invalid).is_err(),
                "{invalid:?} should be invalid"
            );
        }
    }
}
