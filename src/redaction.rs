use std::{fmt, path::PathBuf};

pub const MAX_SUMMARY_BYTES: usize = 4 * 1024;
pub const MAX_QUESTION_BYTES: usize = 1024;
pub const MAX_QUESTION_COUNT: usize = 16;
pub const MAX_CHANGED_FILE_BYTES: usize = 256;
pub const MAX_CHANGED_FILE_COUNT: usize = 256;
pub const MAX_FAILURE_REASON_BYTES: usize = 1024;
pub const MAX_DIFF_STAT_BYTES: usize = 4 * 1024;
pub const MAX_TITLE_BYTES: usize = 120;

const PATH_PLACEHOLDER: &str = "[path]";
const TOKEN_PLACEHOLDER: &str = "[token]";
const MIN_HEX_TOKEN: usize = 32;
const MIN_BASE64_TOKEN: usize = 32;
const MIN_SK_TOKEN: usize = 8;
const COMMON_PATH_PREFIXES: &[&str] = &[
    "/Users/",
    "/home/",
    "/private/var/folders/",
    "/var/folders/",
    "/private/tmp/",
    "/tmp/",
];

#[derive(Clone)]
pub struct RedactionBoundary {
    homes: Vec<String>,
    secrets: Vec<String>,
}

impl fmt::Debug for RedactionBoundary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedactionBoundary")
            .field("home_count", &self.homes.len())
            .field("secret_count", &self.secrets.len())
            .finish()
    }
}

impl RedactionBoundary {
    pub fn from_env() -> Self {
        Self::for_home(std::env::var_os("HOME").map(PathBuf::from))
    }

    pub fn new(home: impl Into<PathBuf>) -> Self {
        Self::for_home(Some(home.into()))
    }

    fn for_home(home: Option<PathBuf>) -> Self {
        let mut homes = COMMON_PATH_PREFIXES
            .iter()
            .map(|prefix| (*prefix).to_owned())
            .collect::<Vec<_>>();
        if let Some(home) = home {
            if let Some(raw) = home.to_str() {
                push_unique(&mut homes, raw.to_owned());
            }
            if let Ok(canonical) = std::fs::canonicalize(&home)
                && let Some(raw) = canonical.to_str()
            {
                push_unique(&mut homes, raw.to_owned());
            }
        }
        homes.sort_by_key(|home| std::cmp::Reverse(home.len()));
        Self {
            homes,
            secrets: Vec::new(),
        }
    }

    pub fn with_secrets<I, S>(mut self, secrets: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for secret in secrets {
            push_unique(&mut self.secrets, escape_controls(secret.as_ref()));
        }
        self.secrets
            .sort_by_key(|secret| std::cmp::Reverse(secret.len()));
        self
    }

    pub fn summary(&self, input: &str) -> String {
        self.text(input, MAX_SUMMARY_BYTES)
    }

    pub fn question(&self, input: &str) -> String {
        self.text(input, MAX_QUESTION_BYTES)
    }

    pub fn changed_file(&self, input: &str) -> String {
        self.text(input, MAX_CHANGED_FILE_BYTES)
    }

    pub fn failure_reason(&self, input: &str) -> String {
        self.text(input, MAX_FAILURE_REASON_BYTES)
    }

    pub fn diff_stat(&self, input: &str) -> String {
        self.text(input, MAX_DIFF_STAT_BYTES)
    }

    pub fn title(&self, input: &str) -> String {
        self.text(input, MAX_TITLE_BYTES)
    }

    pub fn questions<I, S>(&self, items: I) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        items
            .into_iter()
            .map(|item| self.question(item.as_ref()))
            .take(MAX_QUESTION_COUNT)
            .collect()
    }

    pub fn changed_files<I, S>(&self, items: I) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        items
            .into_iter()
            .map(|item| self.changed_file(item.as_ref()))
            .take(MAX_CHANGED_FILE_COUNT)
            .collect()
    }

    pub fn text(&self, input: &str, max_bytes: usize) -> String {
        let escaped = escape_controls(input);
        let redacted = self.redact(&escaped);
        truncate_bytes(&redacted, max_bytes)
    }

    fn redact(&self, input: &str) -> String {
        let input = self.secrets.iter().fold(input.to_owned(), |input, secret| {
            input.replace(secret, TOKEN_PLACEHOLDER)
        });
        redact_tokens(&redact_home_paths(
            &self.redact_tilde_paths(&input),
            &self.homes,
        ))
    }

    fn redact_tilde_paths(&self, input: &str) -> String {
        let mut output = String::with_capacity(input.len());
        let mut rest = input;
        while !rest.is_empty() {
            if let Some(start) = find_tilde_path(rest) {
                output.push_str(&rest[..start]);
                output.push_str(PATH_PLACEHOLDER);
                rest = skip_path_token(&rest[start..]);
            } else {
                output.push_str(rest);
                break;
            }
        }
        output
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !value.is_empty() && !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn redact_home_paths(input: &str, homes: &[String]) -> String {
    if homes.is_empty() {
        return input.to_owned();
    }
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while !rest.is_empty() {
        let Some((start, home)) = next_home_path(rest, homes) else {
            output.push_str(rest);
            break;
        };
        output.push_str(&rest[..start]);
        output.push_str(PATH_PLACEHOLDER);
        rest = skip_path_token(&rest[start + home.len()..]);
        if rest.starts_with(home) {
            // Avoid a tight loop if a home path is a prefix of itself after skip.
            rest = &rest[home.chars().next().map(char::len_utf8).unwrap_or(1)..];
        }
    }
    output
}

fn next_home_path<'a>(input: &'a str, homes: &[String]) -> Option<(usize, &'a str)> {
    let mut found: Option<(usize, &str)> = None;
    for home in homes {
        let mut search = input;
        let mut offset = 0;
        while let Some(local) = search.find(home.as_str()) {
            let start = offset + local;
            if is_path_token_start(input, start) {
                let candidate = &input[start..start + home.len()];
                if found.is_none_or(|(best, current)| {
                    start < best || (start == best && candidate.len() > current.len())
                }) {
                    found = Some((start, candidate));
                }
            }
            let advance = local + home.len();
            search = &search[advance..];
            offset += advance;
        }
    }
    found
}

fn find_tilde_path(input: &str) -> Option<usize> {
    input.char_indices().find_map(|(index, character)| {
        (character == '~'
            && is_path_token_start(input, index)
            && matches!(input[index + 1..].chars().next(), None | Some('/')))
        .then_some(index)
    })
}

fn is_path_token_start(input: &str, index: usize) -> bool {
    index == 0
        || input[..index].chars().next_back().is_some_and(|character| {
            character.is_whitespace()
                || matches!(character, '"' | '\'' | '=' | ':' | ',' | '(' | '[')
        })
}

fn skip_path_token(input: &str) -> &str {
    let end = input
        .char_indices()
        .find(|(_, character)| {
            character.is_whitespace() || matches!(character, '"' | '\'' | ',' | ')' | ']' | ';')
        })
        .map(|(index, _)| index)
        .unwrap_or(input.len());
    &input[end..]
}

fn redact_tokens(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while !rest.is_empty() {
        if let Some(stripped) = strip_prefix_ignore_ascii_case(rest, "Bearer")
            && stripped.starts_with(char::is_whitespace)
        {
            output.push_str("Bearer ");
            let value = stripped.trim_start_matches(char::is_whitespace);
            rest = skip_token_run(value);
            output.push_str(TOKEN_PLACEHOLDER);
            continue;
        }
        if rest.starts_with("sk-") && sk_token_len(&rest[3..]) >= MIN_SK_TOKEN {
            output.push_str(TOKEN_PLACEHOLDER);
            rest = skip_token_run(&rest[3..]);
            continue;
        }
        let ascii_run = ascii_run_len(rest);
        if ascii_run >= MIN_HEX_TOKEN
            && rest.as_bytes()[..ascii_run]
                .iter()
                .all(u8::is_ascii_hexdigit)
        {
            output.push_str(TOKEN_PLACEHOLDER);
            rest = &rest[ascii_run..];
            continue;
        }
        if let Some(len) = base64_token_len(rest) {
            output.push_str(TOKEN_PLACEHOLDER);
            rest = &rest[len..];
            continue;
        }
        let next = rest.chars().next().expect("non-empty remainder");
        output.push(next);
        rest = &rest[next.len_utf8()..];
    }
    output
}

fn strip_prefix_ignore_ascii_case<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    if input.is_char_boundary(prefix.len()) && input[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&input[prefix.len()..])
    } else {
        None
    }
}

fn ascii_run_len(input: &str) -> usize {
    input
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_alphanumeric())
        .count()
}

fn sk_token_len(input: &str) -> usize {
    input
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'-'))
        .count()
}

fn skip_token_run(input: &str) -> &str {
    let len = input
        .as_bytes()
        .iter()
        .take_while(|byte| {
            byte.is_ascii_graphic() && !matches!(*byte, b'"' | b'\'' | b',' | b')' | b']')
        })
        .count();
    &input[len..]
}

fn base64_token_len(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut len = 0;
    while len < bytes.len() && is_base64_body(bytes[len]) {
        len += 1;
    }
    let mut padded = len;
    while padded < bytes.len() && bytes[padded] == b'=' && padded - len < 2 {
        padded += 1;
    }
    if len >= MIN_BASE64_TOKEN
        && (padded == bytes.len()
            || !bytes[padded].is_ascii_alphanumeric()
                && bytes[padded] != b'+'
                && bytes[padded] != b'/')
    {
        Some(padded)
    } else {
        None
    }
}

fn is_base64_body(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/')
}

fn escape_controls(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for character in input.chars() {
        match character {
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            other if other.is_control() => {
                escaped.push_str(&format!("\\u{{{:04x}}}", u32::from(other)));
            }
            other => escaped.push(other),
        }
    }
    escaped
}

fn truncate_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_owned();
    }
    let mut end = max_bytes;
    while end > 0 && !input.is_char_boundary(end) {
        end -= 1;
    }
    input[..end].to_owned()
}
