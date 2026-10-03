use std::collections::{BTreeMap, HashMap, VecDeque};

use crate::error::WorkerError;

/// Replacement text for secret spans.
pub const SCRUBBED: &str = "[scrubbed]";

/// Structure-aware session secret scrubber.
pub struct Scrubber {
    exact: Vec<ExactNode>,
}

#[derive(Default)]
struct ExactNode {
    edges: BTreeMap<u8, usize>,
    failure: usize,
    longest: usize,
}

/// A JSON line and the number of replaced secret spans.
pub struct ScrubbedLine {
    /// Encoded JSON, preserving untouched bytes.
    pub bytes: Vec<u8>,
    /// Number of secret spans replaced.
    pub replacements: u32,
}

impl Scrubber {
    /// Build a scrubber, ignoring exact secrets shorter than eight bytes.
    pub fn new(exact_secrets: Vec<String>) -> Self {
        // A reversed failure-link trie finds the longest exact match at each
        // starting byte in one backwards pass, including overlapping secrets.
        let mut exact = vec![ExactNode::default()];
        for secret in exact_secrets.into_iter().filter(|secret| secret.len() >= 8) {
            let mut state = 0;
            for byte in secret.bytes().rev() {
                state = if let Some(&next) = exact[state].edges.get(&byte) {
                    next
                } else {
                    let next = exact.len();
                    exact.push(ExactNode::default());
                    exact[state].edges.insert(byte, next);
                    next
                };
            }
            exact[state].longest = secret.len();
        }
        let mut queue: VecDeque<usize> = exact[0].edges.values().copied().collect();
        while let Some(state) = queue.pop_front() {
            let edges: Vec<_> = exact[state].edges.iter().map(|(&b, &s)| (b, s)).collect();
            for (byte, next) in edges {
                let mut failure = exact[state].failure;
                while failure != 0 && !exact[failure].edges.contains_key(&byte) {
                    failure = exact[failure].failure;
                }
                exact[next].failure = exact[failure].edges.get(&byte).copied().unwrap_or(0);
                exact[next].longest = exact[next].longest.max(exact[exact[next].failure].longest);
                queue.push_back(next);
            }
        }
        Self { exact }
    }

    /// Scrub JSON string values without changing object keys.
    pub fn scrub_line(&self, line: &[u8]) -> Result<ScrubbedLine, WorkerError> {
        // Validate first; error messages must never echo transcript contents.
        serde_json::from_slice::<serde_json::Value>(line).map_err(|_| unreadable())?;
        let mut bytes = Vec::with_capacity(line.len());
        let mut replacements = 0;
        let mut copied = 0;
        let mut cursor = 0;
        while cursor < line.len() {
            if line[cursor] != b'"' {
                cursor += 1;
                continue;
            }
            let start = cursor;
            cursor += 1;
            while line[cursor] != b'"' {
                if line[cursor] == b'\\' {
                    cursor += 1;
                }
                cursor += 1;
            }
            cursor += 1;
            let mut after = cursor;
            while after < line.len() && line[after].is_ascii_whitespace() {
                after += 1;
            }
            if line.get(after) == Some(&b':') {
                continue;
            }
            let value: String =
                serde_json::from_slice(&line[start..cursor]).map_err(|_| unreadable())?;
            let (scrubbed, count) = self.scrub_text(&value);
            if count != 0 {
                bytes.extend_from_slice(&line[copied..start]);
                let scrubbed = std::str::from_utf8(&scrubbed).map_err(|_| unreadable())?;
                bytes.extend_from_slice(&serde_json::to_vec(scrubbed).map_err(|_| unreadable())?);
                copied = cursor;
                replacements += count;
            }
        }
        bytes.extend_from_slice(&line[copied..]);
        Ok(ScrubbedLine {
            bytes,
            replacements,
        })
    }

    fn scrub_text(&self, text: &str) -> (Vec<u8>, u32) {
        let input = text.as_bytes();
        let mut spans = vec![0; input.len()];
        if self.exact.len() > 1 {
            let mut state = 0;
            for index in (0..input.len()).rev() {
                let byte = input[index];
                while state != 0 && !self.exact[state].edges.contains_key(&byte) {
                    state = self.exact[state].failure;
                }
                state = self.exact[state].edges.get(&byte).copied().unwrap_or(0);
                spans[index] = self.exact[state].longest;
            }
        }
        pem_spans(input, &mut spans);
        let mut output = Vec::new();
        let mut count = 0;
        let mut copied = 0;
        let mut index = 0;
        while index < input.len() {
            let (pattern_length, keep) = if index == 0 || !word(input[index - 1]) {
                token_span(&input[index..])
            } else {
                (0, 0)
            };
            let length = spans[index].max(pattern_length);
            if length == 0 {
                index += 1;
                continue;
            }
            output.extend_from_slice(&input[copied..index]);
            // Exact secrets win ties: an exact Bearer secret is removed whole.
            if pattern_length > spans[index] {
                output.extend_from_slice(&input[index..index + keep]);
            }
            output.extend_from_slice(SCRUBBED.as_bytes());
            count += 1;
            index += length;
            copied = index;
        }
        if count != 0 {
            output.extend_from_slice(&input[copied..]);
        }
        (output, count)
    }
}

fn unreadable() -> WorkerError {
    WorkerError::task("SESSION_UNREADABLE", "session line is not valid JSON")
}

fn word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

type TokenPattern<'a> = (&'a [u8], usize, fn(u8) -> bool, usize);

fn token_span(input: &[u8]) -> (usize, usize) {
    let (prefix, minimum, allowed, keep): TokenPattern<'_> = match input[0] {
        b'B' if input.starts_with(b"Bearer ") => (
            b"Bearer ",
            16,
            |b| b.is_ascii_alphanumeric() || b"._~+/=-".contains(&b),
            7,
        ),
        b's' if input.starts_with(b"sk-ant-") => (b"sk-ant-", 16, |b| word(b) || b == b'-', 0),
        b's' if input.starts_with(b"sk-") => (b"sk-", 20, |b| word(b) || b == b'-', 0),
        b'g' if input.starts_with(b"github_pat_") => (b"github_pat_", 40, word, 0),
        b'g' if input.len() >= 4
            && &input[..2] == b"gh"
            && b"posur".contains(&input[2])
            && input[3] == b'_' =>
        {
            (&input[..4], 30, |b| b.is_ascii_alphanumeric(), 0)
        }
        b'x' if input.len() >= 5
            && &input[..3] == b"xox"
            && b"abprs".contains(&input[3])
            && input[4] == b'-' =>
        {
            (
                &input[..5],
                10,
                |b| b.is_ascii_alphanumeric() || b == b'-',
                0,
            )
        }
        b'A' if input.starts_with(b"AKIA") && input.len() >= 20 => {
            if input[4..20]
                .iter()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                && !input.get(20).is_some_and(u8::is_ascii_alphanumeric)
            {
                return (20, 0);
            }
            return (0, 0);
        }
        _ => return (0, 0),
    };
    let length = input[prefix.len()..]
        .iter()
        .take_while(|&&b| allowed(b))
        .count();
    if length >= minimum {
        (prefix.len() + length, keep)
    } else {
        (0, 0)
    }
}

fn pem_spans(input: &[u8], spans: &mut [usize]) {
    let mut pending: HashMap<&[u8], usize> = HashMap::new();
    let mut index = 0;
    while index < input.len() {
        let (prefix, begin) = if input[index..].starts_with(b"-----BEGIN ") {
            (11, true)
        } else if input[index..].starts_with(b"-----END ") {
            (9, false)
        } else {
            index += 1;
            continue;
        };
        let start = index;
        let label_start = index + prefix;
        index = label_start;
        // A delimiter label cannot contain a dash or a line break. Advancing
        // past the label prevents repeated scans of malformed long headers.
        while index < input.len() && !matches!(input[index], b'-' | b'\n' | b'\r') {
            index += 1;
        }
        let label = &input[label_start..index];
        if !(label == b"PRIVATE KEY" || label.ends_with(b" PRIVATE KEY"))
            || !input[index..].starts_with(b"-----")
        {
            continue;
        }
        index += 5;
        if begin {
            if start == 0 || !word(input[start - 1]) {
                pending.entry(label).or_insert(start);
            }
        } else if let Some(start) = pending.remove(label) {
            spans[start] = spans[start].max(index - start);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scrub(text: &str, secrets: Vec<String>) -> (String, u32) {
        let line = serde_json::to_vec(text).unwrap();
        let result = Scrubber::new(secrets).scrub_line(&line).unwrap();
        (
            serde_json::from_slice(&result.bytes).unwrap(),
            result.replacements,
        )
    }

    #[test]
    fn all_token_shapes_and_counts() {
        let tokens = [
            format!("Bearer {}", "a".repeat(16)),
            format!("sk-ant-{}", "a".repeat(16)),
            format!("sk-{}", "a".repeat(20)),
            format!("github_pat_{}", "a".repeat(40)),
            "AKIA0123456789ABCDEF".into(),
        ];
        for token in tokens {
            let expected = if token.starts_with("Bearer ") {
                "Bearer [scrubbed]"
            } else {
                SCRUBBED
            };
            assert_eq!(scrub(&token, vec![]), (expected.into(), 1));
        }
        for prefix in ["ghp_", "gho_", "ghs_", "ghu_", "ghr_"] {
            assert_eq!(
                scrub(&format!("{prefix}{}", "a".repeat(30)), vec![]),
                (SCRUBBED.into(), 1)
            );
        }
        for prefix in ["xoxa-", "xoxb-", "xoxp-", "xoxr-", "xoxs-"] {
            assert_eq!(
                scrub(&format!("{prefix}{}", "a".repeat(10)), vec![]),
                (SCRUBBED.into(), 1)
            );
        }
        assert_eq!(
            scrub(
                &format!("sk-{} ghp_{}", "a".repeat(20), "b".repeat(30)),
                vec![]
            ),
            ("[scrubbed] [scrubbed]".into(), 2)
        );
    }

    #[test]
    fn false_positives_and_boundaries_stay_byte_identical() {
        let values = [
            "disk-usage",
            "task-abcdefghijklmnopqrst",
            "sk-learn",
            "0123456789abcdef0123456789abcdef0123456789",
            "550e8400-e29b-41d4-a716-446655440000",
            "sk-1234567890123456789",
            "sk-ant-123456789012345",
            "ghp_12345678901234567890123456789",
            "github_pat_123456789012345678901234567890123456789",
            "Bearer 123456789012345",
            "xoxb-123456789",
            "AKIA0123456789ABCDEFA",
            "AKIA0123456789ABCDEFz",
        ];
        for value in values {
            assert_eq!(scrub(value, vec![]), (value.into(), 0));
        }
        for prefix in ["a", "9", "_"] {
            let value = format!("{prefix}sk-{}", "a".repeat(20));
            assert_eq!(scrub(&value, vec![]), (value.clone(), 0));
        }
        let line = b" { \"plain\" : \"\\u0061\", \"n\": 2e0 } \n";
        assert_eq!(Scrubber::new(vec![]).scrub_line(line).unwrap().bytes, line);
    }

    #[test]
    fn escapes_nested_text_and_keys() {
        let token = format!("sk-{}", "a".repeat(20));
        let nested = format!("\n{{\"token\":\"{token}\"}}");
        let line = serde_json::to_vec(&serde_json::json!({token.clone(): [nested, {"v": token}]}))
            .unwrap();
        let result = Scrubber::new(vec![]).scrub_line(&line).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&result.bytes).unwrap();
        assert_eq!(result.replacements, 2);
        assert_eq!(value[&token][0], "\n{\"token\":\"[scrubbed]\"}");
        assert_eq!(value[&token][1]["v"], SCRUBBED);
    }

    #[test]
    fn exact_secrets_escaping_short_and_overlapping() {
        let secret = "secret\"\\\n雪";
        assert_eq!(
            scrub(&format!("x{secret}y"), vec![secret.into()]),
            ("x[scrubbed]y".into(), 1)
        );
        assert_eq!(
            scrub(
                "short abcd12345678 abcd12345678",
                vec!["short".into(), "abcd1234".into(), "abcd12345678".into()]
            ),
            ("short [scrubbed] [scrubbed]".into(), 2)
        );
        assert_eq!(scrub("éééé", vec!["éééé".into()]), (SCRUBBED.into(), 1));
    }

    #[test]
    fn pem_blocks_require_matching_end() {
        for label in [
            "PRIVATE KEY",
            "RSA PRIVATE KEY",
            "EC PRIVATE KEY",
            "ENCRYPTED PRIVATE KEY",
            "OPENSSH PRIVATE KEY",
        ] {
            let text =
                format!("before -----BEGIN {label}-----\nsynthetic\n-----END {label}----- after");
            assert_eq!(scrub(&text, vec![]), ("before [scrubbed] after".into(), 1));
        }
        for text in [
            "-----BEGIN RSA PRIVATE KEY-----\nx\n-----END EC PRIVATE KEY-----",
            "-----BEGIN PUBLIC KEY-----\nx\n-----END PUBLIC KEY-----",
            "-----BEGIN PRIVATE KEY-----\nx",
        ] {
            assert_eq!(scrub(text, vec![]), (text.into(), 0));
        }
    }

    #[test]
    fn pem_labels_and_preceding_word_guards() {
        for text in [
            "-----BEGIN NOTPRIVATE KEY-----\nx\n-----END NOTPRIVATE KEY-----",
            "a-----BEGIN PRIVATE KEY-----\nx\n-----END PRIVATE KEY-----",
        ] {
            assert_eq!(scrub(text, vec![]), (text.into(), 0));
        }
    }

    #[test]
    fn exact_failure_links_and_escaped_token_bytes() {
        assert_eq!(
            scrub(
                "abcdefgh bcdefghi abcdefgh",
                vec!["abcdefgh".into(), "bcdefghi".into(), "zzabcdefgh".into()]
            ),
            ("[scrubbed] [scrubbed] [scrubbed]".into(), 3)
        );
        let line = br#"{"v":"\u0073k-aaaaaaaaaaaaaaaaaaaa","same":"\u0062"}"#;
        let result = Scrubber::new(vec![]).scrub_line(line).unwrap();
        assert_eq!(result.bytes, br#"{"v":"[scrubbed]","same":"\u0062"}"#);
        assert_eq!(result.replacements, 1);
    }

    #[test]
    fn invalid_json_is_unreadable() {
        for line in [
            b"{".as_slice(),
            b"{\"a\":1,}",
            b"\"bad\\q\"",
            b"true false",
            b"\"\xff\"",
            b"",
            b"[1 2]",
        ] {
            assert!(matches!(
                Scrubber::new(vec![]).scrub_line(line),
                Err(WorkerError::Task {
                    code: "SESSION_UNREADABLE",
                    ..
                })
            ));
        }
    }

    #[test]
    fn five_mib_synthetic_image_line() {
        let image = "A".repeat(5 * 1024 * 1024);
        let token = format!("sk-{}", "a".repeat(20));
        let line = serde_json::to_vec(&serde_json::json!({"image": image, "text": token})).unwrap();
        let result = Scrubber::new(vec!["synthetic-secret".into()])
            .scrub_line(&line)
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&result.bytes).unwrap();
        assert_eq!(value["image"], image);
        assert_eq!(value["text"], SCRUBBED);
        assert_eq!(result.replacements, 1);
    }
}
