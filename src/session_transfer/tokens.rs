use super::session_error;
use crate::error::WorkerError;
pub const WORKSPACE_TOKEN: &str = "@@MW_WORKSPACE@@";
pub const SESSION_TOKEN: &str = "@@MW_SESSION@@";
fn boundary(byte: u8) -> bool {
    byte.is_ascii() && !(byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}
fn left_boundary(text: &[u8], index: usize) -> bool {
    if index == 0 || boundary(text[index - 1]) {
        return true;
    }
    if !b"nrtbf\"/".contains(&text[index - 1]) {
        return false;
    }
    let backslashes = text[..index - 1]
        .iter()
        .rev()
        .take_while(|&&byte| byte == b'\\')
        .count();
    backslashes % 2 == 1
}
/// Rewrite roots only at path boundaries. ASCII alphanumerics, `_`, `.`, `-`
/// and all non-ASCII bytes are name characters on both sides. On the left,
/// JSON escapes (`n`, `r`, `t`, `b`, `f`, `"`, `/`) after an odd run of
/// backslashes also delimit paths; an escaped backslash plus literal `n`
/// does not. A following backslash and the start/end of text are boundaries.
pub fn rewrite_root(text: &[u8], from: &str, to: &str) -> Vec<u8> {
    let from = from.as_bytes();
    if from.is_empty() {
        return text.to_vec();
    }
    let mut output = Vec::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        if text[i..].starts_with(from)
            && left_boundary(text, i)
            && (i + from.len() == text.len() || boundary(text[i + from.len()]))
        {
            output.extend_from_slice(to.as_bytes());
            i += from.len();
        } else {
            output.push(text[i]);
            i += 1;
        }
    }
    output
}
fn replace(text: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    if from.is_empty() {
        return text.to_vec();
    }
    let mut output = Vec::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        if text[i..].starts_with(from) {
            output.extend_from_slice(to);
            i += from.len();
        } else {
            output.push(text[i]);
            i += 1;
        }
    }
    output
}
fn json_safe_path(path: &str) -> bool {
    !path
        .bytes()
        .any(|b| b == b'"' || b == b'\\' || b.is_ascii_control())
}
pub fn normalize(text: &[u8], roots: &[&str], session_id: &str) -> Result<Vec<u8>, WorkerError> {
    if [WORKSPACE_TOKEN, SESSION_TOKEN]
        .iter()
        .any(|token| text.windows(token.len()).any(|w| w == token.as_bytes()))
    {
        return Err(session_error(
            "SESSION_UNREADABLE",
            "session contains a reserved token",
        ));
    }
    if roots.iter().any(|root| !json_safe_path(root)) {
        return Err(session_error(
            "SESSION_UNREADABLE",
            "session path is not JSON-safe",
        ));
    }
    let mut roots = roots.to_vec();
    roots.sort_by_key(|root| std::cmp::Reverse(root.len()));
    let mut output = text.to_vec();
    for root in roots {
        output = rewrite_root(&output, root, WORKSPACE_TOKEN);
    }
    Ok(replace(
        &output,
        session_id.as_bytes(),
        SESSION_TOKEN.as_bytes(),
    ))
}
pub fn materialize(text: &[u8], workspace: &str, session_id: &str) -> Result<Vec<u8>, WorkerError> {
    if !json_safe_path(workspace) {
        return Err(session_error(
            "SESSION_PLACEMENT_FAILED",
            "workspace path is not JSON-safe",
        ));
    }
    Ok(replace(
        &replace(text, WORKSPACE_TOKEN.as_bytes(), workspace.as_bytes()),
        SESSION_TOKEN.as_bytes(),
        session_id.as_bytes(),
    ))
}
