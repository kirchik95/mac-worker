use super::session_error;
use crate::error::WorkerError;
pub const WORKSPACE_TOKEN: &str = "@@MW_WORKSPACE@@";
pub const SESSION_TOKEN: &str = "@@MW_SESSION@@";
fn boundary(byte: u8) -> bool {
    !(byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
}
pub fn rewrite_root(text: &[u8], from: &str, to: &str) -> Vec<u8> {
    let from = from.as_bytes();
    if from.is_empty() {
        return text.to_vec();
    }
    let mut output = Vec::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        if text[i..].starts_with(from)
            && (i == 0 || boundary(text[i - 1]))
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
pub fn materialize(text: &[u8], workspace: &str, session_id: &str) -> Vec<u8> {
    replace(
        &replace(text, WORKSPACE_TOKEN.as_bytes(), workspace.as_bytes()),
        SESSION_TOKEN.as_bytes(),
        session_id.as_bytes(),
    )
}
