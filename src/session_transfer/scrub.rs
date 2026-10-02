use crate::error::WorkerError;
pub const SCRUBBED: &str = "[scrubbed]";
pub struct Scrubber {
    _secrets: Vec<String>,
}
pub struct ScrubbedLine {
    pub bytes: Vec<u8>,
    pub replacements: u32,
}
impl Scrubber {
    pub fn new(exact_secrets: Vec<String>) -> Self {
        Self {
            _secrets: exact_secrets
                .into_iter()
                .filter(|secret| secret.len() >= 8)
                .collect(),
        }
    }
    pub fn scrub_line(&self, line: &[u8]) -> Result<ScrubbedLine, WorkerError> {
        Ok(ScrubbedLine {
            bytes: line.to_vec(),
            replacements: 0,
        })
    }
}
