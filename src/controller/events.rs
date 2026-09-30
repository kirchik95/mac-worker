//! Shared contracts for optional, lossy controller lifecycle hints.
//!
//! Saved task state remains authoritative. This module advertises no feature
//! and opens no state or journal. Component facades are filled by later tracks.

pub mod contracts;
pub use contracts::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_and_dispatch_are_safe_contracts() {
        let seq = "9007199254740993".parse::<Seq>().unwrap();
        assert_eq!(serde_json::to_string(&seq).unwrap(), "\"9007199254740993\"");
        assert!("01".parse::<Seq>().is_err());
        let body = EventSelector::Read(ReadQuery {
            after: None,
            limit: 128,
            wait_ms: 15_000,
        })
        .request_body()
        .unwrap();
        assert_eq!(body.as_object().unwrap().len(), 1);
        assert_eq!(body["controller_events"]["op"], "read");
    }

    #[test]
    fn selector_is_one_safe_task_list_key() {
        let body = EventSelector::Read(ReadQuery {
            after: None,
            limit: 128,
            wait_ms: 15_000,
        })
        .request_body()
        .unwrap();
        assert_eq!(
            body,
            serde_json::json!({"controller_events": {
                "op": "read", "after": null, "limit": 128, "wait_ms": 15_000
            }})
        );
    }
}
