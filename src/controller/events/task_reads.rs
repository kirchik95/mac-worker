//! Independent task-only event reads; Phase A module placement is temporary.

use crate::{error::WorkerError, task::TaskId};

pub const REPAIR_MAX_DIRECTORY_ENTRIES: usize = 100_000;

/// Validate the names-only registry before reading any task or queue record.
pub fn sorted_task_ids(names: Vec<Vec<u8>>) -> Result<Vec<TaskId>, WorkerError> {
    if names.len() > REPAIR_MAX_DIRECTORY_ENTRIES {
        return Err(WorkerError::Unavailable(
            "CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE: repair unavailable, registry too large"
                .into(),
        ));
    }
    let mut ids = Vec::with_capacity(names.len());
    for name in names {
        if name == b".mac-worker-rooted-fs" || crate::rooted_fs::is_private_replacement_name(&name)
        {
            continue;
        }
        let text = std::str::from_utf8(&name).map_err(|_| invalid_state())?;
        let id_text = text.strip_suffix(".json").ok_or_else(invalid_state)?;
        ids.push(id_text.parse::<TaskId>().map_err(|_| invalid_state())?);
    }
    ids.sort_unstable_by_key(TaskId::to_string);
    Ok(ids)
}

fn invalid_state() -> WorkerError {
    WorkerError::Unavailable("CONTROLLER_EVENTS_UNAVAILABLE: invalid task state".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(number: u128) -> TaskId {
        TaskId::new(uuid::Uuid::from_u128(number))
    }

    fn name(number: u128) -> Vec<u8> {
        format!("{}.json", id(number)).into_bytes()
    }

    #[test]
    fn repair_names_are_sorted_keys_and_residue_is_excluded() {
        let names = vec![
            name(3),
            b".mac-worker-rooted-fs".to_vec(),
            name(1),
            b"replace-00000000-0000-0000-0000-000000000001".to_vec(),
            name(2),
        ];
        assert_eq!(sorted_task_ids(names).unwrap(), vec![id(1), id(2), id(3)]);
    }

    #[test]
    fn registry_over_cap_is_rejected_before_name_validation() {
        let error = sorted_task_ids(vec![b"invalid".to_vec(); 100_001]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("CONTROLLER_EVENTS_REPAIR_REGISTRY_TOO_LARGE")
        );
    }

    #[test]
    fn residue_counts_toward_cap_but_at_cap_is_admitted() {
        let names = vec![b".mac-worker-rooted-fs".to_vec(); 100_000];
        assert!(sorted_task_ids(names.clone()).unwrap().is_empty());
        let mut over = names;
        over.push(name(1));
        assert!(sorted_task_ids(over).is_err());
    }

    #[test]
    fn unsafe_registry_names_are_rejected() {
        for invalid in [
            b"not-a-task.json".as_slice(),
            b"00000000-0000-0000-0000-000000000001.json",
            b"0000000000000000000000000000000A.json",
            b"00000000000000000000000000000001",
            b"replace-invalid",
            b"\xff.json",
        ] {
            assert!(
                sorted_task_ids(vec![invalid.to_vec()]).is_err(),
                "{invalid:?}"
            );
        }
    }
}
