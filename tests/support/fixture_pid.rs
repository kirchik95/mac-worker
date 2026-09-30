//! Fixture process ids that cannot collide with a live pid.
//!
//! Real pids never exceed 99_999 on macOS or 4_194_304 on Linux. Keep this
//! constant equal to `src/fixture_pid.rs`.

/// Added to every made-up fixture pid. Above both operating-system maxima.
pub const FIXTURE_PID_BASE: u32 = 5_000_000;

/// `FIXTURE_PID_BASE + seed`. `inspect_process` converts the pid to `c_int`,
/// so the result stays at or below `i32::MAX`.
pub fn fixture_pid(seed: u32) -> u32 {
    let pid = FIXTURE_PID_BASE
        .checked_add(seed)
        .expect("fixture pid fits in u32");
    assert!(
        i32::try_from(pid).is_ok(),
        "fixture pid {pid} must fit in c_int"
    );
    pid
}
