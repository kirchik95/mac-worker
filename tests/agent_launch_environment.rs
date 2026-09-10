#[allow(dead_code)]
mod support;

use std::fs;

use mac_worker::{
    agent::AgentKind,
    agent::{prebind_login_request, render_prebind_shell},
    agent_facts::AgentAuth,
    job::{ClientId, CommandSpec, JobId, LeaseRecord, LeaseToken, RequestFingerprintMaterial},
    process::{ProcessPolicy, ProcessRequest, ProcessRunner, SystemProcessRunner},
    supervisor::LaunchPlan,
    task::{BaseOid, GitIdentity, TaskId},
    turn::{EnvProfile, TurnMaterial, TurnSection},
};
use support::agent_launch_fixture::{
    DiagnosticProcessRunner, FixtureLayout, LOGIN_BAD, LOGIN_GOOD, PARENT_ONLY, PROFILE_GOOD,
    PROFILE_NAME, assert_subprocess_success, classify_cursor_process_result,
    cursor_auth_for_profile, cursor_probe, cursor_profile_auth, empty_base_path,
    fixture_home_from_env, fixture_only_path, prebind_status_auth, refresh_cursor_facts,
    run_launch_plan_cursor_auth, skip_unless_subtest, write_profile_entries,
};

fn cursor_status_launch_plan(fixture: &FixtureLayout, profile_name: &str) -> LaunchPlan {
    let profile_path = fixture
        .home
        .join(format!(".config/mac-worker/env/{profile_name}.env"));
    let profile = EnvProfile::load(&profile_path).unwrap();
    let turn = TurnMaterial::from_prompt(
        TaskId::generate(),
        1,
        AgentKind::Cursor,
        None,
        None,
        mac_worker::agent::PermissionPolicy::Unattended,
        mac_worker::agent::TurnLimits::new(60_000, None, None).unwrap(),
        "a".repeat(40).parse::<BaseOid>().unwrap(),
        b"prompt",
        Some(profile_name.into()),
        uuid::Uuid::from_u128(1),
        false,
    )
    .unwrap();
    let section = TurnSection::new(
        turn.clone(),
        "b".repeat(64),
        GitIdentity::new("Ada", "ada@example.test").unwrap(),
    )
    .unwrap();
    let command = CommandSpec::shell(
        render_prebind_shell(&["cursor-agent".into(), "status".into()]).unwrap(),
    )
    .unwrap();
    let material = RequestFingerprintMaterial::new(
        JobId::generate(),
        ClientId::generate(),
        LeaseToken::generate(),
        100,
        "mini-1".into(),
        "b".repeat(64),
        "c".repeat(64),
        turn.digest(),
        String::new(),
        30_000,
        "heavy".into(),
        command.clone(),
    )
    .unwrap();
    let lease = LeaseRecord::new(&material, material.fingerprint(), 100, 30_100).unwrap();
    LaunchPlan::turn(
        &command,
        &lease,
        &section,
        &fixture.home,
        &profile,
        section.git_identity(),
    )
    .unwrap()
}

fn clobber_fixture() -> (tempfile::TempDir, FixtureLayout) {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_zprofile(&format!(
        "export PATH=\"{}\"\nexport CURSOR_API_KEY={LOGIN_BAD}\n",
        fixture_only_path(&fixture.home.join("bin"))
    ));
    fixture.install_home_cursor(PROFILE_GOOD);
    fixture.write_profile_env(PROFILE_NAME, &format!("CURSOR_API_KEY={PROFILE_GOOD}\n"));
    (temp, fixture)
}

#[test]
fn refresh_facts_reports_unauthenticated_when_login_startup_clobbers_profile() {
    let (_temp, fixture) = clobber_fixture();
    let auth = cursor_auth_for_profile(&fixture.home, PROFILE_NAME);
    assert_eq!(
        auth,
        AgentAuth::Unauthenticated,
        "facts must follow effective login-shell credentials, not profile-only direct exec"
    );
    let plan = cursor_status_launch_plan(&fixture, PROFILE_NAME);
    assert_eq!(
        run_launch_plan_cursor_auth(&plan),
        AgentAuth::Unauthenticated,
        "LaunchPlan exec must follow the same login-shell credential boundary as facts"
    );
}

#[test]
fn refresh_facts_uses_supplied_home_not_process_home_wrapper() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_zprofile(&format!(
        "export PATH=\"{}\"\n",
        fixture_only_path(&fixture.home.join("bin"))
    ));
    fixture.install_home_cursor(PROFILE_GOOD);
    fixture.write_profile_env(PROFILE_NAME, &format!("CURSOR_API_KEY={PROFILE_GOOD}\n"));

    let other = tempfile::tempdir().unwrap();
    let other_home = other.path().join("other-home");
    fs::create_dir_all(&other_home).unwrap();

    assert_subprocess_success(
        "refresh_facts_uses_supplied_home_not_process_home",
        &[
            ("FIXTURE_HOME", fixture.home.to_str().unwrap()),
            ("HOME", other_home.to_str().unwrap()),
            ("PATH", empty_base_path()),
        ],
        true,
    );
}

#[test]
fn refresh_facts_uses_supplied_home_not_process_home() {
    if skip_unless_subtest() {
        return;
    }
    let fixture_home = fixture_home_from_env();
    let auth = cursor_auth_for_profile(&fixture_home, PROFILE_NAME);
    assert_eq!(auth, AgentAuth::Authenticated);
}

#[test]
fn prebind_status_matches_effective_login_shell_for_profile() {
    let (_temp, fixture) = clobber_fixture();
    let entries = write_profile_entries(&format!("CURSOR_API_KEY={PROFILE_GOOD}\n"));
    let auth = prebind_status_auth(&fixture.home, &entries);
    assert_eq!(auth, AgentAuth::Unauthenticated);
}

#[test]
fn profile_credential_is_accepted_when_login_does_not_clobber() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_zprofile(&format!(
        "export PATH=\"{}\"\n",
        fixture_only_path(&fixture.home.join("bin"))
    ));
    fixture.install_home_cursor(PROFILE_GOOD);
    fixture.write_profile_env(PROFILE_NAME, &format!("CURSOR_API_KEY={PROFILE_GOOD}\n"));
    assert_eq!(
        cursor_auth_for_profile(&fixture.home, PROFILE_NAME),
        AgentAuth::Authenticated
    );
    let entries = write_profile_entries(&format!("CURSOR_API_KEY={PROFILE_GOOD}\n"));
    assert_eq!(
        prebind_status_auth(&fixture.home, &entries),
        AgentAuth::Authenticated
    );
    let plan = cursor_status_launch_plan(&fixture, PROFILE_NAME);
    assert_eq!(
        run_launch_plan_cursor_auth(&plan),
        AgentAuth::Authenticated,
        "LaunchPlan exec must match facts/prebind when login startup does not clobber profile"
    );
}

#[test]
fn login_only_credential_is_accepted_by_facts_and_prebind() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_zprofile(&format!(
        "export PATH=\"{}\"\nexport CURSOR_API_KEY={LOGIN_GOOD}\n",
        fixture_only_path(&fixture.home.join("bin"))
    ));
    fixture.install_home_cursor(LOGIN_GOOD);
    fixture.write_profile_env(PROFILE_NAME, "\n");
    assert_eq!(
        cursor_auth_for_profile(&fixture.home, PROFILE_NAME),
        AgentAuth::Authenticated
    );
    assert_eq!(
        prebind_status_auth(&fixture.home, &[]),
        AgentAuth::Authenticated
    );
    let plan = cursor_status_launch_plan(&fixture, PROFILE_NAME);
    assert_eq!(
        run_launch_plan_cursor_auth(&plan),
        AgentAuth::Authenticated,
        "LaunchPlan exec must accept login-only credentials with an empty profile"
    );
}

#[test]
fn profile_only_binary_is_available_when_login_startup_augments_path() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_hermetic_login_zprofile();
    fixture.install_profile_cursor(PROFILE_GOOD);
    fixture.write_profile_env(
        PROFILE_NAME,
        &format!(
            "PATH={}\nCURSOR_API_KEY={PROFILE_GOOD}\n",
            fixture.profile_bin.display()
        ),
    );
    let facts = refresh_cursor_facts(&fixture.home);
    let cursor = cursor_probe(&facts).expect("profile-only binary must still emit cursor probe");
    assert_eq!(
        cursor.version, None,
        "base account must not resolve a binary"
    );
    assert_eq!(
        cursor.auth,
        AgentAuth::Unknown,
        "base auth stays unknown without base binary"
    );
    assert_eq!(
        cursor_profile_auth(&facts, PROFILE_NAME),
        AgentAuth::Authenticated
    );
}

#[test]
fn profile_and_login_binaries_resolve_different_verdicts() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_hermetic_login_zprofile();
    fixture.install_login_cursor(LOGIN_GOOD);
    fixture.install_profile_cursor(PROFILE_GOOD);
    fixture.write_profile_env(
        PROFILE_NAME,
        &format!(
            "PATH={}\nCURSOR_API_KEY={PROFILE_GOOD}\n",
            fixture.profile_bin.display()
        ),
    );
    let facts = refresh_cursor_facts(&fixture.home);
    let cursor = cursor_probe(&facts).expect("cursor probe must be present");
    assert_eq!(
        cursor.auth,
        AgentAuth::Unauthenticated,
        "base account resolves the login startup binary without a matching credential"
    );
    assert_eq!(
        cursor_profile_auth(&facts, PROFILE_NAME),
        AgentAuth::Authenticated,
        "profile account keeps profile PATH ahead of the augmented login PATH"
    );
}

#[test]
fn login_path_override_wins_over_profile_path() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_zprofile(&format!(
        "export PATH=\"{}\"\n",
        fixture_only_path(&fixture.login_bin)
    ));
    fixture.install_login_cursor(LOGIN_BAD);
    fixture.write_profile_env(
        PROFILE_NAME,
        &format!(
            "PATH={}\nCURSOR_API_KEY={PROFILE_GOOD}\n",
            fixture.profile_bin.display()
        ),
    );
    fixture.install_profile_cursor(PROFILE_GOOD);
    assert_eq!(
        cursor_auth_for_profile(&fixture.home, PROFILE_NAME),
        AgentAuth::Unauthenticated
    );
    let entries = write_profile_entries(&format!(
        "PATH={}\nCURSOR_API_KEY={PROFILE_GOOD}\n",
        fixture.profile_bin.display()
    ));
    assert_eq!(
        prebind_status_auth(&fixture.home, &entries),
        AgentAuth::Unauthenticated,
        "prebind must resolve the login PATH override ahead of the profile binary"
    );
    let plan = cursor_status_launch_plan(&fixture, PROFILE_NAME);
    assert_eq!(
        run_launch_plan_cursor_auth(&plan),
        AgentAuth::Unauthenticated,
        "LaunchPlan exec must follow login PATH override ahead of profile PATH"
    );
}

#[test]
fn launch_plan_and_prebind_share_login_shell_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_zprofile(&format!(
        "export PATH=\"{}\"\nexport CURSOR_API_KEY={LOGIN_BAD}\n",
        fixture_only_path(&fixture.home.join("bin"))
    ));
    fixture.install_home_cursor(PROFILE_GOOD);
    fixture.write_profile_env(PROFILE_NAME, &format!("CURSOR_API_KEY={PROFILE_GOOD}\n"));
    let plan = cursor_status_launch_plan(&fixture, PROFILE_NAME);
    let shell = render_prebind_shell(&["cursor-agent".into(), "status".into()]).unwrap();
    assert_eq!(
        plan.program(),
        std::env::current_exe().unwrap().to_str().unwrap()
    );
    assert_eq!(plan.args()[1], mac_worker::prepare_turn::ARG);
    assert_eq!(plan.args()[2], "--");
    assert_eq!(
        plan.prepare_turn_agent_args().map(ToOwned::to_owned),
        Some(vec!["/bin/zsh".into(), "-lc".into(), shell])
    );
    assert!(
        plan.env()
            .iter()
            .any(|(name, value)| name == "CURSOR_API_KEY" && value == PROFILE_GOOD)
    );
    let entries = write_profile_entries(&format!("CURSOR_API_KEY={PROFILE_GOOD}\n"));
    assert_eq!(
        prebind_status_auth(&fixture.home, &entries),
        AgentAuth::Unauthenticated
    );
}

#[test]
fn parent_only_credential_wrapper() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_zprofile(&format!(
        "export PATH=\"{}\"\n",
        fixture_only_path(&fixture.home.join("bin"))
    ));
    fixture.install_home_cursor(LOGIN_GOOD);
    assert_subprocess_success(
        "parent_only_credential_isolated",
        &[
            ("FIXTURE_HOME", fixture.home.to_str().unwrap()),
            ("CURSOR_API_KEY", PARENT_ONLY),
            ("PATH", empty_base_path()),
        ],
        true,
    );
}

#[test]
fn parent_only_credential_isolated() {
    if skip_unless_subtest() {
        return;
    }
    let home = fixture_home_from_env();
    let request =
        prebind_login_request(&["cursor-agent".into(), "status".into()], &home, &[]).unwrap();
    assert!(request.isolate_parent_environment);
    let result = DiagnosticProcessRunner.run(&request).unwrap();
    assert_eq!(
        classify_cursor_process_result(&result),
        AgentAuth::Unauthenticated
    );
}

#[test]
fn non_isolated_runner_still_inherits_parent_credentials_wrapper() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = FixtureLayout::create(temp.path());
    fixture.write_zprofile(&format!(
        "export PATH=\"{}\"\n",
        fixture_only_path(&fixture.home.join("bin"))
    ));
    fixture.install_home_cursor(PARENT_ONLY);
    assert_subprocess_success(
        "non_isolated_runner_still_inherits_parent_credentials",
        &[
            ("FIXTURE_HOME", fixture.home.to_str().unwrap()),
            ("CURSOR_API_KEY", PARENT_ONLY),
            ("PATH", empty_base_path()),
        ],
        true,
    );
}

#[test]
fn non_isolated_runner_still_inherits_parent_credentials() {
    if skip_unless_subtest() {
        return;
    }
    let home = fixture_home_from_env();
    let shell = render_prebind_shell(&["cursor-agent".into(), "status".into()]).unwrap();
    let request = ProcessRequest {
        program: "/bin/zsh".into(),
        args: vec!["-lc".into(), shell.into()],
        environment: vec![("HOME".into(), home.as_os_str().into())],
        environment_remove: Vec::new(),
        stdin: None,
        policy: ProcessPolicy {
            stdout_limit: 4 * 1024,
            stderr_limit: 4 * 1024,
            deadline: std::time::Duration::from_secs(2),
        },
        isolate_parent_environment: false,
    };
    let result = SystemProcessRunner.run(&request).unwrap();
    assert_eq!(
        classify_cursor_process_result(&result),
        AgentAuth::Authenticated
    );
}
