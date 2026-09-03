mod support;

use std::{
    ffi::OsString,
    fs,
    os::unix::{fs::symlink, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::Mutex,
    time::Duration,
};

use mac_worker::{
    error::WorkerError,
    inputs::{InputOrigin, InputSelector, RelativePath, SelectedInputKind},
    process::{ProcessRequest, ProcessResult, ProcessRunner, SystemProcessRunner},
    project::{ProjectContext, ProjectInspector},
    project_config::SnapshotSettings,
};
use proptest::{prelude::*, test_runner::Config as ProptestConfig};

use support::{GitRepo, create_directory};

const PROPERTY_CASES: u32 = 256;
const MAX_GENERATED_PATH_BYTES: usize = 1_024;

fn generated_component() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            Just('a'),
            Just('Z'),
            Just('0'),
            Just('-'),
            Just('_'),
            Just(' '),
            Just('\t'),
            Just('\n'),
            Just('é'),
            Just('界'),
            Just('🙂'),
        ],
        1..=24,
    )
    .prop_map(|characters| {
        let mut component = String::from("c");
        component.extend(characters);
        component
    })
}

fn valid_relative_path() -> impl Strategy<Value = String> {
    prop::collection::vec(generated_component(), 0..=8).prop_map(|mut generated| {
        let mut components = vec![
            "space name".to_owned(),
            "tab\tname".to_owned(),
            "line\nname".to_owned(),
            "é界🙂".to_owned(),
        ];
        components.append(&mut generated);
        components.join("/")
    })
}

fn plain_component() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            Just('a'),
            Just('b'),
            Just('Z'),
            Just('0'),
            Just('-'),
            Just('_')
        ],
        1..=32,
    )
    .prop_map(|characters| characters.into_iter().collect())
}

fn invalid_relative_path() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        plain_component().prop_map(|component| format!("/{component}").into_bytes()),
        plain_component().prop_map(|component| format!("../{component}").into_bytes()),
        (plain_component(), plain_component())
            .prop_map(|(left, right)| format!("{left}/../{right}").into_bytes()),
        (plain_component(), plain_component())
            .prop_map(|(left, right)| format!("{left}/./{right}").into_bytes()),
        plain_component().prop_map(|component| format!(".git/{component}").into_bytes()),
        (plain_component(), plain_component())
            .prop_map(|(left, right)| format!("{left}/.git/{right}").into_bytes()),
        (plain_component(), plain_component())
            .prop_map(|(left, right)| format!("{left}//{right}").into_bytes()),
        (plain_component(), plain_component())
            .prop_map(|(left, right)| format!("{left}\\{right}").into_bytes()),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: PROPERTY_CASES,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn relative_path_parse_display_and_reparse_are_stable(raw in valid_relative_path()) {
        // Catches lossy UTF-8 handling, whitespace splitting, diagnostic
        // control-byte leakage, and serialization that cannot be reparsed.
        prop_assert!(raw.len() <= MAX_GENERATED_PATH_BYTES);
        let parsed = RelativePath::parse(raw.as_bytes()).expect("generated path is valid");
        prop_assert_eq!(parsed.as_str().as_bytes(), raw.as_bytes());

        let expected_display = raw.replace('\t', "\\t").replace('\n', "\\n");
        prop_assert_eq!(parsed.to_string(), expected_display);

        let reparsed = RelativePath::parse(parsed.as_str().as_bytes())
            .expect("serialized valid path remains valid");
        prop_assert_eq!(reparsed, parsed);
    }

    #[test]
    fn absolute_traversal_and_reserved_paths_are_rejected(raw in invalid_relative_path()) {
        // Catches weakening any absolute, traversal, separator, or .git
        // rejection branch while keeping generated failures bounded.
        prop_assert!(raw.len() <= MAX_GENERATED_PATH_BYTES);
        let error = RelativePath::parse(&raw).expect_err("generated path is invalid");
        prop_assert_eq!(error.code, "INVALID_PATH");
        prop_assert_eq!(error.total_path_count, 0);
    }
}

fn settings(
    include_untracked: &[&str],
    include_empty_dirs: &[&str],
    allow_sensitive: &[&str],
) -> SnapshotSettings {
    SnapshotSettings {
        include_untracked: include_untracked
            .iter()
            .map(|value| (*value).into())
            .collect(),
        include_empty_dirs: include_empty_dirs
            .iter()
            .map(|value| (*value).into())
            .collect(),
        allow_sensitive: allow_sensitive
            .iter()
            .map(|value| (*value).into())
            .collect(),
    }
}

fn context(repo: &GitRepo) -> ProjectContext {
    ProjectInspector::new(&SystemProcessRunner)
        .inspect(repo.root())
        .expect("inspect Git fixture")
}

fn select(
    repo: &GitRepo,
    settings: &SnapshotSettings,
) -> Result<mac_worker::inputs::InputSelection, mac_worker::inputs::SelectionFailure> {
    InputSelector::new(&SystemProcessRunner).select(&context(repo), settings)
}

#[test]
fn relative_paths_preserve_spaces_unicode_and_newlines_but_reject_escape() {
    // Catches decoding Git filenames lossily or allowing a selected name to
    // escape the worktree root.
    for accepted in [
        "src/a b.rs",
        "fixtures/привет.txt",
        "fixtures/line\nbreak.txt",
    ] {
        let parsed = RelativePath::parse(accepted.as_bytes()).unwrap();
        assert_eq!(parsed.as_str(), accepted);
        assert_eq!(parsed.as_path(), Path::new(accepted));
    }
    for rejected in [
        b"".as_slice(),
        b"/absolute",
        b"../escape",
        b"a/../../escape",
        b"a//empty",
        b"a/./same",
        b".git/config",
        b"bad\0name",
        b"windows\\separator",
        &[0xff],
    ] {
        assert!(RelativePath::parse(rejected).is_err());
    }
}

#[test]
fn nul_delimited_git_names_preserve_whitespace_and_unicode() {
    // Catches splitting Git output on lines or ASCII whitespace instead of
    // its required NUL record terminators.
    let repo = GitRepo::init();
    let paths = [
        "fixtures/a b.txt",
        "fixtures/tab\tname.txt",
        "fixtures/line\nbreak.txt",
        "fixtures/привет.txt",
    ];
    for path in paths {
        repo.write(path, path.as_bytes());
    }
    repo.commit_all("track unusual names");

    let selection = select(&repo, &settings(&[], &[], &[])).unwrap();
    let selected = selection
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        selected,
        [
            "fixtures/a b.txt",
            "fixtures/line\nbreak.txt",
            "fixtures/tab\tname.txt",
            "fixtures/привет.txt",
        ]
    );
}

#[test]
fn malformed_nul_streams_and_non_utf8_names_have_stable_failures() {
    // Catches silently accepting a truncated final Git record or replacing a
    // non-UTF-8 filename with the Unicode replacement character.
    for (stdout, expected_code) in [
        (
            b"100644 0123456789012345678901234567890123456789 0\tmissing-final-nul".to_vec(),
            "INVALID_GIT_OUTPUT",
        ),
        (
            [
                b"100644 0123456789012345678901234567890123456789 0\t".as_slice(),
                b"bad\xffname\0",
            ]
            .concat(),
            "UNSUPPORTED_PATH_ENCODING",
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let context = synthetic_context(directory.path());
        let runner = FixedOutputRunner { stdout };

        let error = InputSelector::new(&runner)
            .select(&context, &settings(&[], &[], &[]))
            .unwrap_err();

        assert_eq!(error.code, expected_code);
        assert!(!error.message.contains('\u{fffd}'));
    }
}

#[test]
fn attribute_output_requires_one_ordered_matching_triplet_per_selected_path() {
    // Catches empty, shortened, duplicate, unexpected, reordered, or excess
    // check-attr output bypassing filter enforcement for selected inputs.
    let directory = tempfile::tempdir().unwrap();
    fs::write(directory.path().join("a.txt"), b"a\n").unwrap();
    fs::write(directory.path().join("b.txt"), b"b\n").unwrap();
    let a = attribute_triplet("a.txt");
    let b = attribute_triplet("b.txt");
    let other = attribute_triplet("other.txt");
    let cases = [
        ("empty", Vec::new()),
        ("shortened", a.clone()),
        ("duplicate", [a.as_slice(), a.as_slice()].concat()),
        ("unexpected", [a.as_slice(), other.as_slice()].concat()),
        ("reordered", [b.as_slice(), a.as_slice()].concat()),
        (
            "excess",
            [a.as_slice(), b.as_slice(), b.as_slice()].concat(),
        ),
    ];

    let observed = cases
        .into_iter()
        .map(|(label, attributes)| {
            let result = InputSelector::new(&AttributeOutputRunner { attributes }).select(
                &synthetic_context(directory.path()),
                &settings(&[], &[], &[]),
            );
            (label, result.err().map(|error| error.code))
        })
        .collect::<Vec<_>>();

    assert_eq!(
        observed,
        vec![
            ("empty", Some("INVALID_GIT_OUTPUT")),
            ("shortened", Some("INVALID_GIT_OUTPUT")),
            ("duplicate", Some("INVALID_GIT_OUTPUT")),
            ("unexpected", Some("INVALID_GIT_OUTPUT")),
            ("reordered", Some("INVALID_GIT_OUTPUT")),
            ("excess", Some("INVALID_GIT_OUTPUT")),
        ]
    );
}

#[test]
fn tracked_worktree_entries_and_deletions_are_selected_deterministically() {
    // Catches reading committed blobs instead of selecting the current staged
    // and unstaged filesystem entries, and catches nondeterministic manifests.
    let repo = GitRepo::init();
    for path in ["z-staged.txt", "a-unstaged.txt", "deleted.txt"] {
        repo.write(path, b"committed\n");
    }
    repo.commit_all("initial tracked files");
    repo.write("z-staged.txt", b"current staged bytes\n");
    assert!(repo.git(&["add", "z-staged.txt"]).status.success());
    repo.write("a-unstaged.txt", b"current unstaged bytes\n");
    fs::remove_file(repo.root().join("deleted.txt")).unwrap();

    let selection = select(&repo, &settings(&[], &[], &[])).unwrap();

    assert_eq!(
        selection
            .entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry.origin, entry.kind))
            .collect::<Vec<_>>(),
        vec![
            (
                "a-unstaged.txt",
                InputOrigin::Tracked,
                SelectedInputKind::FilesystemEntry,
            ),
            (
                "z-staged.txt",
                InputOrigin::Tracked,
                SelectedInputKind::FilesystemEntry,
            ),
        ]
    );
    assert_eq!(
        selection
            .tracked_deletions
            .iter()
            .map(RelativePath::as_str)
            .collect::<Vec<_>>(),
        vec!["deleted.txt"]
    );
    assert_eq!(
        fs::read(repo.root().join("z-staged.txt")).unwrap(),
        b"current staged bytes\n"
    );
    assert_eq!(
        fs::read(repo.root().join("a-unstaged.txt")).unwrap(),
        b"current unstaged bytes\n"
    );
}

#[test]
fn uncovered_untracked_inputs_are_one_bounded_exact_diagnostic() {
    // Catches silently omitting local inputs and unboundedly reporting a large
    // worktree in one error.
    let repo = GitRepo::init();
    for index in (0..105).rev() {
        repo.write(&format!("untracked/{index:03}.txt"), b"local\n");
    }
    repo.write("untracked/000\nbreak.txt", b"newline\n");

    let error = select(&repo, &settings(&[], &[], &[])).unwrap_err();

    assert_eq!(error.code, "UNTRACKED_INPUT");
    assert_eq!(error.total_path_count, 106);
    assert_eq!(error.paths.len(), 100);
    assert_eq!(error.paths[0].as_str(), "untracked/000\nbreak.txt");
    assert_eq!(error.paths[1].as_str(), "untracked/000.txt");
    assert!(error.message.contains("untracked/000\\nbreak.txt"));
    assert!(!error.message.contains("000\nbreak.txt"));
}

#[test]
fn core_excludes_file_is_honoured_when_global_git_config_is_neutralized() {
    // A file ignored only through the user's core.excludesFile must stay out
    // of the captured selection and must not raise UNTRACKED_INPUT, even
    // though selection commands neutralize GIT_CONFIG_GLOBAL.
    let _home = IsolatedHome::lock();
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("initial");
    repo.write("secret-only.dat", b"ignored only by core.excludesFile\n");
    repo.write("visible.txt", b"untracked and included\n");

    let home_dir = tempfile::tempdir().unwrap();
    let home = home_dir.path();
    let excludes = home.join("excludes");
    fs::write(&excludes, "secret-only.dat\n").unwrap();
    fs::write(
        home.join(".gitconfig"),
        format!("[core]\n\texcludesFile = {}\n", excludes.display()),
    )
    .unwrap();
    IsolatedHome::set(home);

    let selection = select(&repo, &settings(&["visible.txt"], &[], &[]))
        .expect("a file ignored only through core.excludesFile must not raise UNTRACKED_INPUT");
    assert!(
        selection
            .entries
            .iter()
            .all(|entry| entry.path.as_str() != "secret-only.dat"),
        "core.excludesFile must keep secret-only.dat out of the selection"
    );
    assert!(
        selection
            .entries
            .iter()
            .any(|entry| entry.path.as_str() == "visible.txt")
    );

    let runner = RecordingRunner::default();
    InputSelector::new(&runner)
        .select(&context(&repo), &settings(&["visible.txt"], &[], &[]))
        .unwrap();
    let requests = runner.requests.lock().unwrap();
    let resolver = requests
        .iter()
        .find(|request| {
            request.args.iter().any(|arg| arg == "config")
                && request.args.iter().any(|arg| arg == "core.excludesFile")
        })
        .expect("selection must resolve core.excludesFile under the user's configuration");
    assert!(
        !resolver
            .environment
            .iter()
            .any(|(key, value)| key == "GIT_CONFIG_GLOBAL" && value == "/dev/null"),
        "the resolver must not neutralize the user's global Git configuration"
    );
    let expected_override = format!("core.excludesFile={}", excludes.display());
    let selection_commands = requests.iter().filter(|request| {
        !request
            .args
            .iter()
            .any(|arg| arg == "core.excludesFile" && request.args.iter().any(|arg| arg == "config"))
    });
    for request in selection_commands {
        assert!(
            request
                .environment
                .iter()
                .any(|(key, value)| { key == "GIT_CONFIG_GLOBAL" && value == "/dev/null" }),
            "selection commands must keep global configuration neutralized"
        );
        assert!(
            request.args.iter().any(|arg| arg == "-c")
                && request
                    .args
                    .iter()
                    .any(|arg| *arg == OsString::from(&expected_override)
                        || arg
                            .to_string_lossy()
                            .contains(excludes.to_string_lossy().as_ref())),
            "selection commands must receive the validated core.excludesFile: {:?}",
            request.args
        );
    }
}

#[test]
fn explicit_patterns_admit_non_ignored_and_ignored_inputs_with_their_origins() {
    // Catches treating ignored and ordinary untracked inputs identically or
    // broadening ignored inclusion beyond the explicit Git pathspec.
    let repo = GitRepo::init();
    repo.write(".gitignore", b"fixtures/*.log\n");
    repo.commit_all("ignore fixture logs");
    repo.write("src/local.rs", b"local source\n");
    repo.write("fixtures/allowed.log", b"ignored fixture\n");
    repo.write("fixtures/absent.tmp", b"not included\n");

    let error = select(&repo, &settings(&["src/*.rs", "fixtures/*.log"], &[], &[])).unwrap_err();
    assert_eq!(error.code, "UNTRACKED_INPUT");
    assert_eq!(error.paths[0].as_str(), "fixtures/absent.tmp");

    fs::remove_file(repo.root().join("fixtures/absent.tmp")).unwrap();
    let selection = select(&repo, &settings(&["src/*.rs", "fixtures/*.log"], &[], &[])).unwrap();

    let local = selection
        .entries
        .iter()
        .find(|entry| entry.path.as_str() == "src/local.rs")
        .unwrap();
    assert_eq!(local.origin, InputOrigin::IncludedUntracked);
    let ignored = selection
        .entries
        .iter()
        .find(|entry| entry.path.as_str() == "fixtures/allowed.log")
        .unwrap();
    assert_eq!(ignored.origin, InputOrigin::IncludedIgnored);
}

#[test]
fn nested_attributes_block_lfs_on_an_included_untracked_file() {
    // Catches checking attributes before explicit untracked inputs have joined
    // the selected filesystem-entry set.
    let repo = GitRepo::init();
    repo.write("nested/.gitattributes", b"*.bin filter=lfs\n");
    repo.commit_all("track nested attributes");
    repo.write("nested/local.bin", b"untracked lfs materialization\n");

    let error = select(&repo, &settings(&["nested/*.bin"], &[], &[])).unwrap_err();

    assert_eq!(error.code, "UNSUPPORTED_LFS");
    assert_eq!(error.paths[0].as_str(), "nested/local.bin");
}

#[test]
fn nested_attributes_block_custom_filters_on_an_included_ignored_file() {
    // Catches checking attributes only for tracked entries while an ignored
    // input with a nested custom filter is explicitly admitted.
    let repo = GitRepo::init();
    repo.write(".gitignore", b"nested/*.dat\n");
    repo.write("nested/.gitattributes", b"*.dat filter=custom\n");
    repo.commit_all("track nested ignore and attributes");
    repo.write("nested/local.dat", b"ignored filtered materialization\n");

    let error = select(&repo, &settings(&["nested/*.dat"], &[], &[])).unwrap_err();

    assert_eq!(error.code, "UNSUPPORTED_FILTER");
    assert_eq!(error.paths[0].as_str(), "nested/local.dat");
}

#[test]
fn only_existing_physical_empty_directories_are_admitted() {
    // Catches declared empty directories escaping through a symlink or being
    // accepted after they gained contents.
    let repo = GitRepo::init();
    create_directory(repo.root().join("fixtures/empty"));

    let selection = select(&repo, &settings(&[], &["fixtures/empty"], &[])).unwrap();
    let empty = selection
        .entries
        .iter()
        .find(|entry| entry.path.as_str() == "fixtures/empty")
        .unwrap();
    assert_eq!(empty.kind, SelectedInputKind::EmptyDirectory);

    for declaration in ["fixtures/missing", "fixtures/link", "fixtures/nonempty"] {
        let repo = GitRepo::init();
        match declaration {
            "fixtures/link" => {
                create_directory(repo.root().join("fixtures/target"));
                symlink("target", repo.root().join("fixtures/link")).unwrap();
            }
            "fixtures/nonempty" => repo.write("fixtures/nonempty/file.txt", b"not empty\n"),
            _ => {}
        }

        let error = select(&repo, &settings(&[], &[declaration], &[])).unwrap_err();
        assert_eq!(error.code, "INVALID_EMPTY_DIRECTORY", "{declaration}");
        assert_eq!(error.paths[0].as_str(), declaration);
    }
}

#[test]
fn empty_directory_declarations_reject_an_in_worktree_parent_symlink() {
    // Catches validating only the final empty-directory component while an
    // earlier component redirects traversal through a symlink.
    let repo = GitRepo::init();
    create_directory(repo.root().join("fixtures/actual/empty"));
    symlink("actual", repo.root().join("fixtures/link")).unwrap();

    let error = select(
        &repo,
        &settings(&["fixtures/link"], &["fixtures/link/empty"], &[]),
    )
    .unwrap_err();

    assert_eq!(error.code, "INVALID_EMPTY_DIRECTORY");
    assert_eq!(error.paths[0].as_str(), "fixtures/link/empty");
}

#[test]
fn cache_dependency_and_metadata_paths_cannot_be_explicitly_included() {
    // Catches project settings pulling repository metadata, dependency caches,
    // build products, or editor state into a snapshot.
    for path in [
        ".git/config",
        "node_modules/pkg/index.js",
        ".pnpm-store/v3/index",
        ".yarn/cache/pkg.zip",
        "vendor/bundle/ruby/gem.rb",
        ".venv/bin/python",
        "pkg/__pycache__/module.pyc",
        ".cache/tool/state",
        "target/debug/app",
        "dist/app.js",
        ".idea/workspace.xml",
        ".vscode/settings.json",
    ] {
        let repo = GitRepo::init();
        let error = select(&repo, &settings(&[path], &[], &[])).unwrap_err();
        assert_eq!(error.code, "FORBIDDEN_PATH", "{path}");
    }
}

#[test]
fn submodules_lfs_and_configured_filters_block_selection() {
    // Catches snapshots whose bytes depend on unsupported Git object or filter
    // behavior instead of ordinary filesystem entries.
    let repo = GitRepo::init();
    repo.write("README.md", b"root\n");
    repo.commit_all("initial");
    let head = String::from_utf8(repo.git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    assert!(
        repo.git(&[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{head},vendor/submodule"),
        ])
        .status
        .success()
    );
    assert_eq!(
        select(&repo, &settings(&[], &[], &[])).unwrap_err().code,
        "UNSUPPORTED_SUBMODULE"
    );

    let repo = GitRepo::init();
    repo.write(".gitattributes", b"*.bin filter=lfs\n");
    repo.write("asset.bin", b"asset bytes\n");
    repo.commit_all("track attributed asset");
    assert_eq!(
        select(&repo, &settings(&[], &[], &[])).unwrap_err().code,
        "UNSUPPORTED_LFS"
    );

    let repo = GitRepo::init();
    assert!(
        repo.git(&["config", "filter.custom.clean", "cat"])
            .status
            .success()
    );
    assert_eq!(
        select(&repo, &settings(&[], &[], &[])).unwrap_err().code,
        "UNSUPPORTED_FILTER"
    );
}

#[test]
fn sensitive_paths_require_an_exact_allowlist_and_emit_warnings() {
    // Catches accidental credential capture and catches broad prefix-based
    // allowlisting of a different sensitive file.
    let repo = GitRepo::init();
    for path in [".env", ".env.example", ".env.sample"] {
        repo.write(path, b"fixture\n");
    }
    repo.commit_all("track environment fixtures");

    let error = select(&repo, &settings(&[], &[], &[])).unwrap_err();
    assert_eq!(error.code, "SENSITIVE_PATH");
    assert_eq!(error.paths.len(), 1);
    assert_eq!(error.paths[0].as_str(), ".env");

    let still_blocked = select(&repo, &settings(&[], &[], &["other/.env"])).unwrap_err();
    assert_eq!(still_blocked.code, "SENSITIVE_PATH");

    let selection = select(&repo, &settings(&[], &[], &[".env"])).unwrap();
    assert_eq!(selection.warnings.len(), 1);
    assert_eq!(selection.warnings[0].code, "SENSITIVE_PATH_ALLOWED");
    assert_eq!(selection.warnings[0].path.as_str(), ".env");
}

#[test]
fn every_sensitive_basename_and_directory_policy_is_enforced() {
    // Catches regressions that protect only .env while allowing other common
    // credentials and cloud configuration directories.
    for path in [
        "nested/.env.production",
        "nested/.npmrc",
        "nested/.pypirc",
        "nested/.netrc",
        "nested/id_rsa",
        "nested/id_ed25519",
        "nested/credentials",
        "nested/credentials.json",
        "home/.ssh/config",
        "home/.aws/credentials",
        "home/.config/gcloud/application_default_credentials.json",
        "home/.kube/config",
    ] {
        let repo = GitRepo::init();
        repo.write(path, b"secret\n");
        repo.commit_all("track sensitive fixture");

        let error = select(&repo, &settings(&[], &[], &[])).unwrap_err();
        assert_eq!(error.code, "SENSITIVE_PATH", "{path}");
        assert_eq!(error.paths[0].as_str(), path);
    }
}

#[test]
fn input_count_and_git_request_policies_are_bounded_and_isolated() {
    // Catches losing the entry-count ceiling even when output remains under
    // the byte ceiling.
    let mut stdout = Vec::new();
    for index in 0..=250_000 {
        stdout.extend_from_slice(
            format!("100644 0123456789012345678901234567890123456789 0\tfiles/{index:06}.txt\0")
                .as_bytes(),
        );
    }
    let directory = tempfile::tempdir().unwrap();
    let error = InputSelector::new(&FixedOutputRunner { stdout })
        .select(
            &synthetic_context(directory.path()),
            &settings(&[], &[], &[]),
        )
        .unwrap_err();
    assert_eq!(error.code, "INPUT_SET_TOO_LARGE");

    // The recording wrapper delegates to real Git, so assertions cover the
    // actual requests used by the successful selector path.
    let repo = GitRepo::init();
    repo.write("README.md", b"tracked\n");
    repo.commit_all("initial");
    let runner = RecordingRunner::default();
    InputSelector::new(&runner)
        .select(&context(&repo), &settings(&["fixtures/*.log"], &[], &[]))
        .unwrap();
    let requests = runner.requests.lock().unwrap();
    assert!(!requests.is_empty());
    for request in requests.iter() {
        assert_eq!(request.program, OsString::from("/usr/bin/git"));
        assert_eq!(request.policy.stdout_limit, 64 * 1024 * 1024);
        assert_eq!(request.policy.stderr_limit, 64 * 1024 * 1024);
        assert_eq!(request.policy.deadline, Duration::from_secs(5));
        let resolves_excludes = request.args.iter().any(|arg| arg == "core.excludesFile")
            && request.args.iter().any(|arg| arg == "--get");
        if resolves_excludes {
            assert!(
                !request
                    .environment
                    .iter()
                    .any(|(key, value)| { key == "GIT_CONFIG_GLOBAL" && value == "/dev/null" }),
                "the core.excludesFile resolver must use the user's configuration"
            );
            continue;
        }
        assert_eq!(
            request.environment,
            vec![
                ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
                ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
                ("GIT_ATTR_NOSYSTEM".into(), "1".into()),
            ]
        );
        for name in [
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_COMMON_DIR",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_CEILING_DIRECTORIES",
            "GIT_DISCOVERY_ACROSS_FILESYSTEM",
            "GIT_CONFIG_COUNT",
            "GIT_CONFIG_PARAMETERS",
            "GIT_ATTR_SOURCE",
            "GIT_CONFIG",
            "GIT_LITERAL_PATHSPECS",
            "GIT_GLOB_PATHSPECS",
            "GIT_NOGLOB_PATHSPECS",
            "GIT_ICASE_PATHSPECS",
        ] {
            assert!(request.environment_remove.contains(&name.into()));
        }
        for preserved in ["HOME", "XDG_CONFIG_HOME", "XDG_CONFIG_DIRS"] {
            assert!(!request.environment_remove.contains(&preserved.into()));
        }
    }
    let ignored = requests
        .iter()
        .find(|request| request.args.iter().any(|arg| arg == "--ignored"))
        .expect("include patterns issue one ignored query");
    assert!(ignored.args.ends_with(&[
        OsString::from("--"),
        OsString::from(":(glob)fixtures/*.log"),
    ]));
}

fn synthetic_context(root: &Path) -> ProjectContext {
    ProjectContext {
        root: root.to_path_buf(),
        relative_cwd: PathBuf::new(),
        git_dir: root.join(".git"),
        common_dir: root.join(".git"),
        head: None,
        branch: Some("main".into()),
        project_id: "project".into(),
        worktree_id: "worktree".into(),
        dirty: false,
    }
}

struct FixedOutputRunner {
    stdout: Vec<u8>,
}

impl ProcessRunner for FixedOutputRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        if request.args.iter().any(|arg| arg == "core.excludesFile")
            && request.args.iter().any(|arg| arg == "--get")
        {
            return Ok(ProcessResult {
                status: ExitStatus::from_raw(1 << 8),
                stdout: Vec::new(),
                stderr: Vec::new(),
            });
        }
        Ok(ProcessResult {
            status: ExitStatus::from_raw(0),
            stdout: self.stdout.clone(),
            stderr: Vec::new(),
        })
    }
}

fn attribute_triplet(path: &str) -> Vec<u8> {
    [path.as_bytes(), b"\0filter\0unspecified\0"].concat()
}

struct AttributeOutputRunner {
    attributes: Vec<u8>,
}

impl ProcessRunner for AttributeOutputRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        let (status, stdout) = if request.args.iter().any(|arg| arg == "--stage") {
            (
                ExitStatus::from_raw(0),
                [
                    b"100644 0123456789012345678901234567890123456789 0\ta.txt\0".as_slice(),
                    b"100644 0123456789012345678901234567890123456789 0\tb.txt\0",
                ]
                .concat(),
            )
        } else if request.args.iter().any(|arg| arg == "--get-regexp")
            || (request.args.iter().any(|arg| arg == "--get")
                && request.args.iter().any(|arg| arg == "core.excludesFile"))
        {
            (ExitStatus::from_raw(1 << 8), Vec::new())
        } else if request.args.iter().any(|arg| arg == "check-attr") {
            (ExitStatus::from_raw(0), self.attributes.clone())
        } else {
            (ExitStatus::from_raw(0), Vec::new())
        };
        Ok(ProcessResult {
            status,
            stdout,
            stderr: Vec::new(),
        })
    }
}

static HOME_LOCK: Mutex<()> = Mutex::new(());

struct IsolatedHome {
    _guard: std::sync::MutexGuard<'static, ()>,
    previous_home: Option<OsString>,
    previous_xdg: Option<OsString>,
}

impl IsolatedHome {
    fn lock() -> Self {
        let previous_home = std::env::var_os("HOME");
        let previous_xdg = std::env::var_os("XDG_CONFIG_HOME");
        Self {
            _guard: HOME_LOCK.lock().expect("home lock"),
            previous_home,
            previous_xdg,
        }
    }

    fn set(home: &Path) {
        // Isolation is scoped to the HOME_LOCK held by the current test.
        unsafe {
            std::env::set_var("HOME", home);
            std::env::remove_var("XDG_CONFIG_HOME");
        }
    }
}

impl Drop for IsolatedHome {
    fn drop(&mut self) {
        unsafe {
            match &self.previous_home {
                Some(value) => std::env::set_var("HOME", value),
                None => std::env::remove_var("HOME"),
            }
            match &self.previous_xdg {
                Some(value) => std::env::set_var("XDG_CONFIG_HOME", value),
                None => std::env::remove_var("XDG_CONFIG_HOME"),
            }
        }
    }
}

#[derive(Default)]
struct RecordingRunner {
    requests: Mutex<Vec<ProcessRequest>>,
}

impl ProcessRunner for RecordingRunner {
    fn run(&self, request: &ProcessRequest) -> Result<ProcessResult, WorkerError> {
        self.requests.lock().unwrap().push(request.clone());
        SystemProcessRunner.run(request)
    }
}
