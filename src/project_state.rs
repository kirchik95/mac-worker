use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{
    dag::DagFrozenSpec,
    error::WorkerError,
    inputs::{InputSelector, SelectionFailure, SelectionWarning},
    process::ProcessRunner,
    project::{ProjectContext, ProjectInspector},
    project_config::{
        ArtifactSettings, ProjectSettings, ResourceClass, SnapshotSettings, TaskSettings,
    },
    requirements::RequirementDetector,
    snapshot::{Snapshot, SnapshotBuilder},
    task::TaskMeta,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectState {
    pub context: ProjectContext,
    pub origin: Option<String>,
    pub settings: ProjectSettings,
    pub requirements: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectPreparationRequest {
    pub project: PathBuf,
    pub cli_includes: Vec<String>,
}

#[derive(Debug)]
pub struct PreparedProject {
    pub state: ProjectState,
    pub selection_warnings: Vec<SelectionWarning>,
    pub snapshot: Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectPreparationStage {
    StableReload,
    SnapshotCaptured,
    SnapshotCleanup,
}

#[derive(Debug, thiserror::Error)]
pub enum ProjectPreparationError {
    #[error(transparent)]
    Selection(SelectionFailure),
    #[error("{error}")]
    Worker {
        error: WorkerError,
        selection_warnings: Vec<SelectionWarning>,
    },
}

impl ProjectPreparationError {
    fn worker(error: WorkerError) -> Self {
        Self::Worker {
            error,
            selection_warnings: Vec::new(),
        }
    }
}

impl ProjectState {
    pub fn load(
        runner: &dyn ProcessRunner,
        project: &Path,
        cli_includes: &[String],
    ) -> Result<Self, WorkerError> {
        let (context, origin) = ProjectInspector::new(runner).inspect_with_origin(project)?;
        let settings = ProjectSettings::load(&context.root, cli_includes)?;
        let detected = RequirementDetector::detect(&context.root)?;
        let requirements = merge_project_requirements(&settings.requires, detected);
        Ok(Self {
            context,
            origin,
            settings,
            requirements,
        })
    }

    /// Loads current local project state for a durable task without reading
    /// the mutable Git origin. The task's submit-time project identity is the
    /// authoritative identity after submission.
    pub fn load_for_task(
        runner: &dyn ProcessRunner,
        project: &Path,
        cli_includes: &[String],
        meta: &TaskMeta,
    ) -> Result<Self, WorkerError> {
        let context = ProjectInspector::new(runner)
            .inspect_with_pinned_project_id(project, meta.project_id())?;
        let settings = ProjectSettings::load(&context.root, cli_includes)?;
        let detected = RequirementDetector::detect(&context.root)?;
        let requirements = merge_project_requirements(&settings.requires, detected);
        Ok(Self {
            context,
            origin: None,
            settings,
            requirements,
        })
    }

    /// Reconstructs submit-time project settings from a DAG freeze. `setup`
    /// stays `None` so `prepare_turn` still reads setup from the selected task
    /// workspace. Callers must not reread `.worker.toml` for DAG tasks.
    pub(crate) fn settings_from_frozen(spec: &DagFrozenSpec) -> ProjectSettings {
        let mut permissions = BTreeMap::new();
        permissions.insert(spec.agent.clone(), spec.permissions.clone());
        ProjectSettings {
            requires: spec.requires.clone(),
            resource_class: ResourceClass::Heavy,
            timeout: Duration::from_millis(spec.timeout_millis),
            snapshot: SnapshotSettings {
                include_untracked: spec.include_untracked.clone(),
                include_empty_dirs: spec.include_empty_dirs.clone(),
                allow_sensitive: spec.allow_sensitive.clone(),
            },
            artifacts: ArtifactSettings {
                include: Vec::new(),
                max_total_bytes: None,
            },
            task: TaskSettings {
                source: spec.source.clone(),
                publish: spec.publish.clone(),
                env_profile: spec.env_profile.clone(),
                model: spec.model.clone(),
                effort: spec.effort.clone(),
                default_agent: spec.agent.clone(),
                timeout: Duration::from_millis(spec.timeout_millis),
                max_followups: spec.max_followups,
                permissions,
            },
            setup: None,
        }
    }

    /// Inspects the frozen project path with the pinned project id. Worktree
    /// identity must still match the freeze; local `.worker.toml` is ignored.
    pub(crate) fn load_for_frozen_dag_task(
        runner: &dyn ProcessRunner,
        spec: &DagFrozenSpec,
    ) -> Result<Self, WorkerError> {
        let context = ProjectInspector::new(runner)
            .inspect_with_pinned_project_id(Path::new(&spec.project_path), &spec.project_id)?;
        if context.worktree_id != spec.worktree_id {
            return Err(WorkerError::task(
                "PROJECT_MISMATCH",
                "current project is not the task's project",
            ));
        }
        Ok(Self {
            context,
            origin: None,
            settings: Self::settings_from_frozen(spec),
            requirements: spec.requires.clone(),
        })
    }

    /// DAG tasks use the freeze; independent runs and tasks with no run load
    /// current project settings. `ordinary` runs only when
    /// `ClientStateStore::frozen_spec_for_record` returned `None`.
    pub(crate) fn load_validated_for_task(
        runner: &dyn ProcessRunner,
        frozen: Option<&DagFrozenSpec>,
        ordinary: impl FnOnce() -> Result<Self, WorkerError>,
    ) -> Result<Self, WorkerError> {
        match frozen {
            Some(spec) => Self::load_for_frozen_dag_task(runner, spec),
            None => ordinary(),
        }
    }

    pub fn prepare(
        runner: &dyn ProcessRunner,
        cache: &Path,
        request: ProjectPreparationRequest,
        probed: &Self,
    ) -> Result<PreparedProject, ProjectPreparationError> {
        Self::prepare_observed(runner, cache, request, probed, &|_| Ok(()))
    }

    pub(crate) fn prepare_observed(
        runner: &dyn ProcessRunner,
        cache: &Path,
        request: ProjectPreparationRequest,
        probed: &Self,
        observer: &dyn Fn(ProjectPreparationStage) -> Result<(), WorkerError>,
    ) -> Result<PreparedProject, ProjectPreparationError> {
        let before_capture = Self::load(runner, &request.project, &request.cli_includes)
            .map_err(ProjectPreparationError::worker)?;
        if &before_capture != probed {
            return Err(ProjectPreparationError::worker(state_changed_error()));
        }
        observer(ProjectPreparationStage::StableReload).map_err(ProjectPreparationError::worker)?;

        let selection = InputSelector::new(runner)
            .select(&before_capture.context, &before_capture.settings.snapshot)
            .map_err(ProjectPreparationError::Selection)?;
        let selection_warnings = selection.warnings.clone();
        let snapshot = SnapshotBuilder::new(runner, cache)
            .capture(
                &before_capture.context,
                &before_capture.settings.snapshot,
                selection,
            )
            .map_err(|error| ProjectPreparationError::Worker {
                error,
                selection_warnings: selection_warnings.clone(),
            })?;
        if let Err(error) = observer(ProjectPreparationStage::SnapshotCaptured) {
            return match cleanup_snapshot_observed(snapshot, observer) {
                Ok(()) => Err(ProjectPreparationError::Worker {
                    error,
                    selection_warnings,
                }),
                Err(cleanup_error) => Err(ProjectPreparationError::Worker {
                    error: cleanup_error,
                    selection_warnings,
                }),
            };
        }

        let after_capture = match Self::load(runner, &request.project, &request.cli_includes) {
            Ok(state) => state,
            Err(error) => {
                return match cleanup_snapshot_observed(snapshot, observer) {
                    Ok(()) => Err(ProjectPreparationError::Worker {
                        error,
                        selection_warnings,
                    }),
                    Err(cleanup_error) => Err(ProjectPreparationError::Worker {
                        error: cleanup_error,
                        selection_warnings,
                    }),
                };
            }
        };
        if after_capture != before_capture {
            return match cleanup_snapshot_observed(snapshot, observer) {
                Ok(()) => Err(ProjectPreparationError::Worker {
                    error: state_changed_error(),
                    selection_warnings,
                }),
                Err(cleanup_error) => Err(ProjectPreparationError::Worker {
                    error: cleanup_error,
                    selection_warnings,
                }),
            };
        }

        Ok(PreparedProject {
            state: before_capture,
            selection_warnings,
            snapshot,
        })
    }
}

impl PreparedProject {
    pub fn cleanup(self) -> Result<(), WorkerError> {
        self.cleanup_observed(&|_| Ok(()))
    }

    pub(crate) fn cleanup_observed(
        self,
        observer: &dyn Fn(ProjectPreparationStage) -> Result<(), WorkerError>,
    ) -> Result<(), WorkerError> {
        cleanup_snapshot_observed(self.snapshot, observer)
    }
}

fn cleanup_snapshot_observed(
    snapshot: Snapshot,
    observer: &dyn Fn(ProjectPreparationStage) -> Result<(), WorkerError>,
) -> Result<(), WorkerError> {
    let cleanup_result = snapshot.cleanup();
    let observer_result = observer(ProjectPreparationStage::SnapshotCleanup);
    match cleanup_result {
        Ok(()) => observer_result,
        Err(cleanup_error) => Err(cleanup_error),
    }
}

pub(crate) fn merge_project_requirements(
    configured: &[String],
    mut detected: Vec<String>,
) -> Vec<String> {
    detected.sort();
    detected.dedup();

    let mut seen = HashSet::new();
    configured
        .iter()
        .chain(&detected)
        .filter(|requirement| seen.insert(requirement.as_str()))
        .cloned()
        .collect()
}

fn state_changed_error() -> WorkerError {
    WorkerError::Snapshot {
        code: "SNAPSHOT_CHANGED",
        message: "project policy, requirements, or Git metadata changed during doctor inspection"
            .into(),
    }
}
