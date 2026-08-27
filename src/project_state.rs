use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use crate::{
    error::WorkerError,
    inputs::{InputSelector, SelectionFailure, SelectionWarning},
    process::ProcessRunner,
    project::{ProjectContext, ProjectInspector},
    project_config::ProjectSettings,
    requirements::RequirementDetector,
    snapshot::{Snapshot, SnapshotBuilder},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectState {
    pub context: ProjectContext,
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
        let context = ProjectInspector::new(runner).inspect(project)?;
        let settings = ProjectSettings::load(&context.root, cli_includes)?;
        let detected = RequirementDetector::detect(&context.root)?;
        let requirements = merge_project_requirements(&settings.requires, detected);
        Ok(Self {
            context,
            settings,
            requirements,
        })
    }

    pub fn prepare(
        runner: &dyn ProcessRunner,
        cache: &Path,
        request: ProjectPreparationRequest,
        probed: &Self,
    ) -> Result<PreparedProject, ProjectPreparationError> {
        let before_capture = Self::load(runner, &request.project, &request.cli_includes)
            .map_err(ProjectPreparationError::worker)?;
        if &before_capture != probed {
            return Err(ProjectPreparationError::worker(state_changed_error()));
        }

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

        let after_capture = match Self::load(runner, &request.project, &request.cli_includes) {
            Ok(state) => state,
            Err(error) => {
                return match snapshot.cleanup() {
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
            return match snapshot.cleanup() {
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
        self.snapshot.cleanup()
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
