use crate::{
  error::{AppError, AppResult},
  models::{CreateSyncJobInput, SyncMode, SyncPlanPreview},
};

pub fn plan_job(input: &CreateSyncJobInput) -> AppResult<SyncPlanPreview> {
  if input.name.trim().is_empty() {
    return Err(AppError::Message("sync job name is required".into()));
  }

  if input.source_id == input.target_id {
    return Err(AppError::Message("source and target must be different".into()));
  }

  if input.selected_tables.is_empty() {
    return Err(AppError::Message(
      "select at least one table before planning a sync job".into(),
    ));
  }

  let incremental = matches!(input.mode, SyncMode::Incremental | SyncMode::FullThenIncremental);
  let mut steps = vec![
    format!("Snapshot metadata for {} table(s)", input.selected_tables.len()),
    "Compare source and target structures to build DDL patch set".into(),
    "Launch chunked bulk copy with resumable checkpoints".into(),
  ];

  if incremental {
    steps.push("Attach CDC reader and persist upstream offsets".into());
    steps.push("Apply upsert/delete stream through target writer pipeline".into());
  }

  Ok(SyncPlanPreview {
    summary: format!(
      "{} will run in {} mode using {}.",
      input.name,
      input.mode.as_str(),
      input.strategy
    ),
    risk_level: if incremental || input.selected_tables.len() > 12 {
      "medium".into()
    } else {
      "low".into()
    },
    estimated_stages: steps.len(),
    steps,
  })
}
