use crate::{
  connectors,
  error::AppResult,
  metadata_store::MetadataStore,
  models::{CdcCheckpoint, DataSourceRecord, IncrementalAssessment, SyncJobSummaryDto},
};

pub async fn assess(
  metadata: &MetadataStore,
  job: &SyncJobSummaryDto,
  source: &DataSourceRecord,
  source_password: &str,
  target: &DataSourceRecord,
) -> AppResult<IncrementalAssessment> {
  let source_capability = connectors::inspect_cdc(source, source_password).await?;
  let checkpoint = metadata.get_checkpoint(&job.id).await?;

  Ok(IncrementalAssessment {
    job_name: job.name.clone(),
    source_name: source.name.clone(),
    target_name: target.name.clone(),
    source_capability,
    checkpoint: checkpoint.clone(),
    event_model: vec![
      "insert/update/delete".into(),
      "table_key".into(),
      "primary_key".into(),
      "before".into(),
      "after".into(),
      "source_offset".into(),
      "event_ts".into(),
    ],
    next_actions: build_next_actions(source, target, checkpoint.as_ref()),
  })
}

fn build_next_actions(
  source: &DataSourceRecord,
  target: &DataSourceRecord,
  checkpoint: Option<&CdcCheckpoint>,
) -> Vec<String> {
  let mut actions = vec![
    format!(
      "Prepare unified CDC reader from {} to {}",
      source.kind.as_str(),
      target.kind.as_str()
    ),
    "Persist source offsets into local SQLite checkpoints".into(),
    "Apply event pipeline through the same target type-mapping layer as full sync".into(),
  ];

  if checkpoint.is_none() {
    actions.push("No checkpoint found yet: first incremental run must bootstrap upstream offset".into());
  } else {
    actions.push("Checkpoint exists: next run can resume from saved offset payload".into());
  }

  actions
}
