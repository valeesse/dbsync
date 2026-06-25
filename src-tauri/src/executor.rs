use crate::{
  connectors,
  diff,
  error::{AppError, AppResult},
  metadata_store::MetadataStore,
  models::{DataSourceRecord, NewSyncRunRecord, SyncJobSummaryDto, SyncRunSummary, TableCatalog, TableRef},
};
use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

const BATCH_SIZE: i64 = 500;

pub async fn run_full_sync(
  metadata: &MetadataStore,
  job: &SyncJobSummaryDto,
  source: &DataSourceRecord,
  source_password: &str,
  target: &DataSourceRecord,
  target_password: &str,
) -> AppResult<SyncRunSummary> {
  let run_id = Uuid::now_v7().to_string();
  metadata
    .create_sync_run(NewSyncRunRecord {
      id: run_id.clone(),
      job_id: job.id.clone(),
      job_name: job.name.clone(),
      status: "running".into(),
      tables_total: job.selected_tables.len() as i64,
      tables_completed: 0,
      rows_copied: 0,
      started_at: Utc::now(),
      finished_at: None,
      detail: Some("Preparing diff plan".into()),
    })
    .await?;

  let diff_preview = diff::preview(
    source,
    source_password,
    target,
    target_password,
    &job.selected_tables,
  )
  .await?;

  let mut total_rows = 0i64;
  let mut tables_completed = 0i64;

  for item in &diff_preview.items {
    for statement in &item.statements {
      connectors::create_table(target, target_password, statement).await?;
    }
  }

  for selected in &job.selected_tables {
    let table_ref = TableRef::parse(selected)?;
    let source_table = connectors::get_table(source, source_password, &table_ref)
      .await?
      .ok_or_else(|| AppError::Message(format!("source table {} not found", table_ref.key())))?;
    let target_table = connectors::get_table(target, target_password, &table_ref)
      .await?
      .ok_or_else(|| AppError::Message(format!("target table {} not found after diff apply", table_ref.key())))?;

    let target_columns = target_table
      .columns
      .iter()
      .filter(|target_column| {
        source_table
          .columns
          .iter()
          .any(|source_column| {
            source_column.name == target_column.name && source_column.supports_value_transfer()
          })
      })
      .cloned()
      .collect::<Vec<_>>();

    if target_columns.is_empty() {
      return Err(AppError::Message(format!(
        "table {} has no transferable columns after diff planning",
        table_ref.key()
      )));
    }

    connectors::truncate_table(target, target_password, &table_ref).await?;

    let mut offset = 0i64;
    loop {
      let batch = connectors::fetch_table_rows_json(
        source,
        source_password,
        &TableCatalog {
          columns: source_table.columns.clone(),
          ..source_table.clone()
        },
        offset,
        BATCH_SIZE,
      )
      .await?;

      if batch.is_empty() {
        break;
      }

      let inserted =
        connectors::insert_rows(target, target_password, &table_ref, &target_columns, &batch).await?;
      total_rows += inserted as i64;
      offset += batch.len() as i64;

      metadata
        .update_sync_run_progress(
          &run_id,
          "running",
          tables_completed,
          total_rows,
          Some(&format!("Syncing {} ({} rows copied)", table_ref.key(), total_rows)),
        )
        .await?;
    }

    tables_completed += 1;
    metadata
      .update_sync_run_progress(
        &run_id,
        "running",
        tables_completed,
        total_rows,
        Some(&format!("Completed {}", table_ref.key())),
      )
      .await?;
  }

  metadata
    .upsert_checkpoint(
      &job.id,
      "full_snapshot",
      &json!({
        "mode": job.mode,
        "tables": job.selected_tables,
        "completed_at": Utc::now(),
        "rows_copied": total_rows,
      })
      .to_string(),
    )
    .await?;

  metadata
    .finish_sync_run(
      &run_id,
      "succeeded",
      tables_completed,
      total_rows,
      Some("Full sync finished successfully"),
    )
    .await?;

  metadata
    .list_sync_runs()
    .await?
    .into_iter()
    .find(|run| run.id == run_id)
    .ok_or_else(|| AppError::Message("finished sync run could not be reloaded".into()))
}
