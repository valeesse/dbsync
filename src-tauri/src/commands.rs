use chrono::Utc;
use tauri::State;
use uuid::Uuid;

use crate::{
  cdc, connectors, diff, executor,
  error::{AppError, CommandResult},
  models::{
    CreateDataSourceInput, CreateSyncJobInput, DashboardSnapshot, DataSourceSummary,
    IncrementalAssessment, NewDataSourceRecord, NewSyncJobRecord, ProbeResult, SchemaCatalog,
    StructureDiffPreview, SyncJobSummaryDto, SyncPlanPreview, SyncRunSummary, TableCatalog,
  },
  state::AppState,
  sync,
};

#[tauri::command]
pub async fn dashboard_snapshot(state: State<'_, AppState>) -> CommandResult<DashboardSnapshot> {
  state.metadata.dashboard_snapshot().await.map_err(Into::into)
}

#[tauri::command]
pub async fn list_data_sources(state: State<'_, AppState>) -> CommandResult<Vec<DataSourceSummary>> {
  state.metadata.list_data_sources().await.map_err(Into::into)
}

#[tauri::command]
pub async fn probe_data_source(input: CreateDataSourceInput) -> CommandResult<ProbeResult> {
  connectors::probe(
    &input.kind,
    &input.host,
    input.port,
    &input.database_name,
    &input.username,
    &input.password,
  )
  .await
  .map_err(Into::into)
}

#[tauri::command]
pub async fn create_data_source(
  state: State<'_, AppState>,
  input: CreateDataSourceInput,
) -> CommandResult<DataSourceSummary> {
  connectors::probe(
    &input.kind,
    &input.host,
    input.port,
    &input.database_name,
    &input.username,
    &input.password,
  )
  .await?;

  let now = Utc::now().to_rfc3339();
  let id = Uuid::now_v7().to_string();
  let secret_ref = format!("datasource:{id}");
  state.secrets.write_password(&secret_ref, &input.password)?;

  state
    .metadata
    .insert_data_source(NewDataSourceRecord {
      id: id.clone(),
      name: input.name,
      kind: input.kind,
      host: input.host,
      port: input.port,
      database_name: input.database_name,
      username: input.username,
      secret_ref,
      last_probe_status: "ok".into(),
      last_probe_at: Utc::now(),
      last_error: None,
    })
    .await?;

  let saved = state.metadata.get_data_source(&id).await?;

  Ok(DataSourceSummary {
    id: saved.id,
    name: saved.name,
    kind: saved.kind.as_str().into(),
    host: saved.host,
    port: i64::from(saved.port),
    database_name: saved.database_name,
    username: saved.username,
    last_probe_status: Some("ok".into()),
    last_probe_at: Some(now),
    last_error: None,
  })
}

#[tauri::command]
pub async fn list_schemas(
  state: State<'_, AppState>,
  data_source_id: String,
) -> CommandResult<Vec<SchemaCatalog>> {
  let record = state.metadata.get_data_source(&data_source_id).await?;
  let password = state.secrets.read_password(&record.secret_ref)?;
  connectors::list_schemas(&record, &password).await.map_err(Into::into)
}

#[tauri::command]
pub async fn list_schema_tables(
  state: State<'_, AppState>,
  data_source_id: String,
  schema_name: String,
) -> CommandResult<Vec<TableCatalog>> {
  let record = state.metadata.get_data_source(&data_source_id).await?;
  let password = state.secrets.read_password(&record.secret_ref)?;
  connectors::list_schema_tables(&record, &password, &schema_name)
    .await
    .map_err(Into::into)
}

#[tauri::command]
pub async fn plan_sync_job(input: CreateSyncJobInput) -> CommandResult<SyncPlanPreview> {
  sync::plan_job(&input).map_err(Into::into)
}

#[tauri::command]
pub async fn create_sync_job(
  state: State<'_, AppState>,
  input: CreateSyncJobInput,
) -> CommandResult<SyncJobSummaryDto> {
  validate_sync_job(&input)?;

  state
    .metadata
    .insert_sync_job(NewSyncJobRecord {
      id: Uuid::now_v7().to_string(),
      name: input.name,
      source_id: input.source_id,
      target_id: input.target_id,
      mode: input.mode.as_str().into(),
      status: "draft".into(),
      strategy: input.strategy,
      selected_tables: input.selected_tables,
      created_at: Utc::now(),
    })
    .await?;

  state
    .metadata
    .list_sync_jobs()
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| AppError::Message("failed to load created sync job".into()).into())
}

#[tauri::command]
pub async fn list_sync_jobs(state: State<'_, AppState>) -> CommandResult<Vec<SyncJobSummaryDto>> {
  state.metadata.list_sync_jobs().await.map_err(Into::into)
}

#[tauri::command]
pub async fn preview_structure_diff(
  state: State<'_, AppState>,
  job_id: String,
) -> CommandResult<StructureDiffPreview> {
  let job = state.metadata.get_sync_job(&job_id).await?;
  let (_, _, source_id, target_id, _, selected_tables) =
    state.metadata.get_sync_job_definition(&job_id).await?;
  let source = state.metadata.get_data_source(&source_id).await?;
  let target = state.metadata.get_data_source(&target_id).await?;
  let source_password = state.secrets.read_password(&source.secret_ref)?;
  let target_password = state.secrets.read_password(&target.secret_ref)?;

  diff::preview(
    &source,
    &source_password,
    &target,
    &target_password,
    &selected_tables,
  )
  .await
  .map(|mut preview| {
    preview.summary = format!("{} · {}", job.name, preview.summary);
    preview
  })
  .map_err(Into::into)
}

#[tauri::command]
pub async fn run_sync_job(
  state: State<'_, AppState>,
  job_id: String,
) -> CommandResult<SyncRunSummary> {
  let job = state.metadata.get_sync_job(&job_id).await?;
  let (_, _, source_id, target_id, mode, _) = state.metadata.get_sync_job_definition(&job_id).await?;
  let source = state.metadata.get_data_source(&source_id).await?;
  let target = state.metadata.get_data_source(&target_id).await?;
  let source_password = state.secrets.read_password(&source.secret_ref)?;
  let target_password = state.secrets.read_password(&target.secret_ref)?;

  if mode == "incremental" {
    return Err(AppError::Message(
      "incremental-only execution is not wired yet; use full or full_then_incremental".into(),
    )
    .into());
  }

  executor::run_full_sync(
    &state.metadata,
    &job,
    &source,
    &source_password,
    &target,
    &target_password,
  )
  .await
  .map_err(Into::into)
}

#[tauri::command]
pub async fn list_sync_runs(state: State<'_, AppState>) -> CommandResult<Vec<SyncRunSummary>> {
  state.metadata.list_sync_runs().await.map_err(Into::into)
}

#[tauri::command]
pub async fn assess_incremental_sync(
  state: State<'_, AppState>,
  job_id: String,
) -> CommandResult<IncrementalAssessment> {
  let job = state.metadata.get_sync_job(&job_id).await?;
  let (_, _, source_id, target_id, _, _) = state.metadata.get_sync_job_definition(&job_id).await?;
  let source = state.metadata.get_data_source(&source_id).await?;
  let target = state.metadata.get_data_source(&target_id).await?;
  let source_password = state.secrets.read_password(&source.secret_ref)?;

  cdc::assess(&state.metadata, &job, &source, &source_password, &target)
    .await
    .map_err(Into::into)
}

fn validate_sync_job(input: &CreateSyncJobInput) -> Result<(), AppError> {
  if input.source_id == input.target_id {
    return Err(AppError::Message("source and target must be different".into()));
  }

  if input.name.trim().is_empty() {
    return Err(AppError::Message("sync job name is required".into()));
  }

  if input.selected_tables.is_empty() {
    return Err(AppError::Message("select at least one table".into()));
  }

  Ok(())
}
