use std::path::Path;

use chrono::Utc;
use sqlx::{
  Row, SqlitePool,
  sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};

use crate::{
  error::AppResult,
  models::{
    CdcCheckpoint, DashboardSnapshot, DataSourceKind, DataSourceRecord, DataSourceSummary,
    NewDataSourceRecord, NewSyncJobRecord, NewSyncRunRecord, SyncJobSummary, SyncJobSummaryDto,
    SyncRunSummary,
  },
};

#[derive(Clone)]
pub struct MetadataStore {
  pool: SqlitePool,
}

impl MetadataStore {
  pub async fn connect(database_path: &Path) -> AppResult<Self> {
    let options = SqliteConnectOptions::new()
      .filename(database_path)
      .create_if_missing(true)
      .journal_mode(SqliteJournalMode::Wal)
      .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
      .max_connections(6)
      .connect_with(options)
      .await?;

    let store = Self { pool };
    store.initialize().await?;
    Ok(store)
  }

  async fn initialize(&self) -> AppResult<()> {
    sqlx::query(
      r#"
      CREATE TABLE IF NOT EXISTS data_sources (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        kind TEXT NOT NULL,
        host TEXT NOT NULL,
        port INTEGER NOT NULL,
        database_name TEXT NOT NULL,
        username TEXT NOT NULL,
        secret_ref TEXT NOT NULL,
        last_probe_status TEXT,
        last_probe_at TEXT,
        last_error TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
      "#,
    )
    .execute(&self.pool)
    .await?;

    sqlx::query(
      r#"
      CREATE TABLE IF NOT EXISTS sync_jobs (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        source_id TEXT NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
        target_id TEXT NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
        mode TEXT NOT NULL,
        status TEXT NOT NULL,
        strategy TEXT NOT NULL,
        selected_tables TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
      "#,
    )
    .execute(&self.pool)
    .await?;

    sqlx::query(
      r#"
      CREATE TABLE IF NOT EXISTS sync_runs (
        id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL REFERENCES sync_jobs(id) ON DELETE CASCADE,
        job_name TEXT NOT NULL,
        status TEXT NOT NULL,
        tables_total INTEGER NOT NULL,
        tables_completed INTEGER NOT NULL,
        rows_copied INTEGER NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        detail TEXT
      )
      "#,
    )
    .execute(&self.pool)
    .await?;

    sqlx::query(
      r#"
      CREATE TABLE IF NOT EXISTS cdc_checkpoints (
        job_id TEXT PRIMARY KEY REFERENCES sync_jobs(id) ON DELETE CASCADE,
        mode TEXT NOT NULL,
        payload TEXT NOT NULL,
        updated_at TEXT NOT NULL
      )
      "#,
    )
    .execute(&self.pool)
    .await?;

    Ok(())
  }

  pub async fn dashboard_snapshot(&self) -> AppResult<DashboardSnapshot> {
    Ok(DashboardSnapshot {
      data_source_count: self.scalar_count("SELECT COUNT(*) FROM data_sources").await?,
      sync_job_count: self.scalar_count("SELECT COUNT(*) FROM sync_jobs").await?,
      mysql_source_count: self
        .scalar_count("SELECT COUNT(*) FROM data_sources WHERE kind = 'mysql'")
        .await?,
      postgres_source_count: self
        .scalar_count("SELECT COUNT(*) FROM data_sources WHERE kind = 'postgresql'")
        .await?,
      sync_run_count: self.scalar_count("SELECT COUNT(*) FROM sync_runs").await?,
      running_sync_count: self
        .scalar_count("SELECT COUNT(*) FROM sync_runs WHERE status = 'running'")
        .await?,
    })
  }

  pub async fn list_data_sources(&self) -> AppResult<Vec<DataSourceSummary>> {
    sqlx::query_as::<_, DataSourceSummary>(
      r#"
      SELECT
        id,
        name,
        kind,
        host,
        port,
        database_name,
        username,
        last_probe_status,
        last_probe_at,
        last_error
      FROM data_sources
      ORDER BY created_at DESC
      "#,
    )
    .fetch_all(&self.pool)
    .await
    .map_err(Into::into)
  }

  pub async fn get_data_source(&self, id: &str) -> AppResult<DataSourceRecord> {
    let row = sqlx::query(
      r#"
      SELECT id, name, kind, host, port, database_name, username, secret_ref
      FROM data_sources
      WHERE id = ?1
      "#,
    )
    .bind(id)
    .fetch_one(&self.pool)
    .await?;

    Ok(DataSourceRecord {
      id: row.try_get("id")?,
      name: row.try_get("name")?,
      kind: DataSourceKind::try_from(row.try_get::<String, _>("kind")?.as_str())?,
      host: row.try_get("host")?,
      port: row.try_get::<i64, _>("port")? as u16,
      database_name: row.try_get("database_name")?,
      username: row.try_get("username")?,
      secret_ref: row.try_get("secret_ref")?,
    })
  }

  pub async fn get_sync_job(&self, job_id: &str) -> AppResult<SyncJobSummaryDto> {
    let row = sqlx::query_as::<_, SyncJobSummary>(
      r#"
      SELECT
        sync_jobs.id,
        sync_jobs.name,
        source_ds.name AS source_name,
        target_ds.name AS target_name,
        sync_jobs.mode,
        sync_jobs.status,
        sync_jobs.strategy,
        sync_jobs.selected_tables,
        sync_jobs.created_at
      FROM sync_jobs
      INNER JOIN data_sources AS source_ds ON source_ds.id = sync_jobs.source_id
      INNER JOIN data_sources AS target_ds ON target_ds.id = sync_jobs.target_id
      WHERE sync_jobs.id = ?1
      "#,
    )
    .bind(job_id)
    .fetch_one(&self.pool)
    .await?;

    row.into_dto()
  }

  pub async fn get_sync_job_definition(&self, job_id: &str) -> AppResult<(String, String, String, String, String, Vec<String>)> {
    let row = sqlx::query(
      r#"
      SELECT id, name, source_id, target_id, mode, selected_tables
      FROM sync_jobs
      WHERE id = ?1
      "#,
    )
    .bind(job_id)
    .fetch_one(&self.pool)
    .await?;

    Ok((
      row.try_get("id")?,
      row.try_get("name")?,
      row.try_get("source_id")?,
      row.try_get("target_id")?,
      row.try_get("mode")?,
      serde_json::from_str::<Vec<String>>(&row.try_get::<String, _>("selected_tables")?)
        .map_err(|error| crate::error::AppError::Message(error.to_string()))?,
    ))
  }

  pub async fn insert_data_source(&self, record: NewDataSourceRecord) -> AppResult<()> {
    let now = Utc::now().to_rfc3339();

    sqlx::query(
      r#"
      INSERT INTO data_sources (
        id, name, kind, host, port, database_name, username, secret_ref,
        last_probe_status, last_probe_at, last_error, created_at, updated_at
      )
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12)
      "#,
    )
    .bind(record.id)
    .bind(record.name)
    .bind(record.kind.as_str())
    .bind(record.host)
    .bind(i64::from(record.port))
    .bind(record.database_name)
    .bind(record.username)
    .bind(record.secret_ref)
    .bind(record.last_probe_status)
    .bind(record.last_probe_at.to_rfc3339())
    .bind(record.last_error)
    .bind(now)
    .execute(&self.pool)
    .await?;

    Ok(())
  }

  pub async fn insert_sync_job(&self, record: NewSyncJobRecord) -> AppResult<()> {
    let selected_tables = serde_json::to_string(&record.selected_tables)
      .map_err(|error| crate::error::AppError::Message(error.to_string()))?;
    let timestamp = record.created_at.to_rfc3339();

    sqlx::query(
      r#"
      INSERT INTO sync_jobs (
        id, name, source_id, target_id, mode, status, strategy, selected_tables, created_at, updated_at
      )
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
      "#,
    )
    .bind(record.id)
    .bind(record.name)
    .bind(record.source_id)
    .bind(record.target_id)
    .bind(record.mode)
    .bind(record.status)
    .bind(record.strategy)
    .bind(selected_tables)
    .bind(timestamp)
    .execute(&self.pool)
    .await?;

    Ok(())
  }

  pub async fn list_sync_jobs(&self) -> AppResult<Vec<SyncJobSummaryDto>> {
    let jobs = sqlx::query_as::<_, SyncJobSummary>(
      r#"
      SELECT
        sync_jobs.id,
        sync_jobs.name,
        source_ds.name AS source_name,
        target_ds.name AS target_name,
        sync_jobs.mode,
        sync_jobs.status,
        sync_jobs.strategy,
        sync_jobs.selected_tables,
        sync_jobs.created_at
      FROM sync_jobs
      INNER JOIN data_sources AS source_ds ON source_ds.id = sync_jobs.source_id
      INNER JOIN data_sources AS target_ds ON target_ds.id = sync_jobs.target_id
      ORDER BY sync_jobs.created_at DESC
      "#,
    )
    .fetch_all(&self.pool)
    .await?;

    jobs.into_iter().map(SyncJobSummary::into_dto).collect()
  }

  pub async fn create_sync_run(&self, record: NewSyncRunRecord) -> AppResult<()> {
    sqlx::query(
      r#"
      INSERT INTO sync_runs (
        id, job_id, job_name, status, tables_total, tables_completed, rows_copied, started_at, finished_at, detail
      )
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
      "#,
    )
    .bind(record.id)
    .bind(record.job_id)
    .bind(record.job_name)
    .bind(record.status)
    .bind(record.tables_total)
    .bind(record.tables_completed)
    .bind(record.rows_copied)
    .bind(record.started_at.to_rfc3339())
    .bind(record.finished_at.map(|value| value.to_rfc3339()))
    .bind(record.detail)
    .execute(&self.pool)
    .await?;

    Ok(())
  }

  pub async fn update_sync_run_progress(
    &self,
    run_id: &str,
    status: &str,
    tables_completed: i64,
    rows_copied: i64,
    detail: Option<&str>,
  ) -> AppResult<()> {
    sqlx::query(
      r#"
      UPDATE sync_runs
      SET status = ?2, tables_completed = ?3, rows_copied = ?4, detail = ?5
      WHERE id = ?1
      "#,
    )
    .bind(run_id)
    .bind(status)
    .bind(tables_completed)
    .bind(rows_copied)
    .bind(detail)
    .execute(&self.pool)
    .await?;

    Ok(())
  }

  pub async fn finish_sync_run(
    &self,
    run_id: &str,
    status: &str,
    tables_completed: i64,
    rows_copied: i64,
    detail: Option<&str>,
  ) -> AppResult<()> {
    sqlx::query(
      r#"
      UPDATE sync_runs
      SET
        status = ?2,
        tables_completed = ?3,
        rows_copied = ?4,
        finished_at = ?5,
        detail = ?6
      WHERE id = ?1
      "#,
    )
    .bind(run_id)
    .bind(status)
    .bind(tables_completed)
    .bind(rows_copied)
    .bind(Utc::now().to_rfc3339())
    .bind(detail)
    .execute(&self.pool)
    .await?;

    Ok(())
  }

  pub async fn list_sync_runs(&self) -> AppResult<Vec<SyncRunSummary>> {
    sqlx::query_as::<_, SyncRunSummary>(
      r#"
      SELECT
        id,
        job_id,
        job_name,
        status,
        tables_total,
        tables_completed,
        rows_copied,
        started_at,
        finished_at,
        detail
      FROM sync_runs
      ORDER BY started_at DESC
      LIMIT 20
      "#,
    )
    .fetch_all(&self.pool)
    .await
    .map_err(Into::into)
  }

  pub async fn upsert_checkpoint(&self, job_id: &str, mode: &str, payload: &str) -> AppResult<()> {
    let now = Utc::now().to_rfc3339();

    sqlx::query(
      r#"
      INSERT INTO cdc_checkpoints (job_id, mode, payload, updated_at)
      VALUES (?1, ?2, ?3, ?4)
      ON CONFLICT(job_id) DO UPDATE SET
        mode = excluded.mode,
        payload = excluded.payload,
        updated_at = excluded.updated_at
      "#,
    )
    .bind(job_id)
    .bind(mode)
    .bind(payload)
    .bind(now)
    .execute(&self.pool)
    .await?;

    Ok(())
  }

  pub async fn get_checkpoint(&self, job_id: &str) -> AppResult<Option<CdcCheckpoint>> {
    sqlx::query_as::<_, CdcCheckpoint>(
      r#"
      SELECT job_id, mode, payload, updated_at
      FROM cdc_checkpoints
      WHERE job_id = ?1
      "#,
    )
    .bind(job_id)
    .fetch_optional(&self.pool)
    .await
    .map_err(Into::into)
  }

  async fn scalar_count(&self, sql: &str) -> AppResult<i64> {
    let row = sqlx::query(sql).fetch_one(&self.pool).await?;
    Ok(row.try_get::<i64, _>(0)?)
  }
}
