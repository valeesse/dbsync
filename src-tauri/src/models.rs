use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataSourceKind {
  Mysql,
  Postgresql,
}

impl DataSourceKind {
  pub fn as_str(&self) -> &'static str {
    match self {
      Self::Mysql => "mysql",
      Self::Postgresql => "postgresql",
    }
  }
}

impl TryFrom<&str> for DataSourceKind {
  type Error = crate::error::AppError;

  fn try_from(value: &str) -> Result<Self, Self::Error> {
    match value {
      "mysql" => Ok(Self::Mysql),
      "postgresql" => Ok(Self::Postgresql),
      other => Err(crate::error::AppError::Message(format!(
        "unsupported datasource kind: {other}"
      ))),
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDataSourceInput {
  pub name: String,
  pub kind: DataSourceKind,
  pub host: String,
  pub port: u16,
  pub database_name: String,
  pub username: String,
  pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct DataSourceSummary {
  pub id: String,
  pub name: String,
  pub kind: String,
  pub host: String,
  pub port: i64,
  pub database_name: String,
  pub username: String,
  pub last_probe_status: Option<String>,
  pub last_probe_at: Option<String>,
  pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DataSourceRecord {
  pub id: String,
  pub name: String,
  pub kind: DataSourceKind,
  pub host: String,
  pub port: u16,
  pub database_name: String,
  pub username: String,
  pub secret_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeResult {
  pub current_database: String,
  pub database_version: String,
  pub schema_count: usize,
  pub table_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DashboardSnapshot {
  pub data_source_count: i64,
  pub sync_job_count: i64,
  pub mysql_source_count: i64,
  pub postgres_source_count: i64,
  pub sync_run_count: i64,
  pub running_sync_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaCatalog {
  pub name: String,
  pub table_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableColumn {
  pub name: String,
  pub data_type: String,
  pub nullable: bool,
  pub is_primary_key: bool,
  pub default_value: Option<String>,
}

impl TableColumn {
  pub fn canonical_family(&self) -> &'static str {
    let normalized = self.data_type.to_ascii_lowercase();

    if normalized.contains("bigint") {
      "bigint"
    } else if normalized.contains("smallint") || normalized.contains("tinyint") {
      "smallint"
    } else if normalized.contains("int") || normalized.contains("serial") {
      "integer"
    } else if normalized.contains("numeric") || normalized.contains("decimal") {
      "numeric"
    } else if normalized.contains("double") {
      "double"
    } else if normalized.contains("float") || normalized.contains("real") {
      "float"
    } else if normalized.contains("bool") {
      "boolean"
    } else if normalized.contains("json") {
      "json"
    } else if normalized.contains("timestamp") || normalized.contains("datetime") {
      "timestamp"
    } else if normalized == "date" {
      "date"
    } else if normalized == "time" || normalized.contains("time without") {
      "time"
    } else if normalized.contains("blob")
      || normalized.contains("binary")
      || normalized.contains("bytea")
    {
      "binary"
    } else if normalized.contains("char")
      || normalized.contains("text")
      || normalized.contains("enum")
      || normalized.contains("uuid")
    {
      "string"
    } else {
      "string"
    }
  }

  pub fn supports_value_transfer(&self) -> bool {
    self.canonical_family() != "binary"
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableCatalog {
  pub schema_name: String,
  pub name: String,
  pub row_estimate: Option<i64>,
  pub columns: Vec<TableColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
  Full,
  Incremental,
  FullThenIncremental,
}

impl SyncMode {
  pub fn as_str(&self) -> &'static str {
    match self {
      Self::Full => "full",
      Self::Incremental => "incremental",
      Self::FullThenIncremental => "full_then_incremental",
    }
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSyncJobInput {
  pub name: String,
  pub source_id: String,
  pub target_id: String,
  pub mode: SyncMode,
  pub selected_tables: Vec<String>,
  pub strategy: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPlanPreview {
  pub summary: String,
  pub risk_level: String,
  pub estimated_stages: usize,
  pub steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct SyncJobSummary {
  pub id: String,
  pub name: String,
  pub source_name: String,
  pub target_name: String,
  pub mode: String,
  pub status: String,
  pub strategy: String,
  pub selected_tables: String,
  pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncJobSummaryDto {
  pub id: String,
  pub name: String,
  pub source_name: String,
  pub target_name: String,
  pub mode: String,
  pub status: String,
  pub strategy: String,
  pub selected_tables: Vec<String>,
  pub created_at: String,
}

impl SyncJobSummary {
  pub fn into_dto(self) -> Result<SyncJobSummaryDto, crate::error::AppError> {
    Ok(SyncJobSummaryDto {
      id: self.id,
      name: self.name,
      source_name: self.source_name,
      target_name: self.target_name,
      mode: self.mode,
      status: self.status,
      strategy: self.strategy,
      selected_tables: serde_json::from_str(&self.selected_tables)
        .map_err(|error| crate::error::AppError::Message(error.to_string()))?,
      created_at: self.created_at,
    })
  }
}

#[derive(Debug, Clone, Serialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct SyncRunSummary {
  pub id: String,
  pub job_id: String,
  pub job_name: String,
  pub status: String,
  pub tables_total: i64,
  pub tables_completed: i64,
  pub rows_copied: i64,
  pub started_at: String,
  pub finished_at: Option<String>,
  pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StructureDiffPreview {
  pub source_name: String,
  pub target_name: String,
  pub summary: String,
  pub items: Vec<TableDiffItem>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableDiffItem {
  pub table_key: String,
  pub status: String,
  pub statements: Vec<String>,
  pub notes: Vec<String>,
  pub transferable_columns: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IncrementalAssessment {
  pub job_name: String,
  pub source_name: String,
  pub target_name: String,
  pub source_capability: CdcSourceCapability,
  pub checkpoint: Option<CdcCheckpoint>,
  pub event_model: Vec<String>,
  pub next_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CdcSourceCapability {
  pub supported: bool,
  pub ready: bool,
  pub engine: String,
  pub details: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
#[serde(rename_all = "camelCase")]
pub struct CdcCheckpoint {
  pub job_id: String,
  pub mode: String,
  pub payload: String,
  pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct NewDataSourceRecord {
  pub id: String,
  pub name: String,
  pub kind: DataSourceKind,
  pub host: String,
  pub port: u16,
  pub database_name: String,
  pub username: String,
  pub secret_ref: String,
  pub last_probe_status: String,
  pub last_probe_at: DateTime<Utc>,
  pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewSyncJobRecord {
  pub id: String,
  pub name: String,
  pub source_id: String,
  pub target_id: String,
  pub mode: String,
  pub status: String,
  pub strategy: String,
  pub selected_tables: Vec<String>,
  pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewSyncRunRecord {
  pub id: String,
  pub job_id: String,
  pub job_name: String,
  pub status: String,
  pub tables_total: i64,
  pub tables_completed: i64,
  pub rows_copied: i64,
  pub started_at: DateTime<Utc>,
  pub finished_at: Option<DateTime<Utc>>,
  pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRef {
  pub schema: String,
  pub table: String,
}

impl TableRef {
  pub fn parse(value: &str) -> Result<Self, crate::error::AppError> {
    let (schema, table) = value.split_once('.').ok_or_else(|| {
      crate::error::AppError::Message(format!("invalid table selector: {value}"))
    })?;

    Ok(Self {
      schema: schema.to_string(),
      table: table.to_string(),
    })
  }

  pub fn key(&self) -> String {
    format!("{}.{}", self.schema, self.table)
  }
}
