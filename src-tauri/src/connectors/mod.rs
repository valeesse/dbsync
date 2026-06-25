mod mysql;
mod postgresql;

use std::collections::HashMap;

use serde_json::Value;

use crate::{
  error::{AppError, AppResult},
  models::{
    CdcSourceCapability, DataSourceKind, DataSourceRecord, ProbeResult, SchemaCatalog, TableCatalog,
    TableColumn, TableRef,
  },
};

pub async fn probe(
  kind: &DataSourceKind,
  host: &str,
  port: u16,
  database_name: &str,
  username: &str,
  password: &str,
) -> AppResult<ProbeResult> {
  match kind {
    DataSourceKind::Mysql => mysql::probe(host, port, database_name, username, password).await,
    DataSourceKind::Postgresql => {
      postgresql::probe(host, port, database_name, username, password).await
    }
  }
}

pub async fn list_schemas(record: &DataSourceRecord, password: &str) -> AppResult<Vec<SchemaCatalog>> {
  match record.kind {
    DataSourceKind::Mysql => mysql::list_schemas(record, password).await,
    DataSourceKind::Postgresql => postgresql::list_schemas(record, password).await,
  }
}

pub async fn list_schema_tables(
  record: &DataSourceRecord,
  password: &str,
  schema_name: &str,
) -> AppResult<Vec<TableCatalog>> {
  match record.kind {
    DataSourceKind::Mysql => mysql::list_schema_tables(record, password, schema_name).await,
    DataSourceKind::Postgresql => {
      postgresql::list_schema_tables(record, password, schema_name).await
    }
  }
}

pub async fn get_table(
  record: &DataSourceRecord,
  password: &str,
  table_ref: &TableRef,
) -> AppResult<Option<TableCatalog>> {
  let tables = list_schema_tables(record, password, &table_ref.schema).await?;
  Ok(tables.into_iter().find(|table| table.name == table_ref.table))
}

pub async fn fetch_table_rows_json(
  record: &DataSourceRecord,
  password: &str,
  table: &TableCatalog,
  offset: i64,
  limit: i64,
) -> AppResult<Vec<HashMap<String, Value>>> {
  match record.kind {
    DataSourceKind::Mysql => mysql::fetch_table_rows_json(record, password, table, offset, limit).await,
    DataSourceKind::Postgresql => {
      postgresql::fetch_table_rows_json(record, password, table, offset, limit).await
    }
  }
}

pub async fn truncate_table(
  record: &DataSourceRecord,
  password: &str,
  table_ref: &TableRef,
) -> AppResult<()> {
  match record.kind {
    DataSourceKind::Mysql => mysql::truncate_table(record, password, table_ref).await,
    DataSourceKind::Postgresql => postgresql::truncate_table(record, password, table_ref).await,
  }
}

pub async fn create_table(
  record: &DataSourceRecord,
  password: &str,
  ddl: &str,
) -> AppResult<()> {
  match record.kind {
    DataSourceKind::Mysql => mysql::execute_sql(record, password, ddl).await,
    DataSourceKind::Postgresql => postgresql::execute_sql(record, password, ddl).await,
  }
}

pub async fn insert_rows(
  record: &DataSourceRecord,
  password: &str,
  table_ref: &TableRef,
  columns: &[TableColumn],
  rows: &[HashMap<String, Value>],
) -> AppResult<usize> {
  if rows.is_empty() {
    return Ok(0);
  }

  match record.kind {
    DataSourceKind::Mysql => mysql::insert_rows(record, password, table_ref, columns, rows).await,
    DataSourceKind::Postgresql => {
      postgresql::insert_rows(record, password, table_ref, columns, rows).await
    }
  }
}

pub async fn inspect_cdc(
  record: &DataSourceRecord,
  password: &str,
) -> AppResult<CdcSourceCapability> {
  match record.kind {
    DataSourceKind::Mysql => mysql::inspect_cdc(record, password).await,
    DataSourceKind::Postgresql => postgresql::inspect_cdc(record, password).await,
  }
}

pub fn map_column_type_for_target(
  source_kind: &DataSourceKind,
  target_kind: &DataSourceKind,
  column: &TableColumn,
) -> String {
  match (source_kind, target_kind) {
    (DataSourceKind::Mysql, DataSourceKind::Postgresql) => map_mysql_to_pg(&column.data_type),
    (DataSourceKind::Postgresql, DataSourceKind::Mysql) => map_pg_to_mysql(&column.data_type),
    _ => column.data_type.clone(),
  }
}

fn map_mysql_to_pg(source: &str) -> String {
  let lower = source.to_ascii_lowercase();
  if lower.starts_with("varchar") {
    source.to_ascii_uppercase()
  } else if lower.starts_with("char") {
    source.to_ascii_uppercase()
  } else if lower.contains("text") {
    "TEXT".into()
  } else if lower.starts_with("bigint") {
    "BIGINT".into()
  } else if lower.starts_with("tinyint(1)") || lower == "boolean" {
    "BOOLEAN".into()
  } else if lower.starts_with("tinyint") || lower.starts_with("smallint") {
    "SMALLINT".into()
  } else if lower.starts_with("mediumint") || lower.starts_with("int") {
    "INTEGER".into()
  } else if lower.starts_with("decimal") || lower.starts_with("numeric") {
    source.to_ascii_uppercase().replace("DECIMAL", "NUMERIC")
  } else if lower.starts_with("double") {
    "DOUBLE PRECISION".into()
  } else if lower.starts_with("float") {
    "REAL".into()
  } else if lower.starts_with("datetime") || lower.starts_with("timestamp") {
    "TIMESTAMP".into()
  } else if lower == "date" {
    "DATE".into()
  } else if lower == "time" {
    "TIME".into()
  } else if lower.contains("json") {
    "JSONB".into()
  } else if lower.contains("blob") || lower.contains("binary") {
    "BYTEA".into()
  } else {
    "TEXT".into()
  }
}

fn map_pg_to_mysql(source: &str) -> String {
  let lower = source.to_ascii_lowercase();
  if lower.starts_with("character varying") || lower.starts_with("varchar") {
    "VARCHAR(255)".into()
  } else if lower.starts_with("character(") || lower.starts_with("char(") {
    "CHAR(1)".into()
  } else if lower.contains("text") || lower.contains("uuid") {
    "TEXT".into()
  } else if lower.contains("bigint") {
    "BIGINT".into()
  } else if lower.contains("smallint") {
    "SMALLINT".into()
  } else if lower.contains("integer") || lower.contains("serial") {
    "INT".into()
  } else if lower.contains("numeric") || lower.contains("decimal") {
    "DECIMAL(38, 10)".into()
  } else if lower.contains("double") {
    "DOUBLE".into()
  } else if lower.contains("real") {
    "FLOAT".into()
  } else if lower.contains("boolean") {
    "BOOLEAN".into()
  } else if lower.contains("timestamp") {
    "DATETIME".into()
  } else if lower == "date" {
    "DATE".into()
  } else if lower == "time" || lower.contains("time without") {
    "TIME".into()
  } else if lower.contains("json") {
    "JSON".into()
  } else if lower.contains("bytea") {
    "LONGBLOB".into()
  } else {
    "TEXT".into()
  }
}

pub fn quote_identifier(kind: &DataSourceKind, value: &str) -> Result<String, AppError> {
  if value.is_empty() {
    return Err(AppError::Message("identifier cannot be empty".into()));
  }

  if value.contains('\0') {
    return Err(AppError::Message("identifier contains null byte".into()));
  }

  Ok(match kind {
    DataSourceKind::Mysql => format!("`{}`", value.replace('`', "``")),
    DataSourceKind::Postgresql => format!("\"{}\"", value.replace('"', "\"\"")),
  })
}
