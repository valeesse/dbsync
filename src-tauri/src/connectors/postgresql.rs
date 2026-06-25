use std::{collections::HashMap, time::Duration};

use serde_json::Value;
use sqlx::{
  Postgres, QueryBuilder, Row,
  postgres::{PgPool, PgPoolOptions, PgQueryResult},
};
use tokio::time::timeout;

use crate::{
  error::{AppError, AppResult},
  models::{CdcSourceCapability, DataSourceRecord, ProbeResult, SchemaCatalog, TableCatalog, TableColumn, TableRef},
};

fn dsn(host: &str, port: u16, database_name: &str, username: &str, password: &str) -> String {
  format!(
    "postgres://{}:{}@{}:{}/{}",
    urlencoding::encode(username),
    urlencoding::encode(password),
    host,
    port,
    database_name
  )
}

async fn connect(record: &DataSourceRecord, password: &str) -> AppResult<PgPool> {
  timeout(
    Duration::from_secs(8),
    PgPoolOptions::new()
      .acquire_timeout(Duration::from_secs(8))
      .max_connections(4)
      .connect(&dsn(
        &record.host,
        record.port,
        &record.database_name,
        &record.username,
        password,
      )),
  )
  .await
  .map_err(|_| AppError::Message("timed out while connecting to PostgreSQL".into()))?
  .map_err(Into::into)
}

pub async fn probe(
  host: &str,
  port: u16,
  database_name: &str,
  username: &str,
  password: &str,
) -> AppResult<ProbeResult> {
  let record = DataSourceRecord {
    id: String::new(),
    name: String::new(),
    kind: crate::models::DataSourceKind::Postgresql,
    host: host.into(),
    port,
    database_name: database_name.into(),
    username: username.into(),
    secret_ref: String::new(),
  };
  let pool = connect(&record, password).await?;
  let row = sqlx::query("SELECT version() AS version, current_database() AS current_database")
    .fetch_one(&pool)
    .await?;
  let schemas = list_schemas_from_pool(&pool).await?;
  Ok(ProbeResult {
    current_database: row.try_get("current_database")?,
    database_version: row.try_get("version")?,
    schema_count: schemas.len(),
    table_count: sample_table_count(&pool, &schemas).await?,
  })
}

pub async fn list_schemas(record: &DataSourceRecord, password: &str) -> AppResult<Vec<SchemaCatalog>> {
  let pool = connect(record, password).await?;
  list_schemas_from_pool(&pool).await
}

async fn list_schemas_from_pool(pool: &PgPool) -> AppResult<Vec<SchemaCatalog>> {
  let rows = sqlx::query(
    r#"
    SELECT t.table_schema AS schema_name, COUNT(*) AS table_count
    FROM information_schema.tables AS t
    WHERE t.table_type = 'BASE TABLE'
      AND t.table_schema NOT IN ('information_schema', 'pg_catalog')
      AND t.table_schema NOT LIKE 'pg_toast%'
      AND t.table_schema NOT LIKE 'pg_temp_%'
    GROUP BY t.table_schema
    ORDER BY t.table_schema
    "#,
  )
  .fetch_all(pool)
  .await?;

  rows
    .into_iter()
    .map(|row| {
      Ok(SchemaCatalog {
        name: row.try_get("schema_name")?,
        table_count: row.try_get::<i64, _>("table_count")? as usize,
      })
    })
    .collect()
}

async fn sample_table_count(pool: &PgPool, schemas: &[SchemaCatalog]) -> AppResult<usize> {
  let mut total = 0usize;
  for schema in schemas.iter().take(3) {
    total += sqlx::query_scalar::<_, i64>(
      "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
    )
    .bind(&schema.name)
    .fetch_one(pool)
    .await? as usize;
  }
  Ok(total)
}

pub async fn list_schema_tables(
  record: &DataSourceRecord,
  password: &str,
  schema_name: &str,
) -> AppResult<Vec<TableCatalog>> {
  let pool = connect(record, password).await?;
  let table_rows = sqlx::query(
    r#"
    SELECT
      table_name,
      (
        SELECT c.reltuples::BIGINT
        FROM pg_class AS c
        INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.relname = t.table_name AND n.nspname = t.table_schema
      ) AS row_estimate
    FROM information_schema.tables AS t
    WHERE table_schema = $1 AND table_type = 'BASE TABLE'
    ORDER BY table_name
    "#,
  )
  .bind(schema_name)
  .fetch_all(&pool)
  .await?;

  let mut tables = Vec::new();
  for table_row in table_rows {
    let table_name = table_row.try_get::<String, _>("table_name")?;
    let columns = fetch_columns(&pool, schema_name, &table_name).await?;
    tables.push(TableCatalog {
      schema_name: schema_name.to_string(),
      name: table_name,
      row_estimate: table_row.try_get("row_estimate")?,
      columns,
    });
  }
  Ok(tables)
}

async fn fetch_columns(pool: &PgPool, schema_name: &str, table_name: &str) -> AppResult<Vec<TableColumn>> {
  let column_rows = sqlx::query(
    r#"
    SELECT
      c.column_name,
      c.data_type,
      c.is_nullable,
      EXISTS (
        SELECT 1
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = c.table_schema
          AND tc.table_name = c.table_name
          AND kcu.column_name = c.column_name
      ) AS is_primary_key,
      c.column_default
    FROM information_schema.columns AS c
    WHERE c.table_schema = $1 AND c.table_name = $2
    ORDER BY c.ordinal_position
    "#,
  )
  .bind(schema_name)
  .bind(table_name)
  .fetch_all(pool)
  .await?;

  column_rows
    .into_iter()
    .map(|column| {
      Ok(TableColumn {
        name: column.try_get("column_name")?,
        data_type: column.try_get("data_type")?,
        nullable: column.try_get::<String, _>("is_nullable")? == "YES",
        is_primary_key: column.try_get("is_primary_key")?,
        default_value: column.try_get("column_default")?,
      })
    })
    .collect()
}

pub async fn fetch_table_rows_json(
  record: &DataSourceRecord,
  password: &str,
  table: &TableCatalog,
  offset: i64,
  limit: i64,
) -> AppResult<Vec<HashMap<String, Value>>> {
  let pool = connect(record, password).await?;
  let selected_columns = table
    .columns
    .iter()
    .filter(|column| column.supports_value_transfer())
    .map(|column| format!("\"{}\"", column.name.replace('"', "\"\"")))
    .collect::<Vec<_>>();
  let order_clause = primary_key_order(table);
  let sql = format!(
    "SELECT row_to_json(t)::text AS row_json FROM (SELECT {columns} FROM \"{schema}\".\"{table}\" {order_clause} OFFSET $1 LIMIT $2) t",
    columns = selected_columns.join(", "),
    schema = table.schema_name.replace('"', "\"\""),
    table = table.name.replace('"', "\"\""),
  );

  let rows = sqlx::query(&sql)
    .bind(offset)
    .bind(limit)
    .fetch_all(&pool)
    .await?;

  rows
    .into_iter()
    .map(|row| {
      let json = row.try_get::<String, _>("row_json")?;
      serde_json::from_str::<HashMap<String, Value>>(&json)
        .map_err(|error| AppError::Message(error.to_string()))
    })
    .collect()
}

pub async fn truncate_table(record: &DataSourceRecord, password: &str, table_ref: &TableRef) -> AppResult<()> {
  let pool = connect(record, password).await?;
  let sql = format!(
    "TRUNCATE TABLE \"{}\".\"{}\"",
    table_ref.schema.replace('"', "\"\""),
    table_ref.table.replace('"', "\"\"")
  );
  sqlx::query(&sql).execute(&pool).await?;
  Ok(())
}

pub async fn execute_sql(record: &DataSourceRecord, password: &str, ddl: &str) -> AppResult<()> {
  let pool = connect(record, password).await?;
  sqlx::query(ddl).execute(&pool).await?;
  Ok(())
}

pub async fn insert_rows(
  record: &DataSourceRecord,
  password: &str,
  table_ref: &TableRef,
  columns: &[TableColumn],
  rows: &[HashMap<String, Value>],
) -> AppResult<usize> {
  let pool = connect(record, password).await?;
  let mut builder = QueryBuilder::<Postgres>::new(format!(
    "INSERT INTO \"{}\".\"{}\" (",
    table_ref.schema.replace('"', "\"\""),
    table_ref.table.replace('"', "\"\"")
  ));

  for (index, column) in columns.iter().enumerate() {
    if index > 0 {
      builder.push(", ");
    }
    builder.push(format!("\"{}\"", column.name.replace('"', "\"\"")));
  }
  builder.push(") VALUES ");

  builder.push_values(rows.iter(), |mut separated, row| {
    for column in columns {
      push_postgres_value(&mut separated, row.get(&column.name), column);
    }
  });

  let result: PgQueryResult = builder.build().execute(&pool).await?;
  Ok(result.rows_affected() as usize)
}

fn push_postgres_value(
  builder: &mut sqlx::query_builder::Separated<'_, '_, Postgres, &'static str>,
  value: Option<&Value>,
  column: &TableColumn,
) {
  match value {
    Some(Value::Null) | None => {
      builder.push_unseparated("NULL");
    }
    Some(raw) => {
      let payload = stringify_json_value(raw);
      match column.canonical_family() {
        "integer" => cast_bind(builder, payload, "INTEGER"),
        "bigint" => cast_bind(builder, payload, "BIGINT"),
        "smallint" => cast_bind(builder, payload, "SMALLINT"),
        "numeric" => cast_bind(builder, payload, "NUMERIC"),
        "double" => cast_bind(builder, payload, "DOUBLE PRECISION"),
        "float" => cast_bind(builder, payload, "REAL"),
        "boolean" => cast_bind(builder, payload, "BOOLEAN"),
        "timestamp" => cast_bind(builder, payload, "TIMESTAMP"),
        "date" => cast_bind(builder, payload, "DATE"),
        "time" => cast_bind(builder, payload, "TIME"),
        "json" => cast_bind(builder, payload, "JSONB"),
        _ => {
          builder.push_bind_unseparated(payload);
        }
      };
    }
  }
}

fn cast_bind(
  builder: &mut sqlx::query_builder::Separated<'_, '_, Postgres, &'static str>,
  payload: String,
  ty: &str,
) {
  builder.push_unseparated("CAST(");
  builder.push_bind_unseparated(payload);
  builder.push_unseparated(format!(" AS {ty})"));
}

pub async fn inspect_cdc(record: &DataSourceRecord, password: &str) -> AppResult<CdcSourceCapability> {
  let pool = connect(record, password).await?;
  let wal_level = sqlx::query_scalar::<_, String>("SELECT current_setting('wal_level')")
    .fetch_one(&pool)
    .await
    .unwrap_or_else(|_| "unknown".into());
  let max_slots = sqlx::query_scalar::<_, i64>("SELECT current_setting('max_replication_slots')::BIGINT")
    .fetch_one(&pool)
    .await
    .unwrap_or_default();
  let max_senders = sqlx::query_scalar::<_, i64>("SELECT current_setting('max_wal_senders')::BIGINT")
    .fetch_one(&pool)
    .await
    .unwrap_or_default();

  Ok(CdcSourceCapability {
    supported: true,
    ready: wal_level == "logical" && max_slots > 0 && max_senders > 0,
    engine: "postgres-logical-replication".into(),
    details: vec![
      format!("wal_level={wal_level}"),
      format!("max_replication_slots={max_slots}"),
      format!("max_wal_senders={max_senders}"),
      "needs REPLICATION privilege and publication / slot bootstrap".into(),
    ],
  })
}

fn primary_key_order(table: &TableCatalog) -> String {
  let keys = table
    .columns
    .iter()
    .filter(|column| column.is_primary_key)
    .map(|column| format!("\"{}\"", column.name.replace('"', "\"\"")))
    .collect::<Vec<_>>();
  if keys.is_empty() {
    String::new()
  } else {
    format!("ORDER BY {}", keys.join(", "))
  }
}

fn stringify_json_value(value: &Value) -> String {
  match value {
    Value::Null => String::new(),
    Value::Bool(value) => value.to_string(),
    Value::Number(value) => value.to_string(),
    Value::String(value) => value.clone(),
    _ => value.to_string(),
  }
}
