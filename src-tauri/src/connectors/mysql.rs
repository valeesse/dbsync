use std::{collections::HashMap, time::Duration};

use serde_json::Value;
use sqlx::{
  MySql, MySqlPool, QueryBuilder, Row,
  mysql::{MySqlPoolOptions, MySqlQueryResult},
};
use tokio::time::timeout;

use crate::{
  error::{AppError, AppResult},
  models::{CdcSourceCapability, DataSourceRecord, ProbeResult, SchemaCatalog, TableCatalog, TableColumn, TableRef},
};

fn dsn(host: &str, port: u16, database_name: &str, username: &str, password: &str) -> String {
  format!(
    "mysql://{}:{}@{}:{}/{}",
    urlencoding::encode(username),
    urlencoding::encode(password),
    host,
    port,
    database_name
  )
}

async fn connect(record: &DataSourceRecord, password: &str) -> AppResult<MySqlPool> {
  timeout(
    Duration::from_secs(8),
    MySqlPoolOptions::new()
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
  .map_err(|_| AppError::Message("timed out while connecting to MySQL".into()))?
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
    kind: crate::models::DataSourceKind::Mysql,
    host: host.into(),
    port,
    database_name: database_name.into(),
    username: username.into(),
    secret_ref: String::new(),
  };
  let pool = connect(&record, password).await?;
  let row = sqlx::query("SELECT VERSION() AS version, DATABASE() AS current_database")
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

async fn list_schemas_from_pool(pool: &MySqlPool) -> AppResult<Vec<SchemaCatalog>> {
  let rows = sqlx::query(
    r#"
    SELECT t.table_schema AS schema_name, COUNT(*) AS table_count
    FROM information_schema.tables AS t
    WHERE t.table_type = 'BASE TABLE'
      AND t.table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
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

async fn sample_table_count(pool: &MySqlPool, schemas: &[SchemaCatalog]) -> AppResult<usize> {
  let mut total = 0usize;
  for schema in schemas.iter().take(3) {
    total += sqlx::query_scalar::<_, i64>(
      "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
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
    SELECT table_name, table_rows
    FROM information_schema.tables
    WHERE table_schema = ?1 AND table_type = 'BASE TABLE'
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
      row_estimate: table_row.try_get("table_rows")?,
      columns,
    });
  }
  Ok(tables)
}

async fn fetch_columns(pool: &MySqlPool, schema_name: &str, table_name: &str) -> AppResult<Vec<TableColumn>> {
  let column_rows = sqlx::query(
    r#"
    SELECT column_name, column_type, is_nullable, column_key, column_default
    FROM information_schema.columns
    WHERE table_schema = ?1 AND table_name = ?2
    ORDER BY ordinal_position
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
        data_type: column.try_get("column_type")?,
        nullable: column.try_get::<String, _>("is_nullable")? == "YES",
        is_primary_key: column.try_get::<String, _>("column_key")? == "PRI",
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
  let columns = table
    .columns
    .iter()
    .filter(|column| column.supports_value_transfer())
    .collect::<Vec<_>>();

  let mut expr = String::from("JSON_OBJECT(");
  for (index, column) in columns.iter().enumerate() {
    if index > 0 {
      expr.push_str(", ");
    }
    expr.push('\'');
    expr.push_str(&column.name.replace('\'', "''"));
    expr.push_str("', ");
    expr.push_str(&format!("`{}`", column.name.replace('`', "``")));
  }
  expr.push(')');

  let order_clause = primary_key_order(table);
  let sql = format!(
    "SELECT {expr} AS row_json FROM `{schema}`.`{table}` {order_clause} LIMIT ? OFFSET ?",
    schema = table.schema_name.replace('`', "``"),
    table = table.name.replace('`', "``")
  );

  let rows = sqlx::query(&sql)
    .bind(limit)
    .bind(offset)
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
    "TRUNCATE TABLE `{}`.`{}`",
    table_ref.schema.replace('`', "``"),
    table_ref.table.replace('`', "``")
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
  let mut builder = QueryBuilder::<MySql>::new(format!(
    "INSERT INTO `{}`.`{}` (",
    table_ref.schema.replace('`', "``"),
    table_ref.table.replace('`', "``")
  ));

  for (index, column) in columns.iter().enumerate() {
    if index > 0 {
      builder.push(", ");
    }
    builder.push(format!("`{}`", column.name.replace('`', "``")));
  }
  builder.push(") VALUES ");

  builder.push_values(rows.iter(), |mut separated, row| {
    for column in columns {
      push_mysql_value(&mut separated, row.get(&column.name), column);
    }
  });

  let result: MySqlQueryResult = builder.build().execute(&pool).await?;
  Ok(result.rows_affected() as usize)
}

fn push_mysql_value(
  builder: &mut sqlx::query_builder::Separated<'_, '_, MySql, &'static str>,
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
        "integer" | "bigint" | "smallint" | "boolean" => {
          builder.push_unseparated("CAST(");
          builder.push_bind_unseparated(payload);
          builder.push_unseparated(" AS SIGNED)");
        }
        "numeric" | "double" | "float" => {
          builder.push_unseparated("CAST(");
          builder.push_bind_unseparated(payload);
          builder.push_unseparated(" AS DECIMAL(65, 20))");
        }
        "timestamp" => {
          builder.push_unseparated("CAST(");
          builder.push_bind_unseparated(payload);
          builder.push_unseparated(" AS DATETIME)");
        }
        "date" => {
          builder.push_unseparated("CAST(");
          builder.push_bind_unseparated(payload);
          builder.push_unseparated(" AS DATE)");
        }
        "time" => {
          builder.push_unseparated("CAST(");
          builder.push_bind_unseparated(payload);
          builder.push_unseparated(" AS TIME)");
        }
        "json" => {
          builder.push_unseparated("CAST(");
          builder.push_bind_unseparated(payload);
          builder.push_unseparated(" AS JSON)");
        }
        _ => {
          builder.push_bind_unseparated(payload);
        }
      };
    }
  }
}

pub async fn inspect_cdc(record: &DataSourceRecord, password: &str) -> AppResult<CdcSourceCapability> {
  let pool = connect(record, password).await?;
  let row = sqlx::query(
    "SELECT @@log_bin AS log_bin, @@binlog_format AS binlog_format, @@server_id AS server_id",
  )
  .fetch_one(&pool)
  .await?;
  let log_bin = row.try_get::<i64, _>("log_bin").unwrap_or_default() == 1;
  let format = row.try_get::<String, _>("binlog_format").unwrap_or_else(|_| "UNKNOWN".into());
  let server_id = row.try_get::<i64, _>("server_id").unwrap_or_default();

  Ok(CdcSourceCapability {
    supported: true,
    ready: log_bin && format.eq_ignore_ascii_case("ROW") && server_id > 0,
    engine: "mysql-binlog".into(),
    details: vec![
      format!("log_bin={log_bin}"),
      format!("binlog_format={format}"),
      format!("server_id={server_id}"),
      "needs REPLICATION SLAVE / REPLICATION CLIENT or equivalent privileges".into(),
    ],
  })
}

fn primary_key_order(table: &TableCatalog) -> String {
  let keys = table
    .columns
    .iter()
    .filter(|column| column.is_primary_key)
    .map(|column| format!("`{}`", column.name.replace('`', "``")))
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
    Value::Bool(value) => {
      if *value { "1".into() } else { "0".into() }
    }
    Value::Number(value) => value.to_string(),
    Value::String(value) => value.clone(),
    _ => value.to_string(),
  }
}
