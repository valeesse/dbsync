use std::collections::HashMap;

use crate::{
  connectors,
  error::{AppError, AppResult},
  models::{DataSourceRecord, StructureDiffPreview, TableCatalog, TableColumn, TableDiffItem, TableRef},
};

pub async fn preview(
  source: &DataSourceRecord,
  source_password: &str,
  target: &DataSourceRecord,
  target_password: &str,
  selected_tables: &[String],
) -> AppResult<StructureDiffPreview> {
  let mut items = Vec::new();

  for selected in selected_tables {
    let table_ref = TableRef::parse(selected)?;
    let source_table = connectors::get_table(source, source_password, &table_ref)
      .await?
      .ok_or_else(|| AppError::Message(format!("source table {} not found", table_ref.key())))?;
    let target_table = connectors::get_table(target, target_password, &table_ref).await?;
    items.push(build_table_diff(source, target, &source_table, target_table.as_ref())?);
  }

  let create_count = items.iter().filter(|item| item.status == "missing_on_target").count();
  let alter_count = items.iter().filter(|item| item.status == "columns_missing").count();
  let conflict_count = items.iter().filter(|item| item.status == "type_conflict").count();

  Ok(StructureDiffPreview {
    source_name: source.name.clone(),
    target_name: target.name.clone(),
    summary: format!(
      "{} tables selected, {} create-table, {} alter-table, {} conflict(s)",
      items.len(),
      create_count,
      alter_count,
      conflict_count
    ),
    items,
  })
}

pub fn build_table_diff(
  source: &DataSourceRecord,
  target: &DataSourceRecord,
  source_table: &TableCatalog,
  target_table: Option<&TableCatalog>,
) -> AppResult<TableDiffItem> {
  let table_ref = TableRef {
    schema: source_table.schema_name.clone(),
    table: source_table.name.clone(),
  };
  let mut statements = Vec::new();
  let mut notes = Vec::new();
  let status;

  match target_table {
    None => {
      statements.push(build_create_table_statement(
        source,
        target,
        source_table,
        &table_ref,
      )?);
      let unsupported = source_table
        .columns
        .iter()
        .filter(|column| !column.supports_value_transfer())
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
      if !unsupported.is_empty() {
        notes.push(format!("binary-like columns skipped in value sync: {}", unsupported.join(", ")));
      }
      status = "missing_on_target";
    }
    Some(target_table) => {
      let target_columns = target_table
        .columns
        .iter()
        .map(|column| (column.name.clone(), column))
        .collect::<HashMap<_, _>>();
      let mut missing_columns = Vec::new();
      let mut conflicts = Vec::new();

      for source_column in &source_table.columns {
        match target_columns.get(&source_column.name) {
          None => missing_columns.push(source_column.clone()),
          Some(target_column) => {
            if source_column.canonical_family() != target_column.canonical_family() {
              conflicts.push(format!(
                "{}: source={} target={}",
                source_column.name, source_column.data_type, target_column.data_type
              ));
            }
          }
        }
      }

      if !missing_columns.is_empty() {
        statements.extend(
          missing_columns
            .iter()
            .map(|column| build_add_column_statement(source, target, &table_ref, column))
            .collect::<AppResult<Vec<_>>>()?,
        );
      }

      if !conflicts.is_empty() {
        notes.extend(conflicts);
        status = "type_conflict";
      } else if !missing_columns.is_empty() {
        status = "columns_missing";
      } else {
        status = "compatible";
      }
    }
  }

  let transferable_columns = source_table
    .columns
    .iter()
    .filter(|column| column.supports_value_transfer())
    .count();

  Ok(TableDiffItem {
    table_key: table_ref.key(),
    status: status.into(),
    statements,
    notes,
    transferable_columns,
  })
}

fn build_create_table_statement(
  source: &DataSourceRecord,
  target: &DataSourceRecord,
  source_table: &TableCatalog,
  table_ref: &TableRef,
) -> AppResult<String> {
  let schema = connectors::quote_identifier(&target.kind, &table_ref.schema)?;
  let table = connectors::quote_identifier(&target.kind, &table_ref.table)?;
  let mut column_defs = source_table
    .columns
    .iter()
    .map(|column| build_column_definition(source, target, column))
    .collect::<AppResult<Vec<_>>>()?;

  let primary_keys = source_table
    .columns
    .iter()
    .filter(|column| column.is_primary_key)
    .map(|column| connectors::quote_identifier(&target.kind, &column.name))
    .collect::<Result<Vec<_>, _>>()?;

  if !primary_keys.is_empty() {
    column_defs.push(format!("PRIMARY KEY ({})", primary_keys.join(", ")));
  }

  Ok(format!(
    "CREATE TABLE {schema}.{table} (\n  {}\n)",
    column_defs.join(",\n  ")
  ))
}

fn build_add_column_statement(
  source: &DataSourceRecord,
  target: &DataSourceRecord,
  table_ref: &TableRef,
  column: &TableColumn,
) -> AppResult<String> {
  let schema = connectors::quote_identifier(&target.kind, &table_ref.schema)?;
  let table = connectors::quote_identifier(&target.kind, &table_ref.table)?;
  let definition = build_column_definition(source, target, column)?;
  Ok(format!("ALTER TABLE {schema}.{table} ADD COLUMN {definition}"))
}

fn build_column_definition(
  source: &DataSourceRecord,
  target: &DataSourceRecord,
  column: &TableColumn,
) -> AppResult<String> {
  let name = connectors::quote_identifier(&target.kind, &column.name)?;
  let ty = connectors::map_column_type_for_target(&source.kind, &target.kind, column);
  let nullable = if column.nullable { "" } else { " NOT NULL" };
  Ok(format!("{name} {ty}{nullable}"))
}
