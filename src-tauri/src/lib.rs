mod commands;
mod cdc;
mod connectors;
mod diff;
mod error;
mod executor;
mod metadata_store;
mod models;
mod secrets;
mod state;
mod sync;

use tauri::Manager;

use state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
  tauri::Builder::default()
    .plugin(
      tauri_plugin_log::Builder::default()
        .level(log::LevelFilter::Info)
        .build(),
    )
    .setup(|app| {
      let state = tauri::async_runtime::block_on(AppState::boot(app.handle().clone()))?;
      app.manage(state);
      Ok(())
    })
    .invoke_handler(tauri::generate_handler![
      commands::dashboard_snapshot,
      commands::list_data_sources,
      commands::probe_data_source,
      commands::create_data_source,
      commands::list_schemas,
      commands::list_schema_tables,
      commands::plan_sync_job,
      commands::create_sync_job,
      commands::list_sync_jobs,
      commands::preview_structure_diff,
      commands::run_sync_job,
      commands::list_sync_runs,
      commands::assess_incremental_sync,
    ])
    .run(tauri::generate_context!())
    .expect("error while running dbsync desktop application");
}
