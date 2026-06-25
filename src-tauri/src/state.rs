use std::fs;

use tauri::{AppHandle, Manager};

use crate::{error::AppResult, metadata_store::MetadataStore, secrets::SecretStore};

#[derive(Clone)]
pub struct AppState {
  pub metadata: MetadataStore,
  pub secrets: SecretStore,
}

impl AppState {
  pub async fn boot(app: AppHandle) -> AppResult<Self> {
    let app_dir = app.path().app_data_dir()?;
    fs::create_dir_all(&app_dir)?;

    Ok(Self {
      metadata: MetadataStore::connect(&app_dir.join("dbsync.sqlite3")).await?,
      secrets: SecretStore::new("io.codex.dbsync"),
    })
  }
}
