use serde::Serialize;
use thiserror::Error;

pub type AppResult<T> = Result<T, AppError>;
pub type CommandResult<T> = Result<T, CommandError>;

#[derive(Debug, Error)]
pub enum AppError {
  #[error("{0}")]
  Message(String),
  #[error("I/O error: {0}")]
  Io(#[from] std::io::Error),
  #[error("Database error: {0}")]
  Database(#[from] sqlx::Error),
  #[error("Secret store error: {0}")]
  SecretStore(#[from] keyring::Error),
  #[error("Tauri error: {0}")]
  Tauri(#[from] tauri::Error),
}

#[derive(Debug, Serialize)]
pub struct CommandError {
  pub message: String,
}

impl From<AppError> for CommandError {
  fn from(error: AppError) -> Self {
    Self {
      message: error.to_string(),
    }
  }
}
