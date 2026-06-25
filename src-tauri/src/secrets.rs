use crate::error::AppResult;

#[derive(Clone)]
pub struct SecretStore {
  service_name: String,
}

impl SecretStore {
  pub fn new(service_name: impl Into<String>) -> Self {
    Self {
      service_name: service_name.into(),
    }
  }

  pub fn write_password(&self, secret_ref: &str, password: &str) -> AppResult<()> {
    let entry = keyring::Entry::new(&self.service_name, secret_ref)?;
    entry.set_password(password)?;
    Ok(())
  }

  pub fn read_password(&self, secret_ref: &str) -> AppResult<String> {
    let entry = keyring::Entry::new(&self.service_name, secret_ref)?;
    Ok(entry.get_password()?)
  }
}
