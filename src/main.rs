mod col;
mod settings;
use anyhow::{Context, Result};
use config::Config;
use settings::Settings;
use std::env;

fn main() -> Result<()> {
    let config_path = env::args()
        .nth(1)
        .context("Usage: myapp <config-file.toml>")?;

    let settings: Settings = Config::builder()
        .add_source(config::File::with_name(&config_path))
        .build()
        .with_context(|| format!("Failed to load config from '{}'", config_path))?
        .try_deserialize()
        .context("Failed to deserialize config")?;

    if settings.debug {
        eprintln!("Config:\n{:#?}", settings);
    }

    Ok(())
}
