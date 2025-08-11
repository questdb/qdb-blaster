mod blasting;
mod col;
mod settings;
use anyhow::{Context, Result};
use config::Config;
use settings::Settings;
use std::env;
use tracing::{error, info};

fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

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

    info!("Starting QDB Blaster with {} tables", settings.tables.len());

    // Blast all tables in parallel
    let mut handles = Vec::new();
    for (table_name, table_config) in settings.tables {
        let database_connection = settings.database.clone();
        let table_name_for_thread = table_name.clone();
        let handle = std::thread::spawn(move || {
            if let Err(e) =
                blasting::blast_table(&table_name_for_thread, &table_config, &database_connection)
            {
                tracing::error!("Table '{}' failed: {}", table_name_for_thread, e);
                return Err(e);
            }
            tracing::info!("Table '{}' completed successfully", table_name_for_thread);
            Ok(())
        });
        handles.push((table_name, handle));
    }

    // Wait for all tables to complete
    let mut errors = Vec::new();
    for (table_name, handle) in handles {
        match handle.join() {
            Ok(Ok(())) => {
                info!("Table '{}' processing completed", table_name);
            }
            Ok(Err(e)) => {
                error!("Table '{}' failed: {}", table_name, e);
                errors.push(format!("Table '{}': {}", table_name, e));
            }
            Err(_) => {
                error!("Table '{}' thread panicked", table_name);
                errors.push(format!("Table '{}': thread panicked", table_name));
            }
        }
    }

    if !errors.is_empty() {
        return Err(anyhow::anyhow!("Some tables failed: {}", errors.join(", ")));
    }

    info!("All tables processed successfully");
    Ok(())
}
