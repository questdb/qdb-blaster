use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use postgres::{Client, NoTls};
use questdb::ingress::{Buffer, ColumnName, Sender as QuestDbSender, TableName, TimestampNanos};
use rand::{Rng, rngs::ThreadRng};
use tracing::{debug, error, info, warn};

use crate::{
    col::ColType,
    settings::{Connection, SendSettings, Table},
};

/// Pre-generated pool of symbol values to randomly select from
const SYMBOL_POOL_SIZE: usize = 4000;

/// Data generator for creating synthetic values for different column types
#[derive(Debug)]
struct DataGenerator {
    symbols: Vec<String>,
    base_timestamp: DateTime<Utc>,
    rng: ThreadRng,
}

impl DataGenerator {
    fn new(base_timestamp: DateTime<Utc>) -> Self {
        let rng = rand::rng();

        // Pre-generate symbol pool
        let symbols: Vec<String> = (0..SYMBOL_POOL_SIZE)
            .map(|i| {
                let variants = [
                    format!("host-{:04}", i % 100),
                    format!("service-{}", i % 50),
                    format!(
                        "region-{}",
                        ["us-east", "us-west", "eu-central", "ap-south"][i % 4]
                    ),
                    format!("env-{}", ["prod", "stage", "dev"][i % 3]),
                    format!("app-{:03}", i % 200),
                ];
                variants[i % variants.len()].clone()
            })
            .collect();

        Self {
            symbols,
            base_timestamp,
            rng,
        }
    }

    fn generate_symbol(&mut self) -> &str {
        let idx = self.rng.random_range(0..self.symbols.len());
        &self.symbols[idx]
    }

    fn generate_long(&mut self) -> i64 {
        self.rng.random_range(0..1_000_000_i64)
    }

    fn generate_double(&mut self) -> f64 {
        self.rng.random_range(0.0..100.0)
    }

    fn generate_timestamp(&mut self) -> i64 {
        // Generate random timestamps for non-designated timestamp columns
        // Random timestamp within a reasonable range around the base timestamp
        let base_nanos = self.base_timestamp.timestamp_nanos_opt().unwrap_or(0);
        let random_offset = self
            .rng
            .random_range(-86_400_000_000_000..86_400_000_000_000); // ±1 day in nanoseconds
        base_nanos + random_offset
    }
}

/// Individual sender thread that blasts data to QuestDB
struct TableSender {
    sender_id: u16,
    table_name: String,
    send_settings: SendSettings,
    ilp_connection: String,
    rows_to_send: u64,
    global_sent_counter: Arc<AtomicU64>,
    // Pre-sorted and pre-validated columns for efficient ILP serialization
    symbol_columns: Vec<String>,
    field_columns: Vec<(String, ColType)>,
}

impl TableSender {
    fn run(self) -> Result<()> {
        info!(
            "Sender {} starting for table '{}'",
            self.sender_id, self.table_name
        );

        let mut rows_sent = 0u64;
        let mut batches_sent = 0u16;
        let mut current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        // Add small random offset to avoid all senders starting at exact same timestamp
        let mut rng = rand::rng();
        current_timestamp += rng.random_range(0..1_000_000_000); // 0-1 second offset

        let mut data_gen = DataGenerator::new(DateTime::from_timestamp_nanos(current_timestamp));
        let mut client: Option<(QuestDbSender, Buffer)> = None;

        while rows_sent < self.rows_to_send {
            // Connect if needed
            if client.is_none() {
                client = Some(self.connect_ilp()?);
                debug!("Sender {} connected to ILP", self.sender_id);
            }

            // Generate random batch size
            let batch_size =
                rng.random_range(self.send_settings.batch_size.0..=self.send_settings.batch_size.1);
            let rows_remaining = self.rows_to_send - rows_sent;
            let actual_batch_size = std::cmp::min(batch_size as u64, rows_remaining) as u32;

            // Send batch
            let (sender, buffer) = client.as_mut().unwrap();
            self.send_batch(
                sender,
                buffer,
                &mut data_gen,
                &mut current_timestamp,
                actual_batch_size,
            )?;

            rows_sent += actual_batch_size as u64;
            batches_sent += 1;

            // Update global counter
            self.global_sent_counter
                .fetch_add(actual_batch_size as u64, Ordering::Relaxed);

            debug!(
                "Sender {} sent batch {}, {} rows total",
                self.sender_id, batches_sent, rows_sent
            );

            // Check if we need to disconnect
            if batches_sent >= self.send_settings.batches_connection_keepalive {
                if let Some((mut sender, mut buffer)) = client.take() {
                    if let Err(e) = sender.flush(&mut buffer) {
                        warn!(
                            "Sender {} failed to flush before disconnect: {}",
                            self.sender_id, e
                        );
                    }
                    debug!(
                        "Sender {} disconnected after {} batches",
                        self.sender_id, batches_sent
                    );
                }
                batches_sent = 0;
            }

            // Pause if not done
            if rows_sent < self.rows_to_send {
                let pause_duration = Duration::from_nanos(rng.random_range(
                    self.send_settings.batch_pause.0.as_nanos()
                        ..=self.send_settings.batch_pause.1.as_nanos(),
                ) as u64);
                debug!("Sender {} pausing for {:?}", self.sender_id, pause_duration);
                thread::sleep(pause_duration);
            }
        }

        // Final flush
        if let Some((mut sender, mut buffer)) = client {
            sender
                .flush(&mut buffer)
                .context("Failed to flush final batch")?;
        }

        info!(
            "Sender {} completed, sent {} rows",
            self.sender_id, rows_sent
        );
        Ok(())
    }

    fn connect_ilp(&self) -> Result<(QuestDbSender, Buffer)> {
        let sender = QuestDbSender::from_conf(&self.ilp_connection)
            .context("Failed to create QuestDB ILP sender")?;
        let buffer = sender.new_buffer();
        Ok((sender, buffer))
    }

    fn send_batch(
        &self,
        sender: &mut QuestDbSender,
        buffer: &mut Buffer,
        data_gen: &mut DataGenerator,
        current_timestamp: &mut i64,
        batch_size: u32,
    ) -> Result<()> {
        for _ in 0..batch_size {
            // Increment timestamp for each row
            *current_timestamp += rand::rng().random_range(1_000_000..10_000_000); // 1-10ms increment

            // Start building a row for the table (unchecked - validated at startup)
            let table_name = TableName::new_unchecked(self.table_name.as_str());
            buffer.table(table_name)?;

            // 1. First, serialize all symbols
            for col_name_str in &self.symbol_columns {
                let col_name = ColumnName::new_unchecked(col_name_str.as_str());
                let value = data_gen.generate_symbol();
                buffer.symbol(col_name, value)?;
            }

            // 2. Then, all remaining non-symbol columns (except designated timestamp)
            for (col_name_str, col_type) in &self.field_columns {
                let col_name = ColumnName::new_unchecked(col_name_str.as_str());
                match col_type {
                    ColType::Long => {
                        let value = data_gen.generate_long();
                        buffer.column_i64(col_name, value)?;
                    }
                    ColType::Double => {
                        let value = data_gen.generate_double();
                        buffer.column_f64(col_name, value)?;
                    }
                    ColType::Timestamp => {
                        // Non-designated timestamp fields
                        let value = data_gen.generate_timestamp();
                        buffer.column_ts(col_name, TimestampNanos::new(value))?;
                    }
                    ColType::Symbol => {
                        // Symbols should not be in field_columns
                        unreachable!("Symbols should be in symbol_columns, not field_columns");
                    }
                }
            }

            // 3. Lastly, set the designated timestamp
            buffer.at(TimestampNanos::new(*current_timestamp))?;
        }

        sender
            .flush(buffer)
            .context("Failed to flush batch to QuestDB")?;

        Ok(())
    }
}

/// Orchestrates the blasting process for a single table
pub fn blast_table(table_name: &str, table_config: &Table, connection: &Connection) -> Result<()> {
    info!("Blasting table '{}'", table_name);

    // Validate table and column names at startup
    validate_names(table_name, table_config)?;

    // Drop and recreate table
    drop_and_create_table(table_name, table_config, &connection.pgsql)?;

    // Calculate rows per sender
    let total_rows = table_config.send.tot_rows;
    let parallel_senders = table_config.send.parallel_senders;
    let base_rows_per_sender = total_rows / parallel_senders as u64;
    let extra_rows = total_rows % parallel_senders as u64;

    info!(
        "Distributing {} total rows across {} senders ({} base + {} extra)",
        total_rows, parallel_senders, base_rows_per_sender, extra_rows
    );

    // Global counter for progress tracking
    let global_sent_counter = Arc::new(AtomicU64::new(0));

    // Spawn sender threads
    let mut handles = Vec::new();
    for sender_id in 0..parallel_senders {
        let rows_for_this_sender =
            base_rows_per_sender + if sender_id < extra_rows as u16 { 1 } else { 0 };

        // Pre-sort columns for efficient ILP serialization
        let mut symbol_columns = Vec::new();
        let mut field_columns = Vec::new();

        for (col_name, col_type) in &table_config.schema {
            if col_name == &table_config.designated_ts {
                // Designated timestamp is handled separately
                continue;
            }

            match col_type {
                ColType::Symbol => symbol_columns.push(col_name.clone()),
                ColType::Long | ColType::Double | ColType::Timestamp => {
                    field_columns.push((col_name.clone(), col_type.clone()));
                }
            }
        }

        let sender = TableSender {
            sender_id,
            table_name: table_name.to_string(),
            send_settings: table_config.send.clone(),
            ilp_connection: connection.ilp.clone(),
            rows_to_send: rows_for_this_sender,
            global_sent_counter: Arc::clone(&global_sent_counter),
            symbol_columns,
            field_columns,
        };

        info!(
            "Starting sender {} with {} rows to send",
            sender_id, rows_for_this_sender
        );

        let handle = thread::spawn(move || {
            if let Err(e) = sender.run() {
                error!("Sender {} failed: {}", sender_id, e);
                return Err(e);
            }
            Ok(())
        });

        handles.push(handle);
    }

    // Wait for all senders to complete
    let mut errors = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => errors.push(e),
            Err(_) => errors.push(anyhow::anyhow!("Thread panicked")),
        }
    }

    if !errors.is_empty() {
        return Err(anyhow::anyhow!("Some senders failed: {:?}", errors));
    }

    let final_count = global_sent_counter.load(Ordering::Relaxed);
    info!(
        "Completed blast for table '{}', sent {} rows",
        table_name, final_count
    );

    Ok(())
}

/// Validates all table and column names at startup to ensure they're valid for QuestDB ILP
fn validate_names(table_name: &str, table_config: &Table) -> Result<()> {
    // Validate table name
    TableName::new(table_name).with_context(|| format!("Invalid table name: '{}'", table_name))?;

    // Validate all column names in schema
    for (col_name, _) in &table_config.schema {
        ColumnName::new(col_name.as_str())
            .with_context(|| format!("Invalid column name: '{}'", col_name))?;
    }

    // Validate designated timestamp column name
    ColumnName::new(table_config.designated_ts.as_str()).with_context(|| {
        format!(
            "Invalid designated timestamp column name: '{}'",
            table_config.designated_ts
        )
    })?;

    info!("All table and column names validated successfully");
    Ok(())
}

/// Drops and recreates the table using the schema configuration
fn drop_and_create_table(
    table_name: &str,
    table_config: &Table,
    pgsql_connection: &str,
) -> Result<()> {
    info!("Dropping and recreating table '{}'", table_name);

    let mut client =
        Client::connect(pgsql_connection, NoTls).context("Failed to connect to PostgreSQL")?;

    // Drop table if exists
    let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
    client
        .execute(&drop_sql, &[])
        .with_context(|| format!("Failed to drop table '{}'", table_name))?;

    // Create table with schema
    let mut create_sql = format!("CREATE TABLE {} (", table_name);
    let mut column_defs = Vec::new();

    for (col_name, col_type) in &table_config.schema {
        let sql_type = match col_type {
            ColType::Symbol => "SYMBOL",
            ColType::Timestamp => "TIMESTAMP",
            ColType::Long => "LONG",
            ColType::Double => "DOUBLE",
        };
        column_defs.push(format!("{} {}", col_name, sql_type));
    }

    create_sql.push_str(&column_defs.join(", "));
    create_sql.push_str(&format!(
        ") TIMESTAMP({}) PARTITION BY DAY",
        table_config.designated_ts
    ));

    debug!("Creating table with SQL: {}", create_sql);
    client.execute(&create_sql, &[]).with_context(|| {
        format!(
            "Failed to create table '{}' with SQL: {}",
            table_name, create_sql
        )
    })?;

    info!("Table '{}' created successfully", table_name);
    Ok(())
}

// Make SendSettings cloneable for the threads
impl Clone for crate::settings::SendSettings {
    fn clone(&self) -> Self {
        Self {
            batch_pause: self.batch_pause,
            batch_size: self.batch_size,
            parallel_senders: self.parallel_senders,
            tot_rows: self.tot_rows,
            batches_connection_keepalive: self.batches_connection_keepalive,
        }
    }
}
