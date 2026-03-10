use std::{
    io::{self, BufRead},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

use anyhow::{anyhow, Context, Result};
use axelbot::{
    backtest::{run_backtest, run_live, run_paper},
    config::AppConfig,
};
use chrono::{DateTime, NaiveDate, Utc};
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, Parser)]
#[command(name = "axelbot", version, about = "Risk-capped Polymarket market-making experiment runner")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Backtest {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "config.toml")]
        config: PathBuf,
    },
    Paper {
        #[arg(long, default_value = "config.toml")]
        config: PathBuf,
        #[arg(long, default_value_t = 3_000)]
        ticks: usize,
    },
    Live {
        #[arg(long, default_value = "config.toml")]
        config: PathBuf,
        #[arg(long, default_value_t = 3_000)]
        ticks: usize,
        #[arg(long)]
        confirm_live: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Backtest { from, to, config } => {
            let cfg = AppConfig::from_path(&config)?;
            let from = parse_ts(&from)?;
            let to = parse_ts(&to)?;
            let report = run_backtest(&cfg, from, to).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::Paper { config, ticks } => {
            let cfg = AppConfig::from_path(&config)?;
            let stop = spawn_manual_stop_listener();
            let report = run_paper(&cfg, ticks, stop).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::Live {
            config,
            ticks,
            confirm_live,
        } => {
            if !confirm_live {
                return Err(anyhow!(
                    "live mode requires --confirm-live to avoid accidental launch"
                ));
            }
            let cfg = AppConfig::from_path(&config)?;
            let stop = spawn_manual_stop_listener();
            let report = run_live(&cfg, ticks, stop).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

fn parse_ts(input: &str) -> Result<DateTime<Utc>> {
    if let Ok(ts) = DateTime::parse_from_rfc3339(input) {
        return Ok(ts.with_timezone(&Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .context("failed to build date at midnight")?;
        return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
    }
    Err(anyhow!(
        "invalid timestamp format: {input}. Use RFC3339 or YYYY-MM-DD."
    ))
}

fn spawn_manual_stop_listener() -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);
    thread::spawn(move || {
        let stdin = io::stdin();
        let mut locked = stdin.lock();
        let mut line = String::new();
        loop {
            line.clear();
            if locked.read_line(&mut line).is_err() {
                break;
            }
            if line.trim().eq_ignore_ascii_case("STOP") {
                stop_clone.store(true, Ordering::Relaxed);
                break;
            }
        }
    });
    stop
}
