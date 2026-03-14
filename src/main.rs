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
    backtest::{run_backtest, run_backtest_history, run_live, run_paper, run_paper_live_data},
    config::AppConfig,
    dashboard::run_dashboard,
    polymarket::auth::{get_api_credentials, ApiCredsMode},
    polymarket::client::PolymarketPublicClient,
};
use chrono::{DateTime, NaiveDate, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, Parser)]
#[command(
    name = "axelbot",
    version,
    about = "Risk-capped Polymarket market-making experiment runner"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    ApiCreds {
        #[arg(long, default_value = "config.toml")]
        config: PathBuf,
        #[arg(long)]
        rest_url: Option<String>,
        #[arg(long, default_value_t = 0)]
        nonce: u64,
        #[arg(long, value_enum, default_value_t = ApiCredsModeArg::CreateOrDerive)]
        mode: ApiCredsModeArg,
    },
    Backtest {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "config.toml")]
        config: PathBuf,
    },
    BacktestHistory {
        #[arg(long)]
        market: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "1m")]
        fidelity: String,
        #[arg(long, default_value_t = 30.0)]
        synthetic_spread_bps: f64,
        #[arg(long, default_value = "config.toml")]
        config: PathBuf,
    },
    ApiCheck {
        #[arg(long, default_value = "config.toml")]
        config: PathBuf,
        #[arg(long)]
        market: Option<String>,
        #[arg(long, default_value = "1m")]
        fidelity: String,
    },
    Dashboard {
        #[arg(long, default_value_t = 8080)]
        port: u16,
        #[arg(long, default_value = "logs")]
        logs_dir: PathBuf,
    },
    Paper {
        #[arg(long, default_value = "config.toml")]
        config: PathBuf,
        #[arg(long, default_value_t = 3_000)]
        ticks: usize,
    },
    PaperLive {
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ApiCredsModeArg {
    Create,
    Derive,
    CreateOrDerive,
}

impl From<ApiCredsModeArg> for ApiCredsMode {
    fn from(value: ApiCredsModeArg) -> Self {
        match value {
            ApiCredsModeArg::Create => ApiCredsMode::Create,
            ApiCredsModeArg::Derive => ApiCredsMode::Derive,
            ApiCredsModeArg::CreateOrDerive => ApiCredsMode::CreateOrDerive,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::ApiCreds {
            config,
            rest_url,
            nonce,
            mode,
        } => {
            let cfg = AppConfig::from_path(&config)?;
            let rest_url = rest_url.unwrap_or(cfg.exchange.rest_url.clone());
            if rest_url.contains("polymarket.example") {
                return Err(anyhow!(
                    "exchange.rest_url is a placeholder. Set it to https://clob.polymarket.com in config.toml or pass --rest-url."
                ));
            }

            let private_key = cfg.exchange.wallet_private_key.clone().ok_or_else(|| {
                anyhow!(
                    "missing wallet private key. Set AXELBOT_WALLET_PRIVATE_KEY in .env or exchange.wallet_private_key in config.toml."
                )
            })?;

            let (address, creds) = get_api_credentials(
                &rest_url,
                &private_key,
                cfg.polymarket.chain_id,
                nonce,
                mode.into(),
                cfg.exchange.proxy_url.as_deref(),
            )
            .await?;

            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "rest_url": rest_url,
                    "signer_address": address,
                    "nonce": nonce,
                    "apiKey": creds.api_key,
                    "secret": creds.secret,
                    "passphrase": creds.passphrase
                }))?
            );
            println!();
            println!("AXELBOT_API_KEY={}", creds.api_key);
            println!("AXELBOT_API_SECRET={}", creds.secret);
            println!("AXELBOT_API_PASSPHRASE={}", creds.passphrase);
        }
        Commands::Backtest { from, to, config } => {
            let cfg = AppConfig::from_path(&config)?;
            let from = parse_ts(&from)?;
            let to = parse_ts(&to)?;
            let report = run_backtest(&cfg, from, to).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::BacktestHistory {
            market,
            from,
            to,
            fidelity,
            synthetic_spread_bps,
            config,
        } => {
            let cfg = AppConfig::from_path(&config)?;
            let from = parse_ts(&from)?;
            let to = parse_ts(&to)?;
            let report =
                run_backtest_history(&cfg, &market, from, to, &fidelity, synthetic_spread_bps)
                    .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::ApiCheck {
            config,
            market,
            fidelity,
        } => {
            let cfg = AppConfig::from_path(&config)?;
            let client = PolymarketPublicClient::new(&cfg.exchange.rest_url, cfg.exchange.proxy_url.as_deref());
            let server_time = client.get_server_time().await?;
            let (fee_rate_bps, fee_error) = if let Some(token) = market.as_deref() {
                match client.get_fee_rate_for_token(token).await {
                    Ok(v) => (Some(v.fee_rate_bps), None),
                    Err(e) => (None, Some(e.to_string())),
                }
            } else {
                match client.get_fee_rate().await {
                    Ok(v) => (Some(v.fee_rate_bps), None),
                    Err(e) => (None, Some(e.to_string())),
                }
            };
            let (tick_size, tick_error) = if let Some(token) = market.as_deref() {
                match client.get_tick_size_for_token(token).await {
                    Ok(v) => (Some(v.tick_size), None),
                    Err(e) => (None, Some(e.to_string())),
                }
            } else {
                match client.get_tick_size().await {
                    Ok(v) => (Some(v.tick_size), None),
                    Err(e) => (None, Some(e.to_string())),
                }
            };

            let history_sample = if let Some(token) = market.as_deref() {
                let end = Utc::now();
                let start = end - chrono::Duration::hours(6);
                match client
                    .get_prices_history(token, start, end, &fidelity)
                    .await
                {
                    Ok(rows) => Some(serde_json::json!({
                        "market": token,
                        "rows": rows.len(),
                        "first_ts": rows.first().map(|r| r.timestamp),
                        "last_ts": rows.last().map(|r| r.timestamp),
                    })),
                    Err(e) => Some(serde_json::json!({
                        "market": token,
                        "error": e.to_string(),
                    })),
                }
            } else {
                None
            };

            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "server_time_ms": server_time.epoch_ms,
                    "fee_rate_bps": fee_rate_bps,
                    "tick_size": tick_size,
                    "fee_error": fee_error,
                    "tick_error": tick_error,
                    "history_sample": history_sample,
                }))?
            );
        }
        Commands::Dashboard { port, logs_dir } => {
            run_dashboard(logs_dir, port).await?;
        }
        Commands::Paper { config, ticks } => {
            let cfg = AppConfig::from_path(&config)?;
            let stop = spawn_manual_stop_listener();
            let report = run_paper(&cfg, ticks, stop).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::PaperLive { config, ticks } => {
            let cfg = AppConfig::from_path(&config)?;
            let stop = spawn_manual_stop_listener();
            let report = run_paper_live_data(&cfg, ticks, stop).await?;
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
