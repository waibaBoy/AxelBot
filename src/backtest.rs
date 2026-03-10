use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::{
    config::AppConfig,
    execution::{ExecutionEngine, JsonlLogger, MetricsSnapshot, SimulatedExchangeClient},
    market_data::{MarketDataCache, MarketDataSource, SimulatedMarketDataSource},
    risk::RiskEngine,
    strategy::MarketMakingStrategy,
};

#[derive(Debug, Clone, Serialize)]
pub struct BacktestReport {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub ticks_executed: usize,
    pub total_fills: usize,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub expectancy_after_fees: f64,
    pub max_drawdown_pct: f64,
    pub positive_segments: usize,
    pub passed_gate: bool,
    pub halted: bool,
    pub halt_reason: Option<String>,
    pub log_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionReport {
    pub mode: String,
    pub ticks_executed: usize,
    pub halted: bool,
    pub halt_reason: Option<String>,
    pub metrics: MetricsSnapshot,
    pub log_path: String,
}

pub async fn run_backtest(config: &AppConfig, from: DateTime<Utc>, to: DateTime<Utc>) -> Result<BacktestReport> {
    if to <= from {
        return Err(anyhow!("--to must be later than --from"));
    }
    let max_ticks = (to - from).num_seconds().clamp(60, 40_000) as usize;
    let mode = "backtest";
    let mut logger = JsonlLogger::new(&config.logging.output_dir, mode)?;
    logger.log_event(
        "session_start",
        &serde_json::json!({
            "mode": mode,
            "from": from,
            "to": to,
            "markets": config.markets.symbols.len(),
        }),
    )?;

    let report = run_core_loop(config, max_ticks, mode, Arc::new(AtomicBool::new(false)), &mut logger).await?;
    let expectancy = if report.metrics.total_fills == 0 {
        0.0
    } else {
        report.metrics.realized_pnl / report.metrics.total_fills as f64
    };
    let positive_segments = report
        .segment_realized_pnl
        .iter()
        .filter(|p| **p > 0.0)
        .count();

    let passed_gate = expectancy > 0.0
        && report.max_drawdown_pct <= config.risk.global_drawdown_stop_pct
        && positive_segments >= 2
        && !report.halted;

    let result = BacktestReport {
        from,
        to,
        ticks_executed: report.ticks_executed,
        total_fills: report.metrics.total_fills,
        realized_pnl: report.metrics.realized_pnl,
        unrealized_pnl: report.metrics.unrealized_pnl,
        expectancy_after_fees: expectancy,
        max_drawdown_pct: report.max_drawdown_pct,
        positive_segments,
        passed_gate,
        halted: report.halted,
        halt_reason: report.halt_reason,
        log_path: logger.path().to_string_lossy().to_string(),
    };
    logger.log_event("backtest_report", &result)?;
    Ok(result)
}

pub async fn run_paper(
    config: &AppConfig,
    ticks: usize,
    manual_stop: Arc<AtomicBool>,
) -> Result<SessionReport> {
    let mode = "paper";
    let mut logger = JsonlLogger::new(&config.logging.output_dir, mode)?;
    let report = run_core_loop(config, ticks, mode, manual_stop, &mut logger).await?;
    let session = SessionReport {
        mode: mode.to_string(),
        ticks_executed: report.ticks_executed,
        halted: report.halted,
        halt_reason: report.halt_reason,
        metrics: report.metrics,
        log_path: logger.path().to_string_lossy().to_string(),
    };
    logger.log_event("paper_report", &session)?;
    Ok(session)
}

pub async fn run_live(
    config: &AppConfig,
    ticks: usize,
    manual_stop: Arc<AtomicBool>,
) -> Result<SessionReport> {
    let mode = "live";
    let mut logger = JsonlLogger::new(&config.logging.output_dir, mode)?;
    if config.exchange.api_key.is_none()
        || config.exchange.api_secret.is_none()
        || config.exchange.wallet_private_key.is_none()
    {
        logger.log_event(
            "live_credentials_warning",
            &serde_json::json!({
                "message": "missing API or wallet secrets; running simulated adapter only",
            }),
        )?;
    }
    let report = run_core_loop(config, ticks, mode, manual_stop, &mut logger).await?;
    let session = SessionReport {
        mode: mode.to_string(),
        ticks_executed: report.ticks_executed,
        halted: report.halted,
        halt_reason: report.halt_reason,
        metrics: report.metrics,
        log_path: logger.path().to_string_lossy().to_string(),
    };
    logger.log_event("live_report", &session)?;
    Ok(session)
}

struct CoreLoopReport {
    ticks_executed: usize,
    metrics: MetricsSnapshot,
    max_drawdown_pct: f64,
    segment_realized_pnl: Vec<f64>,
    halted: bool,
    halt_reason: Option<String>,
}

async fn run_core_loop(
    config: &AppConfig,
    ticks: usize,
    mode: &str,
    manual_stop: Arc<AtomicBool>,
    logger: &mut JsonlLogger,
) -> Result<CoreLoopReport> {
    let mut source = SimulatedMarketDataSource::new(config.markets.symbols.clone());
    let mut cache = MarketDataCache::new(&config.markets.symbols);
    let strategy = MarketMakingStrategy::new(config.strategy.clone());
    let risk = RiskEngine::new(config.risk.clone(), config.bankroll.starting_cash);
    let client = SimulatedExchangeClient::new(config.execution.fill_fee_bps);
    let mut engine = ExecutionEngine::new(
        client,
        config.execution.clone(),
        risk,
        config.bankroll.starting_cash,
    );
    let mut next_quote_due: HashMap<String, DateTime<Utc>> = HashMap::new();
    let mut last_universe_refresh = Utc::now();
    let mut peak_equity = config.bankroll.starting_cash;
    let mut max_drawdown_pct = 0.0_f64;
    let segment_size = (ticks.max(3) + 2) / 3;
    let mut segment_realized_pnl = Vec::new();
    let mut segment_start_realized = 0.0_f64;

    logger.log_event(
        "session_bootstrap",
        &serde_json::json!({
            "mode": mode,
            "markets": &config.markets.symbols,
            "risk_limits": {
                "global_drawdown_stop_pct": config.risk.global_drawdown_stop_pct,
                "max_per_market_exposure": config.risk.max_per_market_exposure,
                "max_open_orders": config.risk.max_open_orders,
            },
            "strategy": {
                "target_spread_bps": config.strategy.target_spread_bps,
                "inventory_skew_bps": config.strategy.inventory_skew_bps,
                "quote_sizes": &config.strategy.quote_sizes,
            }
        }),
    )?;

    let mut ticks_executed = 0;
    for _ in 0..ticks {
        if manual_stop.load(Ordering::Relaxed) {
            engine
                .risk_mut()
                .emergency_stop("manual emergency stop command received");
        }
        if engine.risk().is_halted() {
            break;
        }

        let event = source.next_event().await?;
        if let Some(snapshot) = cache.apply_event(event) {
            if (snapshot.timestamp - last_universe_refresh).num_seconds()
                >= config.markets.refresh_secs as i64
            {
                logger.log_event(
                    "market_universe_refresh",
                    &serde_json::json!({
                        "selection_mode": config.markets.selection_mode,
                        "tracked_markets": config.markets.symbols.len(),
                    }),
                )?;
                last_universe_refresh = snapshot.timestamp;
            }
            let inventory = engine.portfolio().inventory_for(&snapshot.market);
            let due = next_quote_due
                .entry(snapshot.market.clone())
                .or_insert(snapshot.timestamp);
            if snapshot.timestamp >= *due {
                let quote = strategy.generate_quote(&snapshot, inventory);
                engine
                    .process_quote(quote, snapshot.timestamp, logger)
                    .await?;
                *due = snapshot.timestamp + Duration::milliseconds(strategy.refresh_interval_ms());
            }
        }
        engine
            .on_tick(&cache.mids(), Utc::now(), cache.last_global_update, logger)
            .await?;

        let equity = engine.portfolio().total_equity();
        if equity > peak_equity {
            peak_equity = equity;
        }
        if peak_equity > 0.0 {
            let dd = ((peak_equity - equity) / peak_equity).max(0.0);
            if dd > max_drawdown_pct {
                max_drawdown_pct = dd;
            }
        }
        ticks_executed += 1;
        if ticks_executed % segment_size == 0 {
            let realized = engine.portfolio().realized_pnl;
            segment_realized_pnl.push(realized - segment_start_realized);
            segment_start_realized = realized;
        }
    }
    if segment_realized_pnl.len() < 3 {
        let realized = engine.portfolio().realized_pnl;
        segment_realized_pnl.push(realized - segment_start_realized);
    }
    let metrics = engine.metrics_snapshot();
    Ok(CoreLoopReport {
        ticks_executed,
        metrics,
        max_drawdown_pct,
        segment_realized_pnl,
        halted: engine.risk().is_halted(),
        halt_reason: engine.risk().halt_reason().map(|s| s.to_string()),
    })
}
