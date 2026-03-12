use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration as StdDuration,
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::Serialize;
use tokio::time::timeout;
use tracing::warn;

use crate::{
    config::AppConfig,
    execution::{
        ExecutionEngine, HttpWsExchangeClient, JsonlLogger, MetricsSnapshot,
        SimulatedExchangeClient,
    },
    market_data::{MarketDataCache, MarketDataSource, SimulatedMarketDataSource},
    polymarket::{
        client::{PolymarketClient, PolymarketPublicClient},
        replay::HistoricalReplaySource,
        signer::PolymarketSigner,
        ws::PolymarketWsSource,
    },
    risk::RiskEngine,
    strategy::MarketMakingStrategy,
    types::MarketPair,
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

pub async fn run_backtest(
    config: &AppConfig,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<BacktestReport> {
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

    let report = run_core_loop(
        config,
        max_ticks,
        mode,
        Arc::new(AtomicBool::new(false)),
        &mut logger,
    )
    .await?;
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

pub async fn run_backtest_history(
    config: &AppConfig,
    market: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    fidelity: &str,
    synthetic_spread_bps: f64,
) -> Result<BacktestReport> {
    if to <= from {
        return Err(anyhow!("--to must be later than --from"));
    }
    let mode = "backtest-history";
    let mut logger = JsonlLogger::new(&config.logging.output_dir, mode)?;
    let public = PolymarketPublicClient::new(&config.exchange.rest_url);
    let server_time = public.get_server_time().await?;
    let fee_rate = match public.get_fee_rate_for_token(market).await {
        Ok(v) => v,
        Err(_) => match public.get_fee_rate().await {
            Ok(v) => v,
            Err(_) => crate::polymarket::types::ClobFeeRate { fee_rate_bps: 0.0 },
        },
    };
    let tick_size = match public.get_tick_size_for_token(market).await {
        Ok(v) => v,
        Err(_) => match public.get_tick_size().await {
            Ok(v) => v,
            Err(_) => crate::polymarket::types::ClobTickSize { tick_size: 0.01 },
        },
    };
    let points = public
        .get_prices_history(market, from, to, fidelity)
        .await?;

    logger.log_event(
        "history_fetch",
        &serde_json::json!({
            "market": market,
            "fidelity": fidelity,
            "points": points.len(),
            "server_time_ms": server_time.epoch_ms,
            "fee_rate_bps": fee_rate.fee_rate_bps,
            "tick_size": tick_size.tick_size,
        }),
    )?;

    let source = HistoricalReplaySource::from_price_history(
        market.to_string(),
        points,
        synthetic_spread_bps,
    );
    let max_ticks = source.len();
    if max_ticks == 0 {
        return Err(anyhow!("no replay points available"));
    }

    let report = run_core_loop_with_source(
        config,
        max_ticks,
        mode,
        Arc::new(AtomicBool::new(false)),
        &mut logger,
        source,
        config.bankroll.starting_cash,
        vec![market.to_string()],
    )
    .await?;

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
    logger.log_event("backtest_history_report", &result)?;
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

/// Paper mode using live Polymarket market data over WebSocket, but simulated
/// execution/fills. This avoids placing real orders while validating the full
/// ingestion/strategy/risk loop on real-time data.
pub async fn run_paper_live_data(
    config: &AppConfig,
    ticks: usize,
    manual_stop: Arc<AtomicBool>,
) -> Result<SessionReport> {
    let mode = "paper-live";
    let mut logger = JsonlLogger::new(&config.logging.output_dir, mode)?;

    let public = PolymarketPublicClient::new(&config.exchange.rest_url);
    let desired_markets = config.markets.min_markets.max(2);

    let mut token_to_market: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut all_token_ids: Vec<String> = Vec::new();
    let mut market_symbols: Vec<String> = Vec::new();

    // First choice: use recently traded tokens, which are much more likely to
    // emit WS events immediately.
    let recent_tokens = public
        .get_recent_trade_asset_ids(desired_markets * 3)
        .await
        .unwrap_or_default();
    for token in recent_tokens {
        if token.is_empty() || market_symbols.contains(&token) {
            continue;
        }
        token_to_market.insert(token.clone(), token.clone());
        all_token_ids.push(token.clone());
        market_symbols.push(token);
        if market_symbols.len() >= desired_markets {
            break;
        }
    }

    // Fallback: derive tokens from active market metadata.
    if market_symbols.len() < 2 {
        let target_active = desired_markets * 20;
        let clob_markets = public.get_all_active_markets(target_active).await?;
        for m in &clob_markets {
            for tok in &m.tokens {
                let token = tok.token_id.trim().to_string();
                if token.is_empty() || market_symbols.contains(&token) {
                    continue;
                }
                token_to_market.insert(token.clone(), token.clone());
                all_token_ids.push(token.clone());
                market_symbols.push(token);
                if market_symbols.len() >= desired_markets {
                    break;
                }
            }
            if market_symbols.len() >= desired_markets {
                break;
            }
        }
    }

    if market_symbols.len() < 2 {
        return Err(anyhow!(
            "insufficient tradable markets discovered (found {}, need at least 2)",
            market_symbols.len(),
        ));
    }

    if market_symbols.len() < desired_markets {
        logger.log_event(
            "paper_live_markets_warning",
            &serde_json::json!({
                "requested_min_markets": desired_markets,
                "selected_markets": market_symbols.len(),
                "message": "running with fewer markets than configured",
            }),
        )?;
    }

    logger.log_event(
        "paper_live_markets_selected",
        &serde_json::json!({
            "count": market_symbols.len(),
            "sample": &market_symbols[..market_symbols.len().min(5)],
        }),
    )?;

    let mut ws_source = PolymarketWsSource::new(
        &config.exchange.ws_url,
        all_token_ids.clone(),
        token_to_market,
    );
    let ws_enabled = match ws_source.connect().await {
        Ok(_) => true,
        Err(err) => {
            logger.log_event(
                "paper_live_ws_connect_failed",
                &serde_json::json!({
                    "error": err.to_string(),
                    "fallback": "rest_polling",
                }),
            )?;
            false
        }
    };
    let rest_source = RestPollingSource::new(&config.exchange.rest_url, all_token_ids);
    let source = HybridLiveSource::new(ws_source, rest_source, ws_enabled);

    let report = run_core_loop_with_source(
        config,
        ticks,
        mode,
        manual_stop,
        &mut logger,
        source,
        config.bankroll.starting_cash,
        market_symbols,
    )
    .await?;

    let session = SessionReport {
        mode: mode.to_string(),
        ticks_executed: report.ticks_executed,
        halted: report.halted,
        halt_reason: report.halt_reason,
        metrics: report.metrics,
        log_path: logger.path().to_string_lossy().to_string(),
    };
    logger.log_event("paper_live_report", &session)?;
    Ok(session)
}

pub async fn run_live(
    config: &AppConfig,
    ticks: usize,
    manual_stop: Arc<AtomicBool>,
) -> Result<SessionReport> {
    let mode = "live";
    let mut logger = JsonlLogger::new(&config.logging.output_dir, mode)?;

    let has_creds = config.exchange.api_key.is_some()
        && config.exchange.api_secret.is_some()
        && config.exchange.wallet_private_key.is_some();

    if !has_creds {
        logger.log_event(
            "live_credentials_warning",
            &serde_json::json!({
                "message": "missing API or wallet secrets; falling back to simulated adapter",
            }),
        )?;
        // Fall back to simulated loop
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
        return Ok(session);
    }

    // ---- Live mode with real Polymarket connection ----
    let api_key = config.exchange.api_key.clone().unwrap();
    let api_secret = config.exchange.api_secret.clone().unwrap();
    let private_key = config.exchange.wallet_private_key.clone().unwrap();
    let wallet_address = config.exchange.wallet_address.clone();

    // Passphrase from env or default empty
    let passphrase = std::env::var("AXELBOT_API_PASSPHRASE").unwrap_or_default();

    let poly_cfg = &config.polymarket;
    let signer = PolymarketSigner::new(
        &private_key,
        poly_cfg.chain_id,
        &poly_cfg.ctf_exchange_address,
        &poly_cfg.neg_risk_ctf_exchange_address,
    )?;

    let pm_client = PolymarketClient::new(
        &config.exchange.rest_url,
        &api_key,
        &api_secret,
        &passphrase,
        signer,
        &wallet_address,
    );

    // Discover markets
    logger.log_event(
        "market_discovery_start",
        &serde_json::json!({ "source": "polymarket_clob_api" }),
    )?;
    let clob_markets = pm_client.get_all_active_markets().await?;
    logger.log_event(
        "market_discovery_done",
        &serde_json::json!({ "total_active_markets": clob_markets.len() }),
    )?;

    // Build market pairs and token mappings
    let mut market_pairs: Vec<MarketPair> = Vec::new();
    let mut token_to_market: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut neg_risk_map: std::collections::HashMap<String, bool> =
        std::collections::HashMap::new();
    let mut all_token_ids: Vec<String> = Vec::new();
    let mut market_symbols: Vec<String> = Vec::new();

    for m in &clob_markets {
        let yes_token = m
            .tokens
            .iter()
            .find(|t| t.outcome.eq_ignore_ascii_case("Yes"));
        let no_token = m
            .tokens
            .iter()
            .find(|t| t.outcome.eq_ignore_ascii_case("No"));
        if let (Some(yes), Some(no)) = (yes_token, no_token) {
            let neg_risk = m.neg_risk.unwrap_or(false);
            let pair = MarketPair {
                condition_id: m.condition_id.clone(),
                yes_token_id: yes.token_id.clone(),
                no_token_id: no.token_id.clone(),
                neg_risk,
            };
            // Use the YES token_id as the market identifier for trading
            token_to_market.insert(yes.token_id.clone(), yes.token_id.clone());
            token_to_market.insert(no.token_id.clone(), yes.token_id.clone());
            neg_risk_map.insert(yes.token_id.clone(), neg_risk);
            all_token_ids.push(yes.token_id.clone());
            all_token_ids.push(no.token_id.clone());
            market_symbols.push(yes.token_id.clone());
            market_pairs.push(pair);
        }
    }

    // Limit to configured min_markets if we have too many
    if market_symbols.len() > config.markets.min_markets * 2 {
        market_symbols.truncate(config.markets.min_markets);
        all_token_ids.truncate(config.markets.min_markets * 2);
    }

    logger.log_event(
        "live_markets_selected",
        &serde_json::json!({
            "count": market_symbols.len(),
            "sample": &market_symbols[..market_symbols.len().min(5)],
        }),
    )?;

    // Set up WebSocket data source
    let mut ws_source = PolymarketWsSource::new(
        &config.exchange.ws_url,
        all_token_ids.clone(),
        token_to_market,
    );
    let ws_enabled = match ws_source.connect().await {
        Ok(_) => true,
        Err(err) => {
            logger.log_event(
                "live_ws_connect_failed",
                &serde_json::json!({
                    "error": err.to_string(),
                    "fallback": "rest_polling",
                }),
            )?;
            false
        }
    };
    let rest_source = RestPollingSource::new(&config.exchange.rest_url, all_token_ids);
    let mut live_source = HybridLiveSource::new(ws_source, rest_source, ws_enabled);

    // Set up live exchange client
    let live_client = HttpWsExchangeClient::new(&config.exchange.rest_url, &config.exchange.ws_url)
        .with_client(pm_client, neg_risk_map);

    let mut cache = MarketDataCache::new(&market_symbols);
    let mut strategy = MarketMakingStrategy::new(config.strategy.clone());
    let risk = RiskEngine::new(config.risk.clone(), config.bankroll.live_tranche_cash);
    let mut engine = ExecutionEngine::new(
        live_client,
        config.execution.clone(),
        risk,
        config.bankroll.live_tranche_cash,
        false,
    );

    let mut next_quote_due: HashMap<String, DateTime<Utc>> = HashMap::new();
    let mut last_universe_refresh = Utc::now();
    let mut peak_equity = config.bankroll.live_tranche_cash;
    let mut max_drawdown_pct = 0.0_f64;
    let segment_size = (ticks.max(3) + 2) / 3;
    let mut segment_realized_pnl = Vec::new();
    let mut segment_start_realized = 0.0_f64;

    logger.log_event(
        "live_session_bootstrap",
        &serde_json::json!({
            "markets": market_symbols.len(),
            "bankroll": config.bankroll.live_tranche_cash,
            "risk_limits": {
                "global_drawdown_stop_pct": config.risk.global_drawdown_stop_pct,
                "max_per_market_exposure": config.risk.max_per_market_exposure,
            },
        }),
    )?;

    let mut ticks_executed = 0;
    let data_timeout_ms = config.risk.heartbeat_timeout_ms.max(15_000) as u64;
    for _ in 0..ticks {
        if manual_stop.load(Ordering::Relaxed) {
            engine
                .risk_mut()
                .emergency_stop("manual emergency stop command received");
        }
        if engine.risk().is_halted() {
            break;
        }

        let event = match timeout(
            StdDuration::from_millis(data_timeout_ms),
            live_source.next_event(),
        )
        .await
        {
            Ok(Ok(e)) => e,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                engine.risk_mut().emergency_stop(format!(
                    "market data timeout: no event within {}ms",
                    data_timeout_ms
                ));
                logger.log_event(
                    "market_data_timeout",
                    &serde_json::json!({ "timeout_ms": data_timeout_ms, "mode": mode }),
                )?;
                break;
            }
        };
        if let Some(snapshot) = cache.apply_event(event) {
            if (snapshot.timestamp - last_universe_refresh).num_seconds()
                >= config.markets.refresh_secs as i64
            {
                last_universe_refresh = snapshot.timestamp;
            }
            let inventory = engine.portfolio().inventory_for(&snapshot.market);
            let due = next_quote_due
                .entry(snapshot.market.clone())
                .or_insert(snapshot.timestamp);
            if snapshot.timestamp >= *due {
                let quote = strategy.generate_quote(&snapshot, inventory);
                engine
                    .process_quote(quote, snapshot.timestamp, &mut logger)
                    .await?;
                *due = snapshot.timestamp + Duration::milliseconds(strategy.refresh_interval_ms());
            }
        }
        engine
            .on_tick(
                &cache.mids(),
                Utc::now(),
                cache.last_global_update,
                &mut logger,
            )
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

    let metrics = engine.metrics_snapshot();
    let session = SessionReport {
        mode: mode.to_string(),
        ticks_executed,
        halted: engine.risk().is_halted(),
        halt_reason: engine.risk().halt_reason().map(|s| s.to_string()),
        metrics,
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

struct RestPollingSource {
    client: PolymarketPublicClient,
    token_ids: Vec<String>,
    cursor: usize,
}

impl RestPollingSource {
    fn new(rest_url: &str, token_ids: Vec<String>) -> Self {
        Self {
            client: PolymarketPublicClient::new(rest_url),
            token_ids,
            cursor: 0,
        }
    }
}

#[async_trait]
impl MarketDataSource for RestPollingSource {
    async fn next_event(&mut self) -> Result<crate::market_data::MarketEvent> {
        if self.token_ids.is_empty() {
            return Err(anyhow!("rest polling source has no tokens"));
        }
        for _ in 0..self.token_ids.len() {
            let token = self.token_ids[self.cursor % self.token_ids.len()].clone();
            self.cursor = (self.cursor + 1) % self.token_ids.len();
            let book = match self.client.get_orderbook(&token).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            let best_bid = book.bids.first().map(|l| l.price_f64()).unwrap_or(0.0);
            let best_ask = book.asks.first().map(|l| l.price_f64()).unwrap_or(0.0);
            if !(best_bid > 0.0 && best_ask > 0.0 && best_ask > best_bid) {
                continue;
            }
            let bid_size = book.bids.first().map(|l| l.size_f64()).unwrap_or(0.0);
            let ask_size = book.asks.first().map(|l| l.size_f64()).unwrap_or(0.0);
            let ts = book
                .timestamp
                .as_deref()
                .and_then(parse_book_ts)
                .unwrap_or_else(Utc::now);
            return Ok(crate::market_data::MarketEvent::BookUpdate {
                market: token,
                bid: best_bid,
                bid_size,
                ask: best_ask,
                ask_size,
                timestamp: ts,
            });
        }
        Err(anyhow!(
            "no valid orderbook snapshot available from REST polling"
        ))
    }
}

struct HybridLiveSource {
    ws: PolymarketWsSource,
    rest: RestPollingSource,
    ws_enabled: bool,
}

impl HybridLiveSource {
    fn new(ws: PolymarketWsSource, rest: RestPollingSource, ws_enabled: bool) -> Self {
        Self {
            ws,
            rest,
            ws_enabled,
        }
    }
}

#[async_trait]
impl MarketDataSource for HybridLiveSource {
    async fn next_event(&mut self) -> Result<crate::market_data::MarketEvent> {
        if self.ws_enabled {
            match timeout(StdDuration::from_millis(60_000), self.ws.next_event()).await {
                Ok(Ok(event)) => return Ok(event),
                Ok(Err(err)) => {
                    warn!(
                        "ws market data stream failed, falling back to REST polling: {}",
                        err
                    );
                    self.ws_enabled = false;
                }
                Err(_) => {
                    warn!("ws market data stream timed out (60s), falling back to REST polling");
                    self.ws_enabled = false;
                }
            }
        }
        self.rest.next_event().await
    }
}

fn parse_book_ts(raw: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(n) = raw.parse::<i64>() {
        if n > 1_000_000_000_000 {
            return Utc.timestamp_millis_opt(n).single();
        }
        return Utc.timestamp_opt(n, 0).single();
    }
    None
}

async fn run_core_loop(
    config: &AppConfig,
    ticks: usize,
    mode: &str,
    manual_stop: Arc<AtomicBool>,
    logger: &mut JsonlLogger,
) -> Result<CoreLoopReport> {
    let source = SimulatedMarketDataSource::new(config.markets.symbols.clone());
    run_core_loop_with_source(
        config,
        ticks,
        mode,
        manual_stop,
        logger,
        source,
        config.bankroll.starting_cash,
        config.markets.symbols.clone(),
    )
    .await
}

async fn run_core_loop_with_source<S: MarketDataSource>(
    config: &AppConfig,
    ticks: usize,
    mode: &str,
    manual_stop: Arc<AtomicBool>,
    logger: &mut JsonlLogger,
    mut source: S,
    starting_cash: f64,
    markets: Vec<String>,
) -> Result<CoreLoopReport> {
    let mut cache = MarketDataCache::new(&markets);
    let mut strategy = MarketMakingStrategy::new(config.strategy.clone());
    let risk = RiskEngine::new(config.risk.clone(), starting_cash);
    let client = SimulatedExchangeClient::new(config.execution.fill_fee_bps);
    let mut engine =
        ExecutionEngine::new(client, config.execution.clone(), risk, starting_cash, true);
    let mut next_quote_due: HashMap<String, DateTime<Utc>> = HashMap::new();
    let mut last_universe_refresh = Utc::now();
    let mut peak_equity = starting_cash;
    let mut max_drawdown_pct = 0.0_f64;
    let segment_size = (ticks.max(3) + 2) / 3;
    let mut segment_realized_pnl = Vec::new();
    let mut segment_start_realized = 0.0_f64;

    logger.log_event(
        "session_bootstrap",
        &serde_json::json!({
            "mode": mode,
            "markets": &markets,
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
    let data_timeout_ms = config.risk.heartbeat_timeout_ms.max(15_000) as u64;
    for _ in 0..ticks {
        if manual_stop.load(Ordering::Relaxed) {
            engine
                .risk_mut()
                .emergency_stop("manual emergency stop command received");
        }
        if engine.risk().is_halted() {
            break;
        }

        let event = match timeout(
            StdDuration::from_millis(data_timeout_ms),
            source.next_event(),
        )
        .await
        {
            Ok(Ok(e)) => e,
            Ok(Err(err)) => {
                logger.log_event(
                    "data_source_exhausted",
                    &serde_json::json!({ "error": err.to_string() }),
                )?;
                break;
            }
            Err(_) => {
                engine.risk_mut().emergency_stop(format!(
                    "market data timeout: no event within {}ms",
                    data_timeout_ms
                ));
                logger.log_event(
                    "market_data_timeout",
                    &serde_json::json!({ "timeout_ms": data_timeout_ms, "mode": mode }),
                )?;
                break;
            }
        };

        if let Some(snapshot) = cache.apply_event(event) {
            if (snapshot.timestamp - last_universe_refresh).num_seconds()
                >= config.markets.refresh_secs as i64
            {
                logger.log_event(
                    "market_universe_refresh",
                    &serde_json::json!({
                        "selection_mode": config.markets.selection_mode,
                        "tracked_markets": markets.len(),
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
