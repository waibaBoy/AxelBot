use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration as StdDuration, Instant},
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
    market_data::{
        MarketDataCache, MarketDataSource, SimulatedMarketDataSource, MAX_VALID_BOOK_SPREAD,
    },
    news::NewsOverlay,
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
    let proxy = config.exchange.proxy_url.as_deref();
    let public = PolymarketPublicClient::new(&config.exchange.rest_url, proxy);
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
        HashMap::from([(market.to_string(), market.to_string())]),
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
    let proxy = config.exchange.proxy_url.as_deref();

    let public = PolymarketPublicClient::new(&config.exchange.rest_url, proxy);
    let desired_markets = config.markets.min_markets.max(2);

    let mut token_to_market: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut all_token_ids: Vec<String> = Vec::new();
    let mut market_symbols: Vec<String> = Vec::new();
    let mut candidate_tokens: Vec<String> = Vec::new();
    let mut token_to_text: HashMap<String, String> = HashMap::new();
    let active_markets = public
        .get_all_active_markets(desired_markets * 25)
        .await
        .unwrap_or_default();
    for m in &active_markets {
        let descriptor = format!(
            "{} {} {}",
            m.question,
            m.description.clone().unwrap_or_default(),
            m.market_slug.clone().unwrap_or_default()
        );
        for tok in &m.tokens {
            if !tok.token_id.trim().is_empty() {
                token_to_text.insert(tok.token_id.clone(), descriptor.clone());
            }
        }
    }

    // First choice: use recently traded tokens, which are much more likely to
    // emit WS events immediately.
    let recent_tokens = public
        .get_recent_trade_asset_ids(desired_markets * 3)
        .await
        .unwrap_or_default();
    for token in recent_tokens {
        if token.is_empty() || candidate_tokens.contains(&token) {
            continue;
        }
        candidate_tokens.push(token);
        if candidate_tokens.len() >= desired_markets * 8 {
            break;
        }
    }

    // Fallback candidate pool: derive additional tokens from active market metadata.
    let clob_markets = if active_markets.is_empty() {
        public.get_all_active_markets(desired_markets * 20).await?
    } else {
        active_markets.clone()
    };
    for m in &clob_markets {
        for tok in &m.tokens {
            let token = tok.token_id.trim().to_string();
            if token.is_empty() || candidate_tokens.contains(&token) {
                continue;
            }
            candidate_tokens.push(token);
            if candidate_tokens.len() >= desired_markets * 25 {
                break;
            }
        }
        if candidate_tokens.len() >= desired_markets * 25 {
            break;
        }
    }

    // Validate candidates up-front so we only subscribe/poll tradable orderbooks.
    let candidate_backup = candidate_tokens.clone();
    let mut scanned = 0usize;
    let mut fetch_failures = 0usize;
    let mut invalid_books = 0usize;
    for token in candidate_tokens {
        scanned += 1;
        let book = match timeout(
            StdDuration::from_millis(config.markets.book_probe_timeout_ms),
            public.get_orderbook(&token),
        )
        .await
        {
            Ok(Ok(v)) => v,
            _ => {
                fetch_failures = fetch_failures.saturating_add(1);
                continue;
            }
        };
        let best_bid = book.bids.first().map(|l| l.price_f64()).unwrap_or(0.0);
        let best_ask = book.asks.first().map(|l| l.price_f64()).unwrap_or(0.0);
        if !(best_bid > 0.0
            && best_ask > 0.0
            && best_ask > best_bid
            && (best_ask - best_bid) <= MAX_VALID_BOOK_SPREAD)
        {
            invalid_books = invalid_books.saturating_add(1);
            continue;
        }
        token_to_market.insert(token.clone(), token.clone());
        all_token_ids.push(token.clone());
        market_symbols.push(token);
        if market_symbols.len() >= desired_markets {
            break;
        }
    }
    logger.log_event(
        "paper_live_candidate_scan",
        &serde_json::json!({
            "candidates": scanned,
            "tradable": market_symbols.len(),
            "fetch_failures": fetch_failures,
            "invalid_books": invalid_books,
            "max_valid_spread": MAX_VALID_BOOK_SPREAD,
        }),
    )?;

    if market_symbols.len() < 2 {
        // Degraded mode: when no tradable markets pass strict validation,
        // fall back to best-effort WS candidates. This covers both flaky
        // REST connectivity (high fetch_failures) and markets with wide
        // spreads that fail book validation (high invalid_books).
        let unusable = fetch_failures.saturating_add(invalid_books);
        if scanned > 0
            && unusable.saturating_mul(5) >= scanned.saturating_mul(4)
            && candidate_backup.len() >= 2
        {
            for token in candidate_backup.into_iter() {
                if market_symbols.contains(&token) {
                    continue;
                }
                token_to_market.insert(token.clone(), token.clone());
                all_token_ids.push(token.clone());
                market_symbols.push(token);
                if market_symbols.len() >= desired_markets.max(2) {
                    break;
                }
            }
            logger.log_event(
                "paper_live_candidate_fallback",
                &serde_json::json!({
                    "reason": "rest_validation_no_tradable",
                    "selected_markets": market_symbols.len(),
                    "fetch_failures": fetch_failures,
                    "invalid_books": invalid_books,
                    "scanned": scanned,
                }),
            )?;
        }
    }

    if market_symbols.len() < 2 {
        if scanned > 0 && fetch_failures >= scanned {
            return Err(anyhow!(
                "insufficient tradable markets discovered after validation (found {}, need at least 2); likely CLOB connectivity issue: all {} orderbook probes failed",
                market_symbols.len(),
                scanned
            ));
        }
        return Err(anyhow!(
            "insufficient tradable markets discovered after validation (found {}, need at least 2)",
            market_symbols.len()
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
        proxy,
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
    let rest_source = RestPollingSource::new(&config.exchange.rest_url, all_token_ids, proxy, config.markets.rest_poll_timeout_ms);
    let source = HybridLiveSource::new(ws_source, rest_source, ws_enabled);
    let market_texts = market_symbols
        .iter()
        .map(|m| {
            (
                m.clone(),
                token_to_text.get(m).cloned().unwrap_or_else(|| m.clone()),
            )
        })
        .collect::<HashMap<_, _>>();

    let report = run_core_loop_with_source(
        config,
        ticks,
        mode,
        manual_stop,
        &mut logger,
        source,
        config.bankroll.starting_cash,
        market_symbols,
        market_texts,
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
    let proxy = config.exchange.proxy_url.as_deref();
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
        proxy,
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
        proxy,
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
    let rest_source = RestPollingSource::new(&config.exchange.rest_url, all_token_ids, proxy, config.markets.rest_poll_timeout_ms);
    let mut live_source = HybridLiveSource::new(ws_source, rest_source, ws_enabled);

    // Set up live exchange client
    let live_client = HttpWsExchangeClient::new(&config.exchange.rest_url, &config.exchange.ws_url, proxy)
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
    let max_consecutive_data_timeouts = 6usize;
    let mut consecutive_data_timeouts = 0usize;
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
            Ok(Ok(e)) => {
                consecutive_data_timeouts = 0;
                e
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                logger.log_event(
                    "market_data_timeout",
                    &serde_json::json!({
                        "timeout_ms": data_timeout_ms,
                        "mode": mode,
                        "consecutive": consecutive_data_timeouts + 1,
                        "limit": max_consecutive_data_timeouts
                    }),
                )?;
                consecutive_data_timeouts = consecutive_data_timeouts.saturating_add(1);
                if consecutive_data_timeouts >= max_consecutive_data_timeouts {
                    engine.risk_mut().emergency_stop(format!(
                        "market data timeout: no event within {}ms (consecutive timeouts: {}/{})",
                        data_timeout_ms,
                        consecutive_data_timeouts,
                        max_consecutive_data_timeouts
                    ));
                    break;
                }
                continue;
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
                let local_now = Utc::now();
                engine
                    .process_quote(quote, local_now, &mut logger)
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
    buffered: std::collections::VecDeque<crate::market_data::MarketEvent>,
    batch_size: usize,
    poll_timeout_ms: u64,
}

impl RestPollingSource {
    fn new(rest_url: &str, token_ids: Vec<String>, proxy_url: Option<&str>, poll_timeout_ms: u64) -> Self {
        let batch = token_ids.len().min(8).max(1);
        Self {
            client: PolymarketPublicClient::new(rest_url, proxy_url),
            token_ids,
            cursor: 0,
            buffered: std::collections::VecDeque::new(),
            batch_size: batch,
            poll_timeout_ms,
        }
    }
}

#[async_trait]
impl MarketDataSource for RestPollingSource {
    async fn next_event(&mut self) -> Result<crate::market_data::MarketEvent> {
        if let Some(evt) = self.buffered.pop_front() {
            return Ok(evt);
        }
        if self.token_ids.is_empty() {
            return Err(anyhow!("rest polling source has no tokens"));
        }
        let n = self.token_ids.len();
        let batch: Vec<String> = (0..self.batch_size)
            .map(|_| {
                let token = self.token_ids[self.cursor % n].clone();
                self.cursor = (self.cursor + 1) % n;
                token
            })
            .collect();

        let poll_timeout_ms = self.poll_timeout_ms;
        let futs: Vec<_> = batch
            .into_iter()
            .map(|token| {
                let client = self.client.clone();
                async move {
                    let book = match timeout(
                        StdDuration::from_millis(poll_timeout_ms),
                        client.get_orderbook(&token),
                    )
                    .await
                    {
                        Ok(Ok(v)) => v,
                        _ => return None,
                    };
                    let best_bid = book.bids.first().map(|l| l.price_f64()).unwrap_or(0.0);
                    let best_ask = book.asks.first().map(|l| l.price_f64()).unwrap_or(0.0);
                    if !(best_bid > 0.0 && best_ask > 0.0 && best_ask > best_bid) {
                        return None;
                    }
                    if (best_ask - best_bid) > MAX_VALID_BOOK_SPREAD {
                        return None;
                    }
                    let bid_size = book.bids.first().map(|l| l.size_f64()).unwrap_or(0.0);
                    let ask_size = book.asks.first().map(|l| l.size_f64()).unwrap_or(0.0);
                    let ts = book
                        .timestamp
                        .as_deref()
                        .and_then(parse_book_ts)
                        .unwrap_or_else(Utc::now);
                    Some(crate::market_data::MarketEvent::BookUpdate {
                        market: token,
                        bid: best_bid,
                        bid_size,
                        ask: best_ask,
                        ask_size,
                        timestamp: ts,
                    })
                }
            })
            .collect();

        let results = futures_util::future::join_all(futs).await;
        for evt in results.into_iter().flatten() {
            self.buffered.push_back(evt);
        }

        self.buffered
            .pop_front()
            .ok_or_else(|| anyhow!("no valid orderbook snapshot available from REST polling"))
    }
}

struct HybridLiveSource {
    ws: PolymarketWsSource,
    rest: RestPollingSource,
    ws_enabled: bool,
    ws_probe_timeout_ms: u64,
    ws_reconnect_at: Instant,
    ws_quiet_count: u32,
    ws_quiet_disable_after: u32,
}

impl HybridLiveSource {
    fn new(ws: PolymarketWsSource, rest: RestPollingSource, ws_enabled: bool) -> Self {
        Self {
            ws,
            rest,
            ws_enabled,
            ws_probe_timeout_ms: 250,
            ws_reconnect_at: Instant::now(),
            ws_quiet_count: 0,
            // Keep WS alive by default; use REST as gap-fill only.
            ws_quiet_disable_after: 0,
        }
    }
}

#[async_trait]
impl MarketDataSource for HybridLiveSource {
    async fn next_event(&mut self) -> Result<crate::market_data::MarketEvent> {
        loop {
            if !self.ws_enabled && Instant::now() >= self.ws_reconnect_at {
                self.ws_enabled = true;
            }

            if self.ws_enabled {
                // Probe WS briefly; if quiet, use REST for this tick without disabling WS.
                match timeout(
                    StdDuration::from_millis(self.ws_probe_timeout_ms),
                    self.ws.next_event(),
                )
                .await
                {
                    Ok(Ok(event)) => {
                        self.ws_quiet_count = 0;
                        return Ok(event);
                    }
                    Ok(Err(err)) => {
                        warn!(
                            "ws market data stream failed, falling back to REST polling: {}",
                            err
                        );
                        self.ws_enabled = false;
                        self.ws_reconnect_at = Instant::now() + StdDuration::from_secs(5);
                    }
                    Err(_) => {
                        self.ws_quiet_count = self.ws_quiet_count.saturating_add(1);
                        if self.ws_quiet_disable_after > 0
                            && self.ws_quiet_count >= self.ws_quiet_disable_after
                        {
                            warn!(
                                "ws market data stream quiet ({} probes), pausing WS and using REST",
                                self.ws_quiet_count
                            );
                            self.ws_enabled = false;
                            self.ws_reconnect_at = Instant::now() + StdDuration::from_secs(5);
                            self.ws_quiet_count = 0;
                        }
                        if self.ws_quiet_count > 0 && self.ws_quiet_count % 50 == 0 {
                            warn!(
                                "ws market data stream quiet ({} probes), using REST gap-fill",
                                self.ws_quiet_count
                            );
                        }
                    }
                }
            }
            match self.rest.next_event().await {
                Ok(evt) => return Ok(evt),
                Err(_err) => {
                    tokio::time::sleep(StdDuration::from_millis(250)).await;
                }
            }
        }
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
        config
            .markets
            .symbols
            .iter()
            .map(|m| (m.clone(), m.clone()))
            .collect::<HashMap<_, _>>(),
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
    market_texts: HashMap<String, String>,
) -> Result<CoreLoopReport> {
    let mut cache = MarketDataCache::new(&markets);
    let mut strategy = MarketMakingStrategy::new(config.strategy.clone());
    let news_overlay = NewsOverlay::new(config.news.clone(), config.exchange.proxy_url.as_deref());
    let mut news_bias_by_market: HashMap<String, f64> = HashMap::new();
    let mut next_news_refresh_at = Utc::now();
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
            },
            "news": {
                "enabled": config.news.enabled,
                "provider": "guardian",
                "poll_secs": config.news.poll_secs,
            }
        }),
    )?;

    let mut ticks_executed = 0;
    let data_timeout_ms = (config.markets.data_event_timeout_ms as i64).max(config.risk.heartbeat_timeout_ms) as u64;
    let max_consecutive_data_timeouts = if mode == "paper-live" { 15usize } else { 1usize };
    let mut consecutive_data_timeouts = 0usize;
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
            Ok(Ok(e)) => {
                consecutive_data_timeouts = 0;
                e
            }
            Ok(Err(err)) => {
                logger.log_event(
                    "data_source_exhausted",
                    &serde_json::json!({ "error": err.to_string() }),
                )?;
                break;
            }
            Err(_) => {
                logger.log_event(
                    "market_data_timeout",
                    &serde_json::json!({
                        "timeout_ms": data_timeout_ms,
                        "mode": mode,
                        "consecutive": consecutive_data_timeouts + 1,
                        "limit": max_consecutive_data_timeouts
                    }),
                )?;
                consecutive_data_timeouts = consecutive_data_timeouts.saturating_add(1);
                if consecutive_data_timeouts >= max_consecutive_data_timeouts {
                    engine.risk_mut().emergency_stop(format!(
                        "market data timeout: no event within {}ms (consecutive timeouts: {}/{})",
                        data_timeout_ms,
                        consecutive_data_timeouts,
                        max_consecutive_data_timeouts
                    ));
                    break;
                }
                continue;
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
                if news_overlay.is_enabled() && Utc::now() >= next_news_refresh_at {
                    match news_overlay.refresh_market_biases(&market_texts).await {
                        Ok(new_biases) => {
                            news_bias_by_market = new_biases;
                            logger.log_event(
                                "news_bias_refresh",
                                &serde_json::json!({
                                    "markets_with_bias": news_bias_by_market.len(),
                                    "sample": news_bias_by_market.iter().take(3).collect::<Vec<_>>(),
                                }),
                            )?;
                        }
                        Err(err) => {
                            logger.log_event(
                                "news_bias_refresh_error",
                                &serde_json::json!({ "error": err.to_string() }),
                            )?;
                        }
                    }
                    next_news_refresh_at =
                        Utc::now() + Duration::seconds(config.news.poll_secs as i64);
                }
                let news_bps = news_bias_by_market
                    .get(&snapshot.market)
                    .copied()
                    .unwrap_or(0.0);
                strategy.set_news_bias_bps(&snapshot.market, news_bps);
                let quote = strategy.generate_quote(&snapshot, inventory);
                let local_now = Utc::now();
                logger.log_event(
                    "feature_sample",
                    &serde_json::json!({
                        "mode": mode,
                        "captured_at": local_now,
                        "market": snapshot.market,
                        "mid": snapshot.mid,
                        "micro_price": snapshot.micro_price,
                        "fair_value": snapshot.fair_value,
                        "spread_bps": snapshot.spread_bps,
                        "imbalance": snapshot.imbalance,
                        "order_flow_signal": snapshot.order_flow_signal,
                        "alpha_bps": snapshot.alpha_bps,
                        "inventory": inventory,
                        "news_bias_bps": news_bps,
                        "quote_bid_price": quote.bid_price,
                        "quote_ask_price": quote.ask_price,
                        "quote_bid_size": quote.bid_size,
                        "quote_ask_size": quote.ask_size,
                        "market_bid": quote.market_bid,
                        "market_ask": quote.market_ask
                    }),
                )?;
                engine
                    .process_quote(quote, local_now, logger)
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
