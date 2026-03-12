use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::{
    sync::{mpsc, mpsc::error::TryRecvError, Mutex},
    time::{sleep, Duration as TokioDuration},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::warn;
use uuid::Uuid;

use crate::{
    config::ExecutionConfig,
    risk::{RiskDecision, RiskEngine},
    strategy::QuoteProposal,
    types::{FillEvent, OrderRequest, PortfolioSnapshot, Position, Side},
};

#[derive(Debug, Clone, Serialize)]
pub struct AckedOrder {
    pub order_id: String,
    pub market: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub accepted_at: DateTime<Utc>,
    pub latency_ms: i64,
}

#[derive(Debug, Clone)]
struct OpenOrder {
    order_id: String,
    market: String,
    side: Side,
    price: f64,
    remaining: f64,
    placed_at: DateTime<Utc>,
}

#[async_trait]
pub trait ExchangeClient {
    async fn place_order(&mut self, req: &OrderRequest) -> Result<AckedOrder>;
    async fn cancel_order(&mut self, order_id: &str) -> Result<()>;
    fn subscribe_fills(&mut self) -> mpsc::Receiver<FillEvent>;
}

pub struct HttpWsExchangeClient {
    pub rest_url: String,
    pub ws_url: String,
    client: Option<Arc<crate::polymarket::client::PolymarketClient>>,
    nonce: crate::polymarket::nonce::NonceManager,
    /// Track which markets use neg_risk (condition_id → bool)
    neg_risk_map: HashMap<String, bool>,
    /// Track order_id → (market, side, price, size, placed_at) for fill polling
    pending_orders: Arc<Mutex<HashMap<String, PendingOrderInfo>>>,
    fill_tx: mpsc::Sender<FillEvent>,
    fill_rx: Option<mpsc::Receiver<FillEvent>>,
    fill_stream_started: bool,
}

const MIN_TRADE_POLL_INTERVAL_MS: i64 = 250;

#[derive(Debug, Clone)]
struct PendingOrderInfo {
    market: String,
    side: Side,
    price: f64,
    placed_at: DateTime<Utc>,
}

impl HttpWsExchangeClient {
    pub fn new(rest_url: impl Into<String>, ws_url: impl Into<String>) -> Self {
        let (fill_tx, fill_rx) = mpsc::channel(1024);
        Self {
            rest_url: rest_url.into(),
            ws_url: ws_url.into(),
            client: None,
            nonce: crate::polymarket::nonce::NonceManager::new(),
            neg_risk_map: HashMap::new(),
            pending_orders: Arc::new(Mutex::new(HashMap::new())),
            fill_tx,
            fill_rx: Some(fill_rx),
            fill_stream_started: false,
        }
    }

    /// Attach the live Polymarket client and market metadata.
    pub fn with_client(
        mut self,
        client: crate::polymarket::client::PolymarketClient,
        neg_risk_map: HashMap<String, bool>,
    ) -> Self {
        self.client = Some(Arc::new(client));
        self.neg_risk_map = neg_risk_map;
        let (tx, rx) = mpsc::channel(1024);
        self.fill_tx = tx;
        self.fill_rx = Some(rx);
        self
    }

    fn start_fill_stream_if_needed(&mut self) {
        if self.fill_stream_started {
            return;
        }
        self.fill_stream_started = true;

        let Some(client) = self.client.clone() else {
            return;
        };
        let ws_url = user_ws_url(&self.ws_url);
        let (api_key, api_secret, passphrase) = client.user_ws_auth();
        let tx = self.fill_tx.clone();
        let pending = Arc::clone(&self.pending_orders);

        tokio::spawn(async move {
            let mut backoff_ms: u64 = (MIN_TRADE_POLL_INTERVAL_MS as u64).saturating_mul(2);
            loop {
                let (ws_stream, _resp) = match connect_async(&ws_url).await {
                    Ok(v) => {
                        backoff_ms = (MIN_TRADE_POLL_INTERVAL_MS as u64).saturating_mul(2);
                        v
                    }
                    Err(_) => {
                        sleep(TokioDuration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(15_000);
                        continue;
                    }
                };

                let (mut writer, mut reader) = ws_stream.split();
                let subscribe = serde_json::json!({
                    "type": "user",
                    "auth": {
                        "apiKey": api_key,
                        "secret": api_secret,
                        "passphrase": passphrase,
                    }
                });
                if writer
                    .send(Message::Text(subscribe.to_string().into()))
                    .await
                    .is_err()
                {
                    sleep(TokioDuration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(15_000);
                    continue;
                }

                let mut seen_fill_ids: HashSet<String> = HashSet::new();
                loop {
                    let msg = match reader.next().await {
                        Some(Ok(m)) => m,
                        Some(Err(_)) | None => break,
                    };
                    let text = match msg {
                        Message::Text(t) => t.to_string(),
                        Message::Binary(b) => match String::from_utf8(b.to_vec()) {
                            Ok(s) => s,
                            Err(_) => continue,
                        },
                        Message::Ping(v) => {
                            let _ = writer.send(Message::Pong(v)).await;
                            continue;
                        }
                        Message::Pong(_) => continue,
                        Message::Close(_) => break,
                        _ => continue,
                    };

                    let pending_snapshot = { pending.lock().await.clone() };
                    let fills = parse_user_ws_fills(&text, &pending_snapshot);
                    for mut fill in fills {
                        if !seen_fill_ids.insert(fill.fill_id.clone()) {
                            continue;
                        }
                        if fill.order_id.is_empty() {
                            fill.order_id =
                                infer_order_id(&pending_snapshot, &fill.market, fill.side)
                                    .unwrap_or_else(|| fill.fill_id.clone());
                        }
                        if tx.send(fill).await.is_err() {
                            return;
                        }
                    }
                    if seen_fill_ids.len() > 50_000 {
                        seen_fill_ids.clear();
                    }
                }

                warn!("user fill websocket disconnected, reconnecting");
                sleep(TokioDuration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(15_000);
            }
        });
    }
}

#[async_trait]
impl ExchangeClient for HttpWsExchangeClient {
    async fn place_order(&mut self, req: &OrderRequest) -> Result<AckedOrder> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("HttpWsExchangeClient: no Polymarket client attached; set API credentials for live trading"))?;

        let token_id = req
            .token_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("OrderRequest missing token_id for live trading"))?;

        let neg_risk = self.neg_risk_map.get(&req.market).copied().unwrap_or(false);

        let nonce = self.nonce.next();
        let before = Utc::now();

        let resp = client
            .place_order(
                token_id,
                req.side,
                req.price,
                req.size,
                neg_risk,
                nonce,
                crate::polymarket::types::ClobOrderType::Gtc,
            )
            .await?;

        let latency_ms = (Utc::now() - before).num_milliseconds().max(1);

        let order_id = resp.order_id.ok_or_else(|| {
            anyhow::anyhow!(
                "order rejected by CLOB: {}",
                resp.error_msg
                    .unwrap_or_else(|| "unknown error".to_string())
            )
        })?;

        let now = Utc::now();
        self.pending_orders.lock().await.insert(
            order_id.clone(),
            PendingOrderInfo {
                market: req.market.clone(),
                side: req.side,
                price: req.price,
                placed_at: now,
            },
        );

        Ok(AckedOrder {
            order_id,
            market: req.market.clone(),
            side: req.side,
            price: req.price,
            size: req.size,
            accepted_at: now,
            latency_ms,
        })
    }

    async fn cancel_order(&mut self, order_id: &str) -> Result<()> {
        let client = self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("HttpWsExchangeClient: no Polymarket client attached")
        })?;

        client.cancel_order(order_id).await?;
        self.pending_orders.lock().await.remove(order_id);
        Ok(())
    }

    fn subscribe_fills(&mut self) -> mpsc::Receiver<FillEvent> {
        self.start_fill_stream_if_needed();
        self.fill_rx.take().unwrap_or_else(|| {
            let (_tx, rx) = mpsc::channel(1);
            rx
        })
    }
}

pub struct SimulatedExchangeClient {
    _fee_bps: f64,
    fill_rx: Option<mpsc::Receiver<FillEvent>>,
}

impl SimulatedExchangeClient {
    pub fn new(fee_bps: f64) -> Self {
        let (_tx, rx) = mpsc::channel(64);
        Self {
            _fee_bps: fee_bps,
            fill_rx: Some(rx),
        }
    }
}

#[async_trait]
impl ExchangeClient for SimulatedExchangeClient {
    async fn place_order(&mut self, req: &OrderRequest) -> Result<AckedOrder> {
        let now = Utc::now();
        let order_id = Uuid::new_v4().to_string();
        let latency_ms = (now - req.quote_ts).num_milliseconds().max(1);
        Ok(AckedOrder {
            order_id,
            market: req.market.clone(),
            side: req.side,
            price: req.price,
            size: req.size,
            accepted_at: now,
            latency_ms,
        })
    }

    async fn cancel_order(&mut self, _order_id: &str) -> Result<()> {
        Ok(())
    }

    fn subscribe_fills(&mut self) -> mpsc::Receiver<FillEvent> {
        self.fill_rx.take().unwrap_or_else(|| {
            let (_tx, rx) = mpsc::channel(1);
            rx
        })
    }
}

pub struct JsonlLogger {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl JsonlLogger {
    pub fn new(output_dir: impl AsRef<Path>, mode: &str) -> Result<Self> {
        let output_dir = output_dir.as_ref();
        fs::create_dir_all(output_dir)?;
        let file_name = format!("{}-{}.jsonl", mode, Utc::now().format("%Y%m%dT%H%M%S"));
        let path = output_dir.join(file_name);
        let file = File::create(&path)?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn log_event<T: Serialize>(&mut self, event: &str, payload: &T) -> Result<()> {
        let row = serde_json::json!({
            "ts": Utc::now(),
            "event": event,
            "payload": payload,
        });
        serde_json::to_writer(&mut self.writer, &row)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub inventory_by_market: HashMap<String, f64>,
    pub avg_order_latency_ms: f64,
    pub avg_fill_latency_ms: f64,
    pub quote_uptime_ratio: f64,
    pub cancel_ratio: f64,
    pub avg_slippage_bps: f64,
    pub total_fills: usize,
    pub total_orders_submitted: usize,
    pub total_orders_rejected: usize,
}

#[derive(Default)]
struct RuntimeMetrics {
    cycles: usize,
    cycles_with_quotes: usize,
    orders_submitted: usize,
    orders_rejected: usize,
    orders_canceled: usize,
    fills: usize,
    latency_ms_accum: i64,
    fill_latency_ms_accum: i64,
    fill_latency_samples: usize,
    slippage_bps_accum: f64,
}

pub struct ExecutionEngine<C: ExchangeClient> {
    client: C,
    cfg: ExecutionConfig,
    risk: RiskEngine,
    portfolio: PortfolioSnapshot,
    open_orders: HashMap<String, OpenOrder>,
    fill_rx: mpsc::Receiver<FillEvent>,
    local_fill_model: bool,
    sim_tick: u64,
    seen_fills: HashSet<String>,
    metrics: RuntimeMetrics,
}

impl<C: ExchangeClient> ExecutionEngine<C> {
    pub fn new(
        mut client: C,
        cfg: ExecutionConfig,
        risk: RiskEngine,
        starting_cash: f64,
        local_fill_model: bool,
    ) -> Self {
        let fill_rx = client.subscribe_fills();
        Self {
            client,
            cfg,
            risk,
            portfolio: PortfolioSnapshot::with_starting_cash(starting_cash),
            open_orders: HashMap::new(),
            fill_rx,
            local_fill_model,
            sim_tick: 0,
            seen_fills: HashSet::new(),
            metrics: RuntimeMetrics::default(),
        }
    }

    pub fn portfolio(&self) -> &PortfolioSnapshot {
        &self.portfolio
    }

    pub fn risk(&self) -> &RiskEngine {
        &self.risk
    }

    pub fn risk_mut(&mut self) -> &mut RiskEngine {
        &mut self.risk
    }

    pub fn current_inventory(&self) -> HashMap<String, f64> {
        self.portfolio
            .positions
            .iter()
            .map(|(m, p)| (m.clone(), p.qty))
            .collect()
    }

    pub async fn process_quote(
        &mut self,
        quote: QuoteProposal,
        now: DateTime<Utc>,
        logger: &mut JsonlLogger,
    ) -> Result<()> {
        self.metrics.cycles += 1;
        self.cancel_market_orders(&quote.market, logger).await?;

        let bid_req = OrderRequest {
            market: quote.market.clone(),
            token_id: Some(quote.market.clone()),
            side: Side::Bid,
            price: quote.bid_price,
            size: quote.bid_size,
            quote_ts: quote.generated_at,
        };
        let ask_req = OrderRequest {
            market: quote.market.clone(),
            token_id: Some(quote.market.clone()),
            side: Side::Ask,
            price: quote.ask_price,
            size: quote.ask_size,
            quote_ts: quote.generated_at,
        };

        let inventory = self.current_inventory();
        let mut accepted_any = false;
        for req in [bid_req, ask_req] {
            if self.cfg.post_only && self.crosses_post_only_guard(&req, &quote) {
                self.metrics.orders_rejected += 1;
                logger.log_event(
                    "order_rejected",
                    &serde_json::json!({
                        "market": req.market,
                        "side": req.side,
                        "price": req.price,
                        "size": req.size,
                        "reason": "post_only_cross_guard",
                    }),
                )?;
                continue;
            }

            let decision = self
                .risk
                .check_pre_trade(&req, now, &inventory, self.open_orders.len());
            if decision.allowed {
                let ack = match self.place_order_with_retries(&req).await {
                    Ok(ack) => ack,
                    Err(err) => {
                        self.metrics.orders_rejected += 1;
                        logger.log_event(
                            "order_submit_failed",
                            &serde_json::json!({
                                "market": req.market,
                                "side": req.side,
                                "price": req.price,
                                "size": req.size,
                                "error": err.to_string(),
                            }),
                        )?;
                        continue;
                    }
                };
                self.metrics.orders_submitted += 1;
                self.metrics.latency_ms_accum += ack.latency_ms;
                self.open_orders.insert(
                    ack.order_id.clone(),
                    OpenOrder {
                        order_id: ack.order_id.clone(),
                        market: ack.market.clone(),
                        side: ack.side,
                        price: ack.price,
                        remaining: ack.size,
                        placed_at: ack.accepted_at,
                    },
                );
                logger.log_event("order_accepted", &ack)?;
                accepted_any = true;
            } else {
                self.metrics.orders_rejected += 1;
                logger.log_event("order_rejected", &Self::decision_for_log(&req, &decision))?;
            }
        }

        if accepted_any {
            self.metrics.cycles_with_quotes += 1;
            logger.log_event("quote_posted", &quote)?;
        }
        Ok(())
    }

    async fn cancel_market_orders(&mut self, market: &str, logger: &mut JsonlLogger) -> Result<()> {
        let ids: Vec<String> = self
            .open_orders
            .values()
            .filter(|o| o.market == market)
            .map(|o| o.order_id.clone())
            .collect();
        for id in ids {
            self.client.cancel_order(&id).await?;
            self.open_orders.remove(&id);
            self.metrics.orders_canceled += 1;
            logger.log_event("order_canceled", &serde_json::json!({ "order_id": id }))?;
        }
        Ok(())
    }

    async fn cancel_stale_orders(
        &mut self,
        now: DateTime<Utc>,
        logger: &mut JsonlLogger,
    ) -> Result<()> {
        if self.cfg.order_ttl_ms <= 0 {
            return Ok(());
        }
        let stale_ids: Vec<String> = self
            .open_orders
            .values()
            .filter(|o| (now - o.placed_at).num_milliseconds() > self.cfg.order_ttl_ms)
            .map(|o| o.order_id.clone())
            .collect();
        for id in stale_ids {
            match self.client.cancel_order(&id).await {
                Ok(_) => {
                    self.open_orders.remove(&id);
                    self.metrics.orders_canceled += 1;
                    logger.log_event(
                        "order_ttl_canceled",
                        &serde_json::json!({
                            "order_id": id,
                            "ttl_ms": self.cfg.order_ttl_ms,
                        }),
                    )?;
                }
                Err(err) => {
                    logger.log_event(
                        "order_ttl_cancel_failed",
                        &serde_json::json!({
                            "order_id": id,
                            "error": err.to_string(),
                        }),
                    )?;
                }
            }
        }
        Ok(())
    }

    pub async fn on_tick(
        &mut self,
        mid_prices: &HashMap<String, f64>,
        now: DateTime<Utc>,
        last_data_at: Option<DateTime<Utc>>,
        logger: &mut JsonlLogger,
    ) -> Result<()> {
        for (market, mid) in mid_prices {
            self.portfolio.set_mid_price(market.clone(), *mid);
        }

        self.cancel_stale_orders(now, logger).await?;

        if self.local_fill_model {
            for fill in self.simulate_local_fills(mid_prices, now) {
                self.apply_fill_event(fill, logger)?;
            }
        }
        for fill in self.drain_external_fills() {
            self.apply_fill_event(fill, logger)?;
        }

        let equity = self.portfolio.total_equity();
        self.risk.check_drawdown(equity);
        self.risk.check_heartbeat(now, last_data_at);
        logger.log_event("metrics", &self.metrics_snapshot())?;
        Ok(())
    }

    fn drain_external_fills(&mut self) -> Vec<FillEvent> {
        let mut out = Vec::new();
        loop {
            match self.fill_rx.try_recv() {
                Ok(fill) => out.push(fill),
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }
        out
    }

    fn simulate_local_fills(
        &mut self,
        mid_prices: &HashMap<String, f64>,
        now: DateTime<Utc>,
    ) -> Vec<FillEvent> {
        self.sim_tick = self.sim_tick.wrapping_add(1);
        let mut fills = Vec::new();
        let mut completed = Vec::new();

        for (id, order) in &mut self.open_orders {
            let mid = mid_prices
                .get(&order.market)
                .copied()
                .unwrap_or(order.price);
            if !(mid > 0.0 && order.price > 0.0) {
                continue;
            }

            let distance_bps = match order.side {
                Side::Bid => ((mid - order.price) / mid) * 10_000.0,
                Side::Ask => ((order.price - mid) / mid) * 10_000.0,
            }
            .max(0.0);
            let age_ms = (now - order.placed_at).num_milliseconds().max(0);
            let base_prob = if distance_bps <= 8.0 {
                0.90
            } else if distance_bps <= 15.0 {
                0.65
            } else if distance_bps <= 30.0 {
                0.35
            } else if distance_bps <= 60.0 {
                0.12
            } else {
                0.04
            };
            let age_boost = ((age_ms as f64) / 1_000.0 * 0.06).min(0.30);
            let fill_prob = (base_prob + age_boost).min(0.95);
            let roll = deterministic_roll(id, self.sim_tick);
            if roll > fill_prob {
                continue;
            }

            let partial = self.sim_tick % 3 == 0 && distance_bps > 12.0;
            let fill_size = if partial {
                (order.remaining * 0.4).max(0.01)
            } else {
                order.remaining
            }
            .min(order.remaining);
            if fill_size <= 0.0 {
                continue;
            }
            order.remaining -= fill_size;
            let fee_paid = fill_size * order.price * (self.cfg.fill_fee_bps / 10_000.0);

            fills.push(FillEvent {
                fill_id: Uuid::new_v4().to_string(),
                order_id: id.clone(),
                market: order.market.clone(),
                side: order.side,
                price: order.price,
                size: fill_size,
                expected_price: order.price,
                fee_paid,
                timestamp: now,
            });

            if order.remaining <= 0.00001 {
                completed.push(id.clone());
            }
        }

        for id in completed {
            self.open_orders.remove(&id);
        }
        fills
    }

    pub fn apply_fill_event(&mut self, fill: FillEvent, logger: &mut JsonlLogger) -> Result<()> {
        if !self.seen_fills.insert(fill.fill_id.clone()) {
            logger.log_event("fill_duplicate_ignored", &fill)?;
            return Ok(());
        }
        self.metrics.fills += 1;
        let slip_bps = Self::slippage_bps(&fill);
        self.metrics.slippage_bps_accum += slip_bps;

        let mut remove_after = false;
        if let Some(order) = self.open_orders.get_mut(&fill.order_id) {
            let latency = (fill.timestamp - order.placed_at).num_milliseconds().max(0);
            self.metrics.fill_latency_ms_accum += latency;
            self.metrics.fill_latency_samples += 1;
            order.remaining -= fill.size;
            if order.remaining <= 0.00001 {
                remove_after = true;
            }
        }
        if remove_after {
            self.open_orders.remove(&fill.order_id);
        }
        self.apply_fill_to_portfolio(&fill);
        self.risk.on_fill(&fill);
        logger.log_event("fill", &fill)?;
        Ok(())
    }

    fn apply_fill_to_portfolio(&mut self, fill: &FillEvent) {
        let pos = self
            .portfolio
            .positions
            .entry(fill.market.clone())
            .or_insert_with(Position::default);

        match fill.side {
            Side::Bid => {
                self.portfolio.cash -= fill.price * fill.size + fill.fee_paid;
                if pos.qty >= 0.0 {
                    let new_qty = pos.qty + fill.size;
                    let weighted = (pos.avg_price * pos.qty) + (fill.price * fill.size);
                    pos.qty = new_qty;
                    pos.avg_price = if new_qty.abs() < f64::EPSILON {
                        0.0
                    } else {
                        weighted / new_qty
                    };
                } else {
                    let cover = fill.size.min(-pos.qty);
                    self.portfolio.realized_pnl += (pos.avg_price - fill.price) * cover;
                    pos.qty += fill.size;
                    let remaining = fill.size - cover;
                    if pos.qty > 0.0 && remaining > 0.0 {
                        pos.avg_price = fill.price;
                    }
                    if pos.qty.abs() < f64::EPSILON {
                        pos.avg_price = 0.0;
                    }
                }
            }
            Side::Ask => {
                self.portfolio.cash += fill.price * fill.size - fill.fee_paid;
                if pos.qty <= 0.0 {
                    let current_short = pos.qty.abs();
                    let new_short = current_short + fill.size;
                    let weighted = (pos.avg_price * current_short) + (fill.price * fill.size);
                    pos.qty -= fill.size;
                    pos.avg_price = if new_short.abs() < f64::EPSILON {
                        0.0
                    } else {
                        weighted / new_short
                    };
                } else {
                    let close = fill.size.min(pos.qty);
                    self.portfolio.realized_pnl += (fill.price - pos.avg_price) * close;
                    pos.qty -= fill.size;
                    let remaining = fill.size - close;
                    if pos.qty < 0.0 && remaining > 0.0 {
                        pos.avg_price = fill.price;
                    }
                    if pos.qty.abs() < f64::EPSILON {
                        pos.avg_price = 0.0;
                    }
                }
            }
        }
        self.portfolio.realized_pnl -= fill.fee_paid;
    }

    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        let inventory = self
            .portfolio
            .positions
            .iter()
            .map(|(market, pos)| (market.clone(), pos.qty))
            .collect::<HashMap<_, _>>();

        let avg_latency = if self.metrics.orders_submitted == 0 {
            0.0
        } else {
            self.metrics.latency_ms_accum as f64 / self.metrics.orders_submitted as f64
        };
        let quote_uptime = if self.metrics.cycles == 0 {
            0.0
        } else {
            self.metrics.cycles_with_quotes as f64 / self.metrics.cycles as f64
        };
        let avg_fill_latency = if self.metrics.fill_latency_samples == 0 {
            0.0
        } else {
            self.metrics.fill_latency_ms_accum as f64 / self.metrics.fill_latency_samples as f64
        };
        let cancel_ratio = if self.metrics.orders_submitted == 0 {
            0.0
        } else {
            self.metrics.orders_canceled as f64 / self.metrics.orders_submitted as f64
        };
        let avg_slippage = if self.metrics.fills == 0 {
            0.0
        } else {
            self.metrics.slippage_bps_accum / self.metrics.fills as f64
        };

        MetricsSnapshot {
            realized_pnl: self.portfolio.realized_pnl,
            unrealized_pnl: self.portfolio.unrealized_pnl(),
            inventory_by_market: inventory,
            avg_order_latency_ms: avg_latency,
            avg_fill_latency_ms: avg_fill_latency,
            quote_uptime_ratio: quote_uptime,
            cancel_ratio,
            avg_slippage_bps: avg_slippage,
            total_fills: self.metrics.fills,
            total_orders_submitted: self.metrics.orders_submitted,
            total_orders_rejected: self.metrics.orders_rejected,
        }
    }

    fn decision_for_log(req: &OrderRequest, decision: &RiskDecision) -> serde_json::Value {
        serde_json::json!({
            "market": req.market,
            "side": req.side,
            "price": req.price,
            "size": req.size,
            "reason": format!("{:?}", decision.reason),
        })
    }

    fn slippage_bps(fill: &FillEvent) -> f64 {
        match fill.side {
            Side::Bid => ((fill.price - fill.expected_price) / fill.expected_price) * 10_000.0,
            Side::Ask => ((fill.expected_price - fill.price) / fill.expected_price) * 10_000.0,
        }
        .max(0.0)
    }

    fn crosses_post_only_guard(&self, req: &OrderRequest, quote: &QuoteProposal) -> bool {
        if !(quote.market_bid > 0.0 && quote.market_ask > 0.0) {
            return false;
        }
        match req.side {
            Side::Bid => req.price >= quote.market_ask,
            Side::Ask => req.price <= quote.market_bid,
        }
    }

    async fn place_order_with_retries(&mut self, req: &OrderRequest) -> Result<AckedOrder> {
        let max_attempts = self.cfg.max_retries.saturating_add(1);
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            match self.client.place_order(req).await {
                Ok(ack) => return Ok(ack),
                Err(err) => {
                    if attempt >= max_attempts || !is_retryable_submit_error(&err) {
                        return Err(err);
                    }
                    // Bounded exponential backoff keeps pressure low under exchange/API stress.
                    let backoff_ms = 25u64.saturating_mul(1u64 << (attempt.min(6) as u32));
                    sleep(TokioDuration::from_millis(backoff_ms)).await;
                }
            }
        }
    }
}

fn is_retryable_submit_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_ascii_lowercase();
    [
        "timeout",
        "temporarily",
        "connection",
        "429",
        "502",
        "503",
        "504",
    ]
    .iter()
    .any(|needle| msg.contains(needle))
}

fn deterministic_roll(order_id: &str, tick: u64) -> f64 {
    let mut h: u64 = 1469598103934665603;
    for b in order_id.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h ^= tick;
    (h % 10_000) as f64 / 10_000.0
}

fn parse_trade_timestamp(raw: Option<&str>) -> Option<DateTime<Utc>> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
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

fn user_ws_url(market_ws_url: &str) -> String {
    if market_ws_url.contains("/ws/market") {
        return market_ws_url.replacen("/ws/market", "/ws/user", 1);
    }
    if market_ws_url.ends_with("/market") {
        return format!("{}{}", market_ws_url.trim_end_matches("/market"), "/user");
    }
    format!("{}/user", market_ws_url.trim_end_matches('/'))
}

fn parse_user_ws_fills(raw: &str, pending: &HashMap<String, PendingOrderInfo>) -> Vec<FillEvent> {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) else {
        return Vec::new();
    };
    let items: Vec<serde_json::Value> = if let Some(arr) = v.as_array() {
        arr.clone()
    } else {
        vec![v]
    };
    items
        .into_iter()
        .filter_map(|item| parse_user_ws_fill_item(&item, pending))
        .collect()
}

fn parse_user_ws_fill_item(
    v: &serde_json::Value,
    pending: &HashMap<String, PendingOrderInfo>,
) -> Option<FillEvent> {
    let event_type = json_get_str_ci(v, &["event_type", "eventType"])
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !event_type.is_empty()
        && !matches!(
            event_type.as_str(),
            "trade" | "fill" | "order_filled" | "match" | "last_trade_price"
        )
    {
        return None;
    }

    let price = json_get_f64_ci(v, &["price", "last_trade_price", "match_price"])?;
    let size = json_get_f64_ci(v, &["size", "matched_amount", "amount"]).unwrap_or(0.0);
    if !(price > 0.0 && size > 0.0) {
        return None;
    }

    let market = json_get_str_ci(v, &["asset_id", "assetId", "token_id", "tokenId"])
        .or_else(|| {
            v.get("maker_orders")
                .and_then(|x| x.as_array())
                .and_then(|arr| arr.first())
                .and_then(|first| {
                    json_get_str_ci(first, &["asset_id", "assetId", "token_id", "tokenId"])
                })
        })
        .unwrap_or_default();
    if market.is_empty() {
        return None;
    }

    let mut order_id = json_get_str_ci(v, &["order_id", "orderID", "maker_order_id"])
        .or_else(|| {
            v.get("maker_orders")
                .and_then(|x| x.as_array())
                .and_then(|arr| arr.first())
                .and_then(|first| json_get_str_ci(first, &["order_id", "orderID", "id"]))
        })
        .unwrap_or_default();
    if order_id.is_empty() {
        order_id = infer_order_id(pending, &market, parse_side(v)).unwrap_or_default();
    }

    let side = if let Some(info) = pending.get(&order_id) {
        info.side
    } else {
        parse_side(v)
    };
    let expected_price = pending.get(&order_id).map(|i| i.price).unwrap_or(price);
    let fee_bps = json_get_f64_ci(
        v,
        &[
            "fee_rate_bps",
            "feeRateBps",
            "maker_fee_rate_bps",
            "taker_fee_rate_bps",
        ],
    )
    .unwrap_or(0.0);
    let fee_paid = price * size * (fee_bps / 10_000.0);
    let timestamp = json_get_ts_ci(
        v,
        &[
            "timestamp",
            "ts",
            "time",
            "match_time",
            "matchTime",
            "created_at",
        ],
    )
    .unwrap_or_else(Utc::now);

    let fill_id = json_get_str_ci(v, &["id", "trade_id", "fill_id"]).unwrap_or_else(|| {
        format!(
            "{}-{}-{}-{}",
            market,
            side.sign(),
            price,
            timestamp.timestamp_millis()
        )
    });

    Some(FillEvent {
        fill_id,
        order_id,
        market,
        side,
        price,
        size,
        expected_price,
        fee_paid,
        timestamp,
    })
}

fn parse_side(v: &serde_json::Value) -> Side {
    let side = json_get_str_ci(v, &["side"]).or_else(|| {
        v.get("maker_orders")
            .and_then(|x| x.as_array())
            .and_then(|arr| arr.first())
            .and_then(|first| json_get_str_ci(first, &["side"]))
    });
    match side
        .unwrap_or_else(|| "buy".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "sell" | "ask" => Side::Ask,
        _ => Side::Bid,
    }
}

fn json_get_f64_ci(v: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|k| {
        v.get(*k).and_then(|raw| {
            raw.as_f64()
                .or_else(|| raw.as_i64().map(|n| n as f64))
                .or_else(|| raw.as_u64().map(|n| n as f64))
                .or_else(|| raw.as_str().and_then(|s| s.parse::<f64>().ok()))
        })
    })
}

fn json_get_str_ci(v: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|k| {
        v.get(*k)
            .and_then(|raw| raw.as_str().map(|s| s.trim().to_string()))
            .filter(|s| !s.is_empty())
    })
}

fn json_get_ts_ci(v: &serde_json::Value, keys: &[&str]) -> Option<DateTime<Utc>> {
    for k in keys {
        let Some(raw) = v.get(*k) else {
            continue;
        };
        if let Some(s) = raw.as_str() {
            if let Some(dt) = parse_trade_timestamp(Some(s)) {
                return Some(dt);
            }
        }
        if let Some(n) = raw.as_i64() {
            let s = n.to_string();
            if let Some(dt) = parse_trade_timestamp(Some(&s)) {
                return Some(dt);
            }
        }
    }
    None
}

fn infer_order_id(
    pending: &HashMap<String, PendingOrderInfo>,
    market: &str,
    side: Side,
) -> Option<String> {
    pending
        .iter()
        .filter(|(_, info)| info.market == market && info.side == side)
        .min_by_key(|(_, info)| info.placed_at)
        .map(|(id, _)| id.clone())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        config::{ExecutionConfig, RiskConfig},
        risk::RiskEngine,
    };

    fn risk_cfg() -> RiskConfig {
        RiskConfig {
            global_drawdown_stop_pct: 0.25,
            max_per_market_exposure: 100.0,
            max_total_exposure: 500.0,
            max_open_orders: 50,
            max_orders_per_sec: 100,
            stale_quote_timeout_ms: 1_000,
            heartbeat_timeout_ms: 5_000,
            kill_switch: false,
            max_adverse_fills_in_window: 3,
            circuit_window_secs: 60,
            max_slippage_bps: 50.0,
        }
    }

    fn exec_cfg() -> ExecutionConfig {
        ExecutionConfig {
            post_only: true,
            order_ttl_ms: 2_000,
            max_retries: 1,
            fill_fee_bps: 2.0,
        }
    }

    #[test]
    fn pnl_accounting_with_fees() {
        let tmp = tempdir().unwrap();
        let mut logger = JsonlLogger::new(tmp.path(), "test").unwrap();
        let risk = RiskEngine::new(risk_cfg(), 1_000.0);
        let client = SimulatedExchangeClient::new(2.0);
        let mut engine = ExecutionEngine::new(client, exec_cfg(), risk, 1_000.0, true);

        let buy = FillEvent {
            fill_id: "f1".to_string(),
            order_id: "o1".to_string(),
            market: "M1".to_string(),
            side: Side::Bid,
            price: 0.40,
            size: 10.0,
            expected_price: 0.40,
            fee_paid: 0.01,
            timestamp: Utc::now(),
        };
        let sell = FillEvent {
            fill_id: "f2".to_string(),
            order_id: "o2".to_string(),
            market: "M1".to_string(),
            side: Side::Ask,
            price: 0.45,
            size: 10.0,
            expected_price: 0.45,
            fee_paid: 0.01,
            timestamp: Utc::now(),
        };

        engine.apply_fill_event(buy, &mut logger).unwrap();
        engine.apply_fill_event(sell, &mut logger).unwrap();
        assert!(engine.portfolio.realized_pnl > 0.45);
        assert!(engine.portfolio.realized_pnl < 0.55);
    }
}
