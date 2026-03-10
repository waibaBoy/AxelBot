use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
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
    remaining: f64,
    placed_at: DateTime<Utc>,
}

#[async_trait]
pub trait ExchangeClient {
    async fn place_order(&mut self, req: &OrderRequest) -> Result<AckedOrder>;
    async fn cancel_order(&mut self, order_id: &str) -> Result<()>;
    async fn poll_fills(
        &mut self,
        mid_prices: &HashMap<String, f64>,
        now: DateTime<Utc>,
    ) -> Result<Vec<FillEvent>>;
}

pub struct HttpWsExchangeClient {
    pub rest_url: String,
    pub ws_url: String,
}

impl HttpWsExchangeClient {
    pub fn new(rest_url: impl Into<String>, ws_url: impl Into<String>) -> Self {
        Self {
            rest_url: rest_url.into(),
            ws_url: ws_url.into(),
        }
    }
}

#[async_trait]
impl ExchangeClient for HttpWsExchangeClient {
    async fn place_order(&mut self, _req: &OrderRequest) -> Result<AckedOrder> {
        Err(anyhow::anyhow!(
            "HttpWsExchangeClient adapter not wired yet; implement REST/WS integration for live trading"
        ))
    }

    async fn cancel_order(&mut self, _order_id: &str) -> Result<()> {
        Err(anyhow::anyhow!(
            "HttpWsExchangeClient adapter not wired yet; implement REST cancel endpoint"
        ))
    }

    async fn poll_fills(
        &mut self,
        _mid_prices: &HashMap<String, f64>,
        _now: DateTime<Utc>,
    ) -> Result<Vec<FillEvent>> {
        Err(anyhow::anyhow!(
            "HttpWsExchangeClient adapter not wired yet; implement WS/user-stream fill handling"
        ))
    }
}

#[derive(Debug)]
struct SimOrder {
    req: OrderRequest,
    remaining: f64,
}

pub struct SimulatedExchangeClient {
    open_orders: HashMap<String, SimOrder>,
    tick: u64,
    fee_bps: f64,
}

impl SimulatedExchangeClient {
    pub fn new(fee_bps: f64) -> Self {
        Self {
            open_orders: HashMap::new(),
            tick: 0,
            fee_bps,
        }
    }
}

#[async_trait]
impl ExchangeClient for SimulatedExchangeClient {
    async fn place_order(&mut self, req: &OrderRequest) -> Result<AckedOrder> {
        let now = Utc::now();
        let order_id = Uuid::new_v4().to_string();
        let latency_ms = (now - req.quote_ts).num_milliseconds().max(1);
        self.open_orders.insert(
            order_id.clone(),
            SimOrder {
                req: req.clone(),
                remaining: req.size,
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
        self.open_orders.remove(order_id);
        Ok(())
    }

    async fn poll_fills(
        &mut self,
        mid_prices: &HashMap<String, f64>,
        now: DateTime<Utc>,
    ) -> Result<Vec<FillEvent>> {
        self.tick = self.tick.wrapping_add(1);
        let mut fills = Vec::new();
        let mut completed = Vec::new();

        for (id, order) in &mut self.open_orders {
            let mid = mid_prices
                .get(&order.req.market)
                .copied()
                .unwrap_or(order.req.price);
            let fillable = match order.req.side {
                Side::Bid => order.req.price >= mid * (1.0 - 0.0008),
                Side::Ask => order.req.price <= mid * (1.0 + 0.0008),
            };
            if !fillable {
                continue;
            }

            let partial = self.tick % 3 == 0;
            let fill_size = if partial {
                (order.remaining * 0.5).max(0.01)
            } else {
                order.remaining
            }
            .min(order.remaining);
            if fill_size <= 0.0 {
                continue;
            }
            order.remaining -= fill_size;
            let fee_paid = fill_size * order.req.price * (self.fee_bps / 10_000.0);
            fills.push(FillEvent {
                fill_id: Uuid::new_v4().to_string(),
                order_id: id.clone(),
                market: order.req.market.clone(),
                side: order.req.side,
                price: order.req.price,
                size: fill_size,
                expected_price: order.req.price,
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
        Ok(fills)
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
    _cfg: ExecutionConfig,
    risk: RiskEngine,
    portfolio: PortfolioSnapshot,
    open_orders: HashMap<String, OpenOrder>,
    seen_fills: HashSet<String>,
    metrics: RuntimeMetrics,
}

impl<C: ExchangeClient> ExecutionEngine<C> {
    pub fn new(client: C, cfg: ExecutionConfig, risk: RiskEngine, starting_cash: f64) -> Self {
        Self {
            client,
            _cfg: cfg,
            risk,
            portfolio: PortfolioSnapshot::with_starting_cash(starting_cash),
            open_orders: HashMap::new(),
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
            token_id: None,
            side: Side::Bid,
            price: quote.bid_price,
            size: quote.bid_size,
            quote_ts: quote.generated_at,
        };
        let ask_req = OrderRequest {
            market: quote.market.clone(),
            token_id: None,
            side: Side::Ask,
            price: quote.ask_price,
            size: quote.ask_size,
            quote_ts: quote.generated_at,
        };

        let inventory = self.current_inventory();
        let mut accepted_any = false;
        for req in [bid_req, ask_req] {
            let decision = self
                .risk
                .check_pre_trade(&req, now, &inventory, self.open_orders.len());
            if decision.allowed {
                let ack = self.client.place_order(&req).await?;
                self.metrics.orders_submitted += 1;
                self.metrics.latency_ms_accum += ack.latency_ms;
                self.open_orders.insert(
                    ack.order_id.clone(),
                    OpenOrder {
                        order_id: ack.order_id.clone(),
                        market: ack.market.clone(),
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

        let fills = self.client.poll_fills(mid_prices, now).await?;
        for fill in fills {
            self.apply_fill_event(fill, logger)?;
        }

        let equity = self.portfolio.total_equity();
        self.risk.check_drawdown(equity);
        self.risk.check_heartbeat(now, last_data_at);
        logger.log_event("metrics", &self.metrics_snapshot())?;
        Ok(())
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
            max_open_orders: 50,
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
        let mut engine = ExecutionEngine::new(client, exec_cfg(), risk, 1_000.0);

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
