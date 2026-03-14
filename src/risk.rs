use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Duration, Utc};

use crate::{
    config::RiskConfig,
    types::{FillEvent, OrderRequest, Side},
};

#[derive(Debug, Clone)]
pub enum RiskRejectReason {
    GlobalHalt(String),
    MarketCircuitBreak(String),
    ExposureLimit {
        market: String,
        projected: f64,
        limit: f64,
    },
    GlobalExposureLimit {
        projected_total: f64,
        limit: f64,
    },
    MaxOpenOrders {
        current: usize,
        limit: usize,
    },
    OrderVelocity {
        current_per_sec: usize,
        limit_per_sec: usize,
    },
    StaleQuote {
        age_ms: i64,
        limit_ms: i64,
    },
}

#[derive(Debug, Clone)]
pub struct RiskDecision {
    pub allowed: bool,
    pub reason: Option<RiskRejectReason>,
}

impl RiskDecision {
    pub fn allow() -> Self {
        Self {
            allowed: true,
            reason: None,
        }
    }

    pub fn reject(reason: RiskRejectReason) -> Self {
        Self {
            allowed: false,
            reason: Some(reason),
        }
    }
}

#[derive(Debug)]
pub struct RiskEngine {
    cfg: RiskConfig,
    session_start_equity: f64,
    peak_equity: f64,
    halted: bool,
    halt_reason: Option<String>,
    halted_markets: HashSet<String>,
    adverse_fill_times: HashMap<String, VecDeque<DateTime<Utc>>>,
    order_submit_times: VecDeque<DateTime<Utc>>,
}

impl RiskEngine {
    pub fn new(cfg: RiskConfig, session_start_equity: f64) -> Self {
        let halted = cfg.kill_switch;
        let halt_reason = if halted {
            Some("kill switch enabled by config/env".to_string())
        } else {
            None
        };
        Self {
            cfg,
            session_start_equity,
            peak_equity: session_start_equity,
            halted,
            halt_reason,
            halted_markets: HashSet::new(),
            adverse_fill_times: HashMap::new(),
            order_submit_times: VecDeque::new(),
        }
    }

    pub fn is_halted(&self) -> bool {
        self.halted
    }

    pub fn halt_reason(&self) -> Option<&str> {
        self.halt_reason.as_deref()
    }

    pub fn halted_markets(&self) -> &HashSet<String> {
        &self.halted_markets
    }

    pub fn max_per_market_exposure(&self) -> f64 {
        self.cfg.max_per_market_exposure
    }

    pub fn emergency_stop(&mut self, reason: impl Into<String>) {
        self.halted = true;
        self.halt_reason = Some(reason.into());
    }

    pub fn check_pre_trade(
        &mut self,
        req: &OrderRequest,
        now: DateTime<Utc>,
        inventory_by_market: &HashMap<String, f64>,
        open_orders_by_market: &HashMap<String, (f64, f64)>, // (total bid remaining, total ask remaining)
        open_order_count: usize,
    ) -> RiskDecision {
        if self.halted {
            return RiskDecision::reject(RiskRejectReason::GlobalHalt(
                self.halt_reason
                    .clone()
                    .unwrap_or_else(|| "unknown halt".to_string()),
            ));
        }
        if self.halted_markets.contains(&req.market) {
            return RiskDecision::reject(RiskRejectReason::MarketCircuitBreak(req.market.clone()));
        }
        if open_order_count >= self.cfg.max_open_orders {
            return RiskDecision::reject(RiskRejectReason::MaxOpenOrders {
                current: open_order_count,
                limit: self.cfg.max_open_orders,
            });
        }
        self.prune_order_velocity_window(now);
        let current_per_sec = self.order_submit_times.len();
        if current_per_sec >= self.cfg.max_orders_per_sec {
            return RiskDecision::reject(RiskRejectReason::OrderVelocity {
                current_per_sec,
                limit_per_sec: self.cfg.max_orders_per_sec,
            });
        }
        let age_ms = (now - req.quote_ts).num_milliseconds();
        if age_ms > self.cfg.stale_quote_timeout_ms {
            return RiskDecision::reject(RiskRejectReason::StaleQuote {
                age_ms,
                limit_ms: self.cfg.stale_quote_timeout_ms,
            });
        }
        let current_inventory = inventory_by_market.get(&req.market).copied().unwrap_or(0.0);
        let (open_bids, open_asks) = open_orders_by_market.get(&req.market).copied().unwrap_or((0.0, 0.0));
        let future_bids = open_bids + if req.side == Side::Bid { req.size } else { 0.0 };
        let future_asks = open_asks + if req.side == Side::Ask { req.size } else { 0.0 };
        let max_long = current_inventory + future_bids;
        let max_short = current_inventory - future_asks;
        let worst_abs = f64::max(max_long.abs(), max_short.abs());
        let projected_inventory = if max_long.abs() > max_short.abs() { max_long } else { max_short };

        if worst_abs > self.cfg.max_per_market_exposure {
            return RiskDecision::reject(RiskRejectReason::ExposureLimit {
                market: req.market.clone(),
                projected: projected_inventory,
                limit: self.cfg.max_per_market_exposure,
            });
        }

        let mut projected_total = 0.0;
        let mut all_markets = std::collections::HashSet::new();
        for m in inventory_by_market.keys() { all_markets.insert(m); }
        for m in open_orders_by_market.keys() { all_markets.insert(m); }
        all_markets.insert(&req.market);

        for m in all_markets {
            let m_inv = inventory_by_market.get(m).copied().unwrap_or(0.0);
            let (m_bids, m_asks) = open_orders_by_market.get(m).copied().unwrap_or((0.0, 0.0));
            
            let m_future_bids = m_bids + if m == &req.market && req.side == Side::Bid { req.size } else { 0.0 };
            let m_future_asks = m_asks + if m == &req.market && req.side == Side::Ask { req.size } else { 0.0 };
            
            let m_max_long = m_inv + m_future_bids;
            let m_max_short = m_inv - m_future_asks;
            projected_total += f64::max(m_max_long.abs(), m_max_short.abs());
        }

        if projected_total > self.cfg.max_total_exposure {
            return RiskDecision::reject(RiskRejectReason::GlobalExposureLimit {
                projected_total,
                limit: self.cfg.max_total_exposure,
            });
        }
        self.order_submit_times.push_back(now);
        RiskDecision::allow()
    }

    pub fn check_drawdown(&mut self, current_equity: f64) {
        if current_equity > self.peak_equity {
            self.peak_equity = current_equity;
        }
        let allowed_loss = self.session_start_equity * self.cfg.global_drawdown_stop_pct;
        let drawdown = self.peak_equity - current_equity;
        if drawdown >= allowed_loss {
            self.emergency_stop(format!(
                "global drawdown stop hit: drawdown {:.4} >= allowed {:.4}",
                drawdown, allowed_loss
            ));
        }
    }

    pub fn check_heartbeat(&mut self, now: DateTime<Utc>, last_data_at: Option<DateTime<Utc>>) {
        let Some(last) = last_data_at else {
            return;
        };
        let silence = (now - last).num_milliseconds();
        if silence > self.cfg.heartbeat_timeout_ms {
            self.emergency_stop(format!(
                "market data heartbeat timeout: {}ms > {}ms",
                silence, self.cfg.heartbeat_timeout_ms
            ));
        }
    }

    pub fn on_fill(&mut self, fill: &FillEvent) {
        if fill.expected_price <= 0.0 || !fill.expected_price.is_finite() || !fill.price.is_finite() {
            return;
        }
        let slippage_bps = match fill.side {
            Side::Bid => ((fill.price - fill.expected_price) / fill.expected_price) * 10_000.0,
            Side::Ask => ((fill.expected_price - fill.price) / fill.expected_price) * 10_000.0,
        };
        if slippage_bps <= self.cfg.max_slippage_bps {
            return;
        }

        let queue = self
            .adverse_fill_times
            .entry(fill.market.clone())
            .or_default();
        queue.push_back(fill.timestamp);
        let window_start = fill.timestamp - Duration::seconds(self.cfg.circuit_window_secs);
        while let Some(front) = queue.front().copied() {
            if front < window_start {
                queue.pop_front();
            } else {
                break;
            }
        }
        if queue.len() >= self.cfg.max_adverse_fills_in_window {
            self.halted_markets.insert(fill.market.clone());
        }
    }

    fn prune_order_velocity_window(&mut self, now: DateTime<Utc>) {
        let window_start = now - Duration::seconds(1);
        while let Some(front) = self.order_submit_times.front().copied() {
            if front < window_start {
                self.order_submit_times.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;

    fn base_cfg() -> RiskConfig {
        RiskConfig {
            global_drawdown_stop_pct: 0.25,
            max_per_market_exposure: 25.0,
            max_total_exposure: 100.0,
            max_open_orders: 4,
            max_orders_per_sec: 10,
            stale_quote_timeout_ms: 1_000,
            heartbeat_timeout_ms: 5_000,
            kill_switch: false,
            max_adverse_fills_in_window: 3,
            circuit_window_secs: 30,
            max_slippage_bps: 25.0,
        }
    }

    #[test]
    fn drawdown_halts_session() {
        let mut risk = RiskEngine::new(base_cfg(), 100.0);
        risk.check_drawdown(70.0);
        assert!(risk.is_halted());
    }

    #[test]
    fn per_market_exposure_rejected() {
        let mut risk = RiskEngine::new(base_cfg(), 100.0);
        let now = Utc::now();
        let req = OrderRequest {
            market: "M1".to_string(),
            token_id: None,
            side: Side::Bid,
            price: 0.5,
            size: 10.0,
            quote_ts: now,
        };
        let mut inv = HashMap::new();
        inv.insert("M1".to_string(), 20.0);
        let decision = risk.check_pre_trade(&req, now, &inv, &HashMap::new(), 0);
        assert!(!decision.allowed);
        assert!(matches!(
            decision.reason,
            Some(RiskRejectReason::ExposureLimit { .. })
        ));
    }

    #[test]
    fn max_open_orders_rejected() {
        let mut risk = RiskEngine::new(base_cfg(), 100.0);
        let now = Utc::now();
        let req = OrderRequest {
            market: "M1".to_string(),
            token_id: None,
            side: Side::Bid,
            price: 0.5,
            size: 1.0,
            quote_ts: now,
        };
        let decision = risk.check_pre_trade(&req, now, &HashMap::new(), &HashMap::new(), 4);
        assert!(!decision.allowed);
        assert!(matches!(
            decision.reason,
            Some(RiskRejectReason::MaxOpenOrders { .. })
        ));
    }

    #[test]
    fn global_exposure_rejected() {
        let mut cfg = base_cfg();
        cfg.max_total_exposure = 21.0;
        let mut risk = RiskEngine::new(cfg, 100.0);
        let now = Utc::now();
        let req = OrderRequest {
            market: "M2".to_string(),
            token_id: None,
            side: Side::Bid,
            price: 0.5,
            size: 5.0,
            quote_ts: now,
        };
        let inv = HashMap::from([("M1".to_string(), 10.0), ("M2".to_string(), 8.0)]);
        let decision = risk.check_pre_trade(&req, now, &inv, &HashMap::new(), 0);
        assert!(!decision.allowed);
        assert!(matches!(
            decision.reason,
            Some(RiskRejectReason::GlobalExposureLimit { .. })
        ));
    }
}
