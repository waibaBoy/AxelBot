use chrono::{Duration, Utc};
use std::fs;
use uuid::Uuid;

use axelbot::{
    config::{ExecutionConfig, RiskConfig},
    execution::{ExecutionEngine, JsonlLogger, SimulatedExchangeClient},
    risk::RiskEngine,
    types::{FillEvent, Side},
};

fn risk_cfg() -> RiskConfig {
    RiskConfig {
        global_drawdown_stop_pct: 0.25,
        max_per_market_exposure: 100.0,
        max_open_orders: 500,
        stale_quote_timeout_ms: 2_000,
        heartbeat_timeout_ms: 10_000,
        kill_switch: false,
        max_adverse_fills_in_window: 3,
        circuit_window_secs: 30,
        max_slippage_bps: 100.0,
    }
}

fn execution_cfg() -> ExecutionConfig {
    ExecutionConfig {
        post_only: true,
        order_ttl_ms: 2_000,
        max_retries: 1,
        fill_fee_bps: 2.0,
    }
}

fn build_engine() -> (ExecutionEngine<SimulatedExchangeClient>, JsonlLogger) {
    let log_dir = std::env::temp_dir().join(format!("axelbot-it-{}", Uuid::new_v4()));
    fs::create_dir_all(&log_dir).expect("create temp log dir");
    let logger = JsonlLogger::new(&log_dir, "integration").expect("logger");
    let risk = RiskEngine::new(risk_cfg(), 1_000.0);
    let client = SimulatedExchangeClient::new(2.0);
    let engine = ExecutionEngine::new(client, execution_cfg(), risk, 1_000.0);
    (engine, logger)
}

#[test]
fn partial_fills_accumulate_inventory() {
    let (mut engine, mut logger) = build_engine();
    let now = Utc::now();

    let fill1 = FillEvent {
        fill_id: "partial-1".to_string(),
        order_id: "order-1".to_string(),
        market: "M1".to_string(),
        side: Side::Bid,
        price: 0.45,
        size: 2.0,
        expected_price: 0.45,
        fee_paid: 0.001,
        timestamp: now,
    };
    let fill2 = FillEvent {
        fill_id: "partial-2".to_string(),
        order_id: "order-1".to_string(),
        market: "M1".to_string(),
        side: Side::Bid,
        price: 0.45,
        size: 3.0,
        expected_price: 0.45,
        fee_paid: 0.0015,
        timestamp: now + Duration::milliseconds(20),
    };

    engine.apply_fill_event(fill1, &mut logger).expect("fill1");
    engine.apply_fill_event(fill2, &mut logger).expect("fill2");
    assert!((engine.portfolio().inventory_for("M1") - 5.0).abs() < 1e-9);
}

#[test]
fn out_of_order_fills_do_not_break_accounting() {
    let (mut engine, mut logger) = build_engine();
    let now = Utc::now();

    let newer = FillEvent {
        fill_id: "oo-2".to_string(),
        order_id: "order-a".to_string(),
        market: "M2".to_string(),
        side: Side::Bid,
        price: 0.52,
        size: 2.0,
        expected_price: 0.52,
        fee_paid: 0.002,
        timestamp: now + Duration::seconds(1),
    };
    let older = FillEvent {
        fill_id: "oo-1".to_string(),
        order_id: "order-a".to_string(),
        market: "M2".to_string(),
        side: Side::Bid,
        price: 0.51,
        size: 1.0,
        expected_price: 0.51,
        fee_paid: 0.001,
        timestamp: now,
    };

    engine.apply_fill_event(newer, &mut logger).expect("newer");
    engine.apply_fill_event(older, &mut logger).expect("older");
    assert!((engine.portfolio().inventory_for("M2") - 3.0).abs() < 1e-9);
}

#[test]
fn replayed_fill_is_ignored_by_fill_id() {
    let (mut engine, mut logger) = build_engine();
    let now = Utc::now();
    let fill = FillEvent {
        fill_id: "dup-1".to_string(),
        order_id: "order-x".to_string(),
        market: "M3".to_string(),
        side: Side::Ask,
        price: 0.60,
        size: 1.5,
        expected_price: 0.60,
        fee_paid: 0.0018,
        timestamp: now,
    };
    engine
        .apply_fill_event(fill.clone(), &mut logger)
        .expect("first fill");
    engine
        .apply_fill_event(fill, &mut logger)
        .expect("duplicate fill");
    assert_eq!(engine.metrics_snapshot().total_fills, 1);
}

#[test]
fn cancel_race_fill_is_still_accounted() {
    let (mut engine, mut logger) = build_engine();
    let now = Utc::now();
    let late_fill = FillEvent {
        fill_id: "late-fill".to_string(),
        order_id: "already-canceled-order".to_string(),
        market: "M4".to_string(),
        side: Side::Ask,
        price: 0.55,
        size: 2.0,
        expected_price: 0.55,
        fee_paid: 0.0022,
        timestamp: now,
    };
    engine
        .apply_fill_event(late_fill, &mut logger)
        .expect("late fill should apply");
    assert!((engine.portfolio().inventory_for("M4") + 2.0).abs() < 1e-9);
}
