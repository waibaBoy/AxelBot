use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::types::Side;

#[derive(Debug, Clone, Serialize)]
pub struct MarketSnapshot {
    pub market: String,
    pub bid: f64,
    pub bid_size: f64,
    pub ask: f64,
    pub ask_size: f64,
    pub mid: f64,
    pub fair_value: f64,
    pub spread_bps: f64,
    pub imbalance: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    BookUpdate {
        market: String,
        bid: f64,
        bid_size: f64,
        ask: f64,
        ask_size: f64,
        timestamp: DateTime<Utc>,
    },
    Trade {
        market: String,
        price: f64,
        size: f64,
        side: Side,
        timestamp: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Default)]
struct LocalBook {
    bid: f64,
    bid_size: f64,
    ask: f64,
    ask_size: f64,
    fair: f64,
    last_ts: Option<DateTime<Utc>>,
}

impl LocalBook {
    fn update_book(&mut self, bid: f64, bid_size: f64, ask: f64, ask_size: f64, ts: DateTime<Utc>) {
        self.bid = bid;
        self.bid_size = bid_size.max(0.0);
        self.ask = ask;
        self.ask_size = ask_size.max(0.0);
        let mid = (bid + ask) * 0.5;
        if self.fair <= 0.0 {
            self.fair = mid;
        } else {
            self.fair = 0.7 * self.fair + 0.3 * mid;
        }
        self.last_ts = Some(ts);
    }

    fn update_trade(&mut self, price: f64, _size: f64, _side: Side, ts: DateTime<Utc>) {
        if self.fair <= 0.0 {
            self.fair = price;
        } else {
            self.fair = 0.85 * self.fair + 0.15 * price;
        }
        self.last_ts = Some(ts);
    }
}

pub struct MarketDataCache {
    books: HashMap<String, LocalBook>,
    pub last_global_update: Option<DateTime<Utc>>,
}

impl MarketDataCache {
    pub fn new(markets: &[String]) -> Self {
        let mut books = HashMap::new();
        for market in markets {
            books.insert(
                market.clone(),
                LocalBook {
                    bid: 0.49,
                    bid_size: 10.0,
                    ask: 0.51,
                    ask_size: 10.0,
                    fair: 0.50,
                    last_ts: None,
                },
            );
        }
        Self {
            books,
            last_global_update: None,
        }
    }

    pub fn apply_event(&mut self, event: MarketEvent) -> Option<MarketSnapshot> {
        match event {
            MarketEvent::BookUpdate {
                market,
                bid,
                bid_size,
                ask,
                ask_size,
                timestamp,
            } => {
                let book = self.books.entry(market.clone()).or_default();
                book.update_book(bid, bid_size, ask, ask_size, timestamp);
                self.last_global_update = Some(Utc::now());
                Some(Self::to_snapshot(&market, book, timestamp))
            }
            MarketEvent::Trade {
                market,
                price,
                size,
                side,
                timestamp,
            } => {
                let book = self.books.entry(market.clone()).or_default();
                book.update_trade(price, size, side, timestamp);
                self.last_global_update = Some(Utc::now());
                if book.bid > 0.0 && book.ask > 0.0 {
                    Some(Self::to_snapshot(&market, book, timestamp))
                } else {
                    None
                }
            }
        }
    }

    fn to_snapshot(market: &str, book: &LocalBook, ts: DateTime<Utc>) -> MarketSnapshot {
        let mid = (book.bid + book.ask) * 0.5;
        let spread_bps = if mid > 0.0 {
            ((book.ask - book.bid) / mid) * 10_000.0
        } else {
            0.0
        };
        let depth_sum = book.bid_size + book.ask_size;
        let imbalance = if depth_sum > 0.0 {
            (book.bid_size - book.ask_size) / depth_sum
        } else {
            0.0
        };
        MarketSnapshot {
            market: market.to_string(),
            bid: book.bid,
            bid_size: book.bid_size,
            ask: book.ask,
            ask_size: book.ask_size,
            mid,
            fair_value: if book.fair > 0.0 { book.fair } else { mid },
            spread_bps,
            imbalance,
            timestamp: ts,
        }
    }

    pub fn mids(&self) -> HashMap<String, f64> {
        self.books
            .iter()
            .map(|(m, b)| (m.clone(), (b.bid + b.ask) * 0.5))
            .collect()
    }
}

#[async_trait]
pub trait MarketDataSource {
    async fn next_event(&mut self) -> Result<MarketEvent>;
}

pub struct SimulatedMarketDataSource {
    markets: Vec<String>,
    tick: u64,
    cursor: usize,
    now: DateTime<Utc>,
}

impl SimulatedMarketDataSource {
    pub fn new(markets: Vec<String>) -> Self {
        Self {
            markets,
            tick: 0,
            cursor: 0,
            now: Utc::now(),
        }
    }

    fn next_market(&mut self) -> String {
        let market = self.markets[self.cursor % self.markets.len()].clone();
        self.cursor = (self.cursor + 1) % self.markets.len();
        market
    }

    fn model_price(&self, market: &str) -> f64 {
        let hash = market
            .bytes()
            .fold(0_u64, |acc, b| acc.wrapping_mul(33).wrapping_add(b as u64));
        let phase = (self.tick as f64 / 37.0) + (hash % 17) as f64;
        let drift = (phase.sin() * 0.012) + (phase.cos() * 0.007);
        (0.50 + drift).clamp(0.02, 0.98)
    }
}

#[async_trait]
impl MarketDataSource for SimulatedMarketDataSource {
    async fn next_event(&mut self) -> Result<MarketEvent> {
        self.tick = self.tick.wrapping_add(1);
        self.now = self.now + Duration::milliseconds(150);
        let market = self.next_market();
        let mid = self.model_price(&market);
        let spread = 0.008 + ((self.tick % 7) as f64 * 0.0003);
        let bid = (mid - spread * 0.5).clamp(0.01, 0.99);
        let ask = (mid + spread * 0.5).clamp(0.01, 0.99);

        if self.tick % 5 == 0 {
            Ok(MarketEvent::Trade {
                market,
                price: mid,
                size: 2.0 + ((self.tick % 4) as f64),
                side: if self.tick % 2 == 0 {
                    Side::Bid
                } else {
                    Side::Ask
                },
                timestamp: self.now,
            })
        } else {
            Ok(MarketEvent::BookUpdate {
                market,
                bid,
                bid_size: 5.0 + ((self.tick % 9) as f64),
                ask,
                ask_size: 5.0 + (((self.tick + 3) % 9) as f64),
                timestamp: self.now,
            })
        }
    }
}
