use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// REST API response types (GET /markets, GET /book, POST /order, etc.)
// ---------------------------------------------------------------------------

/// A single market from GET /markets or GET /markets/{condition_id}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClobMarket {
    pub condition_id: String,
    pub question: String,
    pub description: Option<String>,
    pub tokens: Vec<ClobToken>,
    pub end_date_iso: Option<String>,
    pub active: bool,
    pub closed: bool,
    pub market_slug: Option<String>,
    pub minimum_order_size: Option<f64>,
    pub minimum_tick_size: Option<f64>,
    /// Whether this market uses the NegRisk CTF Exchange
    pub neg_risk: Option<bool>,
}

/// A token (outcome) within a market
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClobToken {
    pub token_id: String,
    pub outcome: String,
    pub price: Option<f64>,
    pub winner: Option<bool>,
}

/// Orderbook snapshot from GET /book?token_id=...
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClobOrderbook {
    pub market: Option<String>,
    pub asset_id: String,
    pub hash: Option<String>,
    pub timestamp: Option<String>,
    pub bids: Vec<ClobOrderbookLevel>,
    pub asks: Vec<ClobOrderbookLevel>,
}

/// A single price level in the orderbook
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClobOrderbookLevel {
    pub price: String,
    pub size: String,
}

impl ClobOrderbookLevel {
    pub fn price_f64(&self) -> f64 {
        self.price.parse().unwrap_or(0.0)
    }

    pub fn size_f64(&self) -> f64 {
        self.size.parse().unwrap_or(0.0)
    }
}

// ---------------------------------------------------------------------------
// Order types (POST /order, GET /orders)
// ---------------------------------------------------------------------------

/// The side of an order on the CLOB
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ClobSide {
    Buy,
    Sell,
}

/// Order type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ClobOrderType {
    Gtc,
    Fok,
    Ioc,
}

/// Payload sent to POST /order
#[derive(Debug, Clone, Serialize)]
pub struct ClobOrderRequest {
    pub order: SignedClobOrder,
    #[serde(rename = "orderType")]
    pub order_type: ClobOrderType,
    pub owner: String,
}

/// The signed order payload within a ClobOrderRequest
#[derive(Debug, Clone, Serialize)]
pub struct SignedClobOrder {
    /// Random salt for uniqueness
    pub salt: String,
    pub maker: String,
    pub signer: String,
    /// Zero address for open orders
    pub taker: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub expiration: String,
    pub nonce: String,
    #[serde(rename = "feeRateBps")]
    pub fee_rate_bps: String,
    /// 0 = Buy, 1 = Sell
    pub side: String,
    #[serde(rename = "signatureType")]
    pub signature_type: String,
    pub signature: String,
}

/// Response from POST /order
#[derive(Debug, Clone, Deserialize)]
pub struct ClobOrderResponse {
    #[serde(rename = "orderID")]
    pub order_id: Option<String>,
    pub success: Option<bool>,
    #[serde(rename = "errorMsg")]
    pub error_msg: Option<String>,
    pub status: Option<String>,
}

/// An open order from GET /orders
#[derive(Debug, Clone, Deserialize)]
pub struct ClobOpenOrder {
    pub id: String,
    pub asset_id: String,
    pub market: Option<String>,
    pub side: String,
    pub price: String,
    pub original_size: String,
    pub size_matched: String,
    pub status: String,
    pub created_at: Option<String>,
}

/// A trade/fill from GET /trades or GET /trades?market=...
#[derive(Debug, Clone, Deserialize)]
pub struct ClobTrade {
    pub id: String,
    pub market: Option<String>,
    pub asset_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub fee_rate_bps: Option<String>,
    pub status: Option<String>,
    pub match_time: Option<String>,
    #[serde(rename = "orderID")]
    pub order_id: Option<String>,
}

// ---------------------------------------------------------------------------
// WebSocket frame types
// ---------------------------------------------------------------------------

/// Top-level WebSocket message envelope
#[derive(Debug, Clone, Deserialize)]
pub struct WsMessage {
    #[serde(rename = "type")]
    pub msg_type: Option<String>,
    pub channel: Option<String>,
    pub market: Option<String>,
    pub asset_id: Option<String>,
    /// For book channel messages
    pub data: Option<serde_json::Value>,
}

/// Book delta from the "book" WebSocket channel
#[derive(Debug, Clone, Deserialize)]
pub struct WsBookDelta {
    pub asset_id: String,
    pub market: Option<String>,
    pub bids: Option<Vec<ClobOrderbookLevel>>,
    pub asks: Option<Vec<ClobOrderbookLevel>>,
    pub timestamp: Option<String>,
    pub hash: Option<String>,
}

/// Trade event from the "trade" WebSocket channel
#[derive(Debug, Clone, Deserialize)]
pub struct WsTradeEvent {
    pub asset_id: String,
    pub market: Option<String>,
    pub side: String,
    pub price: String,
    pub size: String,
    pub timestamp: Option<String>,
}

/// Subscription message sent to the WebSocket
#[derive(Debug, Clone, Serialize)]
pub struct WsSubscription {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub channel: String,
    pub assets_ids: Vec<String>,
}

impl WsSubscription {
    pub fn book(token_ids: Vec<String>) -> Self {
        Self {
            msg_type: "subscribe".to_string(),
            channel: "book".to_string(),
            assets_ids: token_ids,
        }
    }

    pub fn trade(token_ids: Vec<String>) -> Self {
        Self {
            msg_type: "subscribe".to_string(),
            channel: "trade".to_string(),
            assets_ids: token_ids,
        }
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers: CLOB types → internal MarketEvent types
// ---------------------------------------------------------------------------

use crate::types::Side;
use crate::market_data::MarketEvent;

impl WsBookDelta {
    /// Convert a book delta into a MarketEvent::BookUpdate using best bid/ask.
    pub fn to_market_event(&self) -> Option<MarketEvent> {
        let best_bid = self
            .bids
            .as_ref()
            .and_then(|b| b.first())
            .map(|l| l.price_f64())
            .unwrap_or(0.0);
        let best_ask = self
            .asks
            .as_ref()
            .and_then(|a| a.first())
            .map(|l| l.price_f64())
            .unwrap_or(1.0);

        if best_bid <= 0.0 && best_ask >= 1.0 {
            return None;
        }

        let ts = self
            .timestamp
            .as_ref()
            .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        Some(MarketEvent::BookUpdate {
            market: self.asset_id.clone(),
            bid: best_bid,
            ask: best_ask,
            timestamp: ts,
        })
    }
}

impl WsTradeEvent {
    /// Convert a WS trade event into a MarketEvent::Trade.
    pub fn to_market_event(&self) -> Option<MarketEvent> {
        let price = self.price.parse::<f64>().ok()?;
        let size = self.size.parse::<f64>().ok()?;
        let side = match self.side.to_uppercase().as_str() {
            "BUY" => Side::Bid,
            "SELL" => Side::Ask,
            _ => return None,
        };

        let ts = self
            .timestamp
            .as_ref()
            .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        Some(MarketEvent::Trade {
            market: self.asset_id.clone(),
            price,
            size,
            side,
            timestamp: ts,
        })
    }
}
