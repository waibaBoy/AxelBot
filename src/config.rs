use std::{env, fs, path::Path};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

use crate::MIN_MARKET_COUNT;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub exchange: ExchangeConfig,
    pub markets: MarketUniverseConfig,
    pub risk: RiskConfig,
    pub execution: ExecutionConfig,
    pub strategy: StrategyConfig,
    #[serde(default)]
    pub news: NewsConfig,
    pub logging: LoggingConfig,
    pub bankroll: BankrollConfig,
    #[serde(default)]
    pub polymarket: PolymarketConfig,
}

impl AppConfig {
    pub fn from_path(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file at {}", path.to_string_lossy()))?;
        let mut config: AppConfig =
            toml::from_str(&raw).context("failed to deserialize config.toml")?;
        config.apply_env_overrides();
        config.normalize();
        config.validate()?;
        Ok(config)
    }

    pub fn normalize(&mut self) {
        Self::empty_to_none(&mut self.exchange.api_key);
        Self::empty_to_none(&mut self.exchange.api_secret);
        Self::empty_to_none(&mut self.exchange.wallet_private_key);
        Self::empty_to_none(&mut self.news.guardian_api_key);
        Self::normalize_proxy_url(&mut self.exchange.proxy_url);

        if self.markets.symbols.len() < self.markets.min_markets {
            let mut idx = self.markets.symbols.len();
            while self.markets.symbols.len() < self.markets.min_markets {
                idx += 1;
                self.markets.symbols.push(format!("POLY-MKT-{idx:03}"));
            }
        }
        self.markets.symbols.sort();
        self.markets.symbols.dedup();
    }

    fn empty_to_none(slot: &mut Option<String>) {
        if slot
            .as_deref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(false)
        {
            *slot = None;
        }
    }

    fn normalize_proxy_url(slot: &mut Option<String>) {
        let normalized = slot.as_deref().map(str::trim).and_then(|raw| {
            if raw.is_empty() {
                return None;
            }
            let unquoted = raw
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .or_else(|| raw.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
                .unwrap_or(raw)
                .trim();
            if unquoted.is_empty() {
                None
            } else {
                Some(unquoted.to_string())
            }
        });
        *slot = normalized;
    }

    pub fn validate(&self) -> Result<()> {
        if self.markets.min_markets < MIN_MARKET_COUNT {
            return Err(anyhow!(
                "markets.min_markets must be at least {} for broad scan mode",
                MIN_MARKET_COUNT
            ));
        }
        if self.markets.symbols.len() < self.markets.min_markets {
            return Err(anyhow!(
                "markets.symbols has {} entries but min_markets requires {}",
                self.markets.symbols.len(),
                self.markets.min_markets
            ));
        }
        if !(0.0..1.0).contains(&self.risk.global_drawdown_stop_pct) {
            return Err(anyhow!("risk.global_drawdown_stop_pct must be in (0, 1)"));
        }
        if self.risk.max_total_exposure <= 0.0 {
            return Err(anyhow!("risk.max_total_exposure must be positive"));
        }
        if self.risk.max_orders_per_sec == 0 {
            return Err(anyhow!("risk.max_orders_per_sec must be >= 1"));
        }
        if self.strategy.target_spread_bps <= 0.0 {
            return Err(anyhow!("strategy.target_spread_bps must be positive"));
        }
        if self.strategy.quote_sizes.is_empty() {
            return Err(anyhow!("strategy.quote_sizes cannot be empty"));
        }
        if !(0.0..=1.0).contains(&self.execution.inventory_soft_limit_ratio) {
            return Err(anyhow!(
                "execution.inventory_soft_limit_ratio must be in [0, 1]"
            ));
        }
        if !(0.0..=1.0).contains(&self.execution.inventory_hard_limit_ratio) {
            return Err(anyhow!(
                "execution.inventory_hard_limit_ratio must be in [0, 1]"
            ));
        }
        if self.execution.inventory_hard_limit_ratio < self.execution.inventory_soft_limit_ratio {
            return Err(anyhow!(
                "execution.inventory_hard_limit_ratio must be >= execution.inventory_soft_limit_ratio"
            ));
        }
        if !(0.0..=1.0).contains(&self.execution.inventory_min_size_scale)
            || self.execution.inventory_min_size_scale <= 0.0
        {
            return Err(anyhow!(
                "execution.inventory_min_size_scale must be in (0, 1]"
            ));
        }
        if self.execution.inventory_flatten_boost < 1.0 {
            return Err(anyhow!(
                "execution.inventory_flatten_boost must be >= 1.0"
            ));
        }
        if self.execution.sim_fill_min_latency_ms < 0 {
            return Err(anyhow!("execution.sim_fill_min_latency_ms must be >= 0"));
        }
        if self.execution.sim_fill_max_latency_ms < self.execution.sim_fill_min_latency_ms {
            return Err(anyhow!(
                "execution.sim_fill_max_latency_ms must be >= execution.sim_fill_min_latency_ms"
            ));
        }
        if self.execution.sim_slippage_bps < 0.0 {
            return Err(anyhow!("execution.sim_slippage_bps must be non-negative"));
        }
        if self.news.poll_secs == 0 {
            return Err(anyhow!("news.poll_secs must be >= 1"));
        }
        if self.news.max_articles == 0 {
            return Err(anyhow!("news.max_articles must be >= 1"));
        }
        if self.news.max_bias_bps < 0.0 {
            return Err(anyhow!("news.max_bias_bps must be non-negative"));
        }
        if self.bankroll.starting_cash <= 0.0 {
            return Err(anyhow!("bankroll.starting_cash must be positive"));
        }
        Ok(())
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(key) = env::var("AXELBOT_API_KEY") {
            self.exchange.api_key = Some(key);
        }
        if let Ok(secret) = env::var("AXELBOT_API_SECRET") {
            self.exchange.api_secret = Some(secret);
        }
        if let Ok(pk) = env::var("AXELBOT_WALLET_PRIVATE_KEY") {
            self.exchange.wallet_private_key = Some(pk);
        }
        if let Ok(v) = env::var("AXELBOT_KILL_SWITCH") {
            self.risk.kill_switch = matches!(v.to_lowercase().as_str(), "1" | "true" | "yes");
        }
        if let Ok(v) = env::var("AXELBOT_PROXY_URL") {
            if !v.trim().is_empty() {
                self.exchange.proxy_url = Some(v);
            }
        }
        if let Ok(v) = env::var("AXELBOT_NEWS_ENABLED") {
            self.news.enabled = matches!(v.to_lowercase().as_str(), "1" | "true" | "yes");
        }
        if let Ok(v) = env::var("AXELBOT_GUARDIAN_API_KEY") {
            self.news.guardian_api_key = Some(v);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub rest_url: String,
    pub ws_url: String,
    pub network: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub wallet_address: String,
    pub wallet_private_key: Option<String>,
    #[serde(default)]
    pub proxy_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketUniverseConfig {
    pub symbols: Vec<String>,
    pub min_markets: usize,
    pub refresh_secs: u64,
    pub selection_mode: String,
    #[serde(default = "default_book_probe_timeout_ms")]
    pub book_probe_timeout_ms: u64,
    #[serde(default = "default_rest_poll_timeout_ms")]
    pub rest_poll_timeout_ms: u64,
    #[serde(default = "default_data_event_timeout_ms")]
    pub data_event_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    pub global_drawdown_stop_pct: f64,
    pub max_per_market_exposure: f64,
    #[serde(default = "default_max_total_exposure")]
    pub max_total_exposure: f64,
    pub max_open_orders: usize,
    #[serde(default = "default_max_orders_per_sec")]
    pub max_orders_per_sec: usize,
    pub stale_quote_timeout_ms: i64,
    pub heartbeat_timeout_ms: i64,
    pub kill_switch: bool,
    pub max_adverse_fills_in_window: usize,
    pub circuit_window_secs: i64,
    pub max_slippage_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UnrecognizedFillPolicy {
    Apply,
    Quarantine,
    Drop,
}

impl Default for UnrecognizedFillPolicy {
    fn default() -> Self {
        Self::Quarantine
    }
}

fn default_unrecognized_fill_policy() -> UnrecognizedFillPolicy {
    UnrecognizedFillPolicy::default()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_unrecognized_fill_policy")]
    pub unrecognized_fill_policy: UnrecognizedFillPolicy,
    pub post_only: bool,
    pub order_ttl_ms: i64,
    pub max_retries: usize,
    pub fill_fee_bps: f64,
    #[serde(default = "default_inventory_soft_limit_ratio")]
    pub inventory_soft_limit_ratio: f64,
    #[serde(default = "default_inventory_hard_limit_ratio")]
    pub inventory_hard_limit_ratio: f64,
    #[serde(default = "default_inventory_min_size_scale")]
    pub inventory_min_size_scale: f64,
    #[serde(default = "default_inventory_flatten_boost")]
    pub inventory_flatten_boost: f64,
    #[serde(default = "default_sim_fill_min_latency_ms")]
    pub sim_fill_min_latency_ms: i64,
    #[serde(default = "default_sim_fill_max_latency_ms")]
    pub sim_fill_max_latency_ms: i64,
    #[serde(default = "default_sim_slippage_bps")]
    pub sim_slippage_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub target_spread_bps: f64,
    pub inventory_skew_bps: f64,
    pub quote_sizes: Vec<f64>,
    pub refresh_interval_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsConfig {
    pub enabled: bool,
    #[serde(default = "default_guardian_endpoint")]
    pub guardian_endpoint: String,
    #[serde(default)]
    pub guardian_api_key: Option<String>,
    #[serde(default = "default_news_query")]
    pub query: String,
    #[serde(default = "default_news_poll_secs")]
    pub poll_secs: u64,
    #[serde(default = "default_news_lookback_minutes")]
    pub lookback_minutes: i64,
    #[serde(default = "default_news_max_articles")]
    pub max_articles: usize,
    #[serde(default = "default_news_max_bias_bps")]
    pub max_bias_bps: f64,
}

impl Default for NewsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            guardian_endpoint: default_guardian_endpoint(),
            guardian_api_key: None,
            query: default_news_query(),
            poll_secs: default_news_poll_secs(),
            lookback_minutes: default_news_lookback_minutes(),
            max_articles: default_news_max_articles(),
            max_bias_bps: default_news_max_bias_bps(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub output_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankrollConfig {
    pub starting_cash: f64,
    pub live_tranche_cash: f64,
}

/// Polymarket-specific chain and contract configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketConfig {
    /// Polygon chain ID (137 for mainnet, 80002 for Amoy testnet)
    pub chain_id: u64,
    /// CTF Exchange contract address
    pub ctf_exchange_address: String,
    /// NegRisk CTF Exchange contract address (for multi-outcome markets)
    pub neg_risk_ctf_exchange_address: String,
    /// USDC collateral token address on Polygon
    pub collateral_token_address: String,
}

impl Default for PolymarketConfig {
    fn default() -> Self {
        Self {
            chain_id: 137,
            ctf_exchange_address: "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E".to_string(),
            neg_risk_ctf_exchange_address: "0xC5d563A36AE78145C45a50134d48A1215220f80a".to_string(),
            collateral_token_address: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".to_string(),
        }
    }
}

fn default_max_total_exposure() -> f64 {
    1_000_000_000.0
}

fn default_max_orders_per_sec() -> usize {
    25
}

fn default_guardian_endpoint() -> String {
    "https://content.guardianapis.com/search".to_string()
}

fn default_inventory_soft_limit_ratio() -> f64 {
    0.60
}

fn default_inventory_hard_limit_ratio() -> f64 {
    0.90
}

fn default_inventory_min_size_scale() -> f64 {
    0.15
}

fn default_inventory_flatten_boost() -> f64 {
    1.75
}

fn default_sim_fill_min_latency_ms() -> i64 {
    80
}

fn default_sim_fill_max_latency_ms() -> i64 {
    350
}

fn default_sim_slippage_bps() -> f64 {
    1.5
}

fn default_news_query() -> String {
    "election OR inflation OR war OR sanctions OR fed OR economy OR earnings OR injury".to_string()
}

fn default_news_poll_secs() -> u64 {
    30
}

fn default_news_lookback_minutes() -> i64 {
    180
}

fn default_news_max_articles() -> usize {
    25
}

fn default_news_max_bias_bps() -> f64 {
    12.0
}

fn default_book_probe_timeout_ms() -> u64 {
    5_000
}

fn default_rest_poll_timeout_ms() -> u64 {
    5_000
}

fn default_data_event_timeout_ms() -> u64 {
    30_000
}
