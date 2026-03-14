use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;

use crate::config::NewsConfig;
use crate::polymarket::http::build_http_client;

pub struct NewsOverlay {
    cfg: NewsConfig,
    http: reqwest::Client,
}

fn resilient_http_client(proxy_url: Option<&str>) -> reqwest::Client {
    match build_http_client(proxy_url) {
        Ok(client) => client,
        Err(err) => {
            eprintln!(
                "warning: failed to build news HTTP client with proxy ({:?}): {}. falling back to direct client",
                proxy_url, err
            );
            // Keep fallback deterministic: don't auto-read ambient *_PROXY env vars.
            build_http_client(None).unwrap_or_else(|_| {
                reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .unwrap_or_else(|_| reqwest::Client::new())
            })
        }
    }
}

impl NewsOverlay {
    pub fn new(cfg: NewsConfig, proxy_url: Option<&str>) -> Self {
        Self {
            http: resilient_http_client(proxy_url),
            cfg,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.cfg.enabled && self.cfg.guardian_api_key.is_some()
    }

    pub async fn refresh_market_biases(
        &self,
        market_texts: &HashMap<String, String>,
    ) -> Result<HashMap<String, f64>> {
        if !self.is_enabled() || market_texts.is_empty() {
            return Ok(HashMap::new());
        }
        let Some(api_key) = self.cfg.guardian_api_key.as_deref() else {
            return Ok(HashMap::new());
        };
        let from_date = (Utc::now() - Duration::minutes(self.cfg.lookback_minutes.max(1)))
            .format("%Y-%m-%d")
            .to_string();
        let page_size = self.cfg.max_articles.clamp(1, 50).to_string();
        let resp = self
            .http
            .get(&self.cfg.guardian_endpoint)
            .query(&[
                ("api-key", api_key),
                ("q", self.cfg.query.as_str()),
                ("show-fields", "headline,trailText,bodyText,lastModified"),
                ("order-by", "newest"),
                ("from-date", from_date.as_str()),
                ("page-size", page_size.as_str()),
            ])
            .send()
            .await?;

        if !resp.status().is_success() {
            return Ok(HashMap::new());
        }

        let body: GuardianEnvelope = match resp.json().await {
            Ok(v) => v,
            Err(_) => return Ok(HashMap::new()),
        };
        let articles = body.response.results;
        if articles.is_empty() {
            return Ok(HashMap::new());
        }

        let market_tokens: HashMap<String, HashSet<String>> = market_texts
            .iter()
            .map(|(m, txt)| (m.clone(), tokenize(txt)))
            .collect();

        let mut raw_scores: HashMap<String, f64> = HashMap::new();
        for article in articles {
            let article_text = format!(
                "{} {} {} {}",
                article.web_title,
                article
                    .fields
                    .as_ref()
                    .and_then(|f| f.headline.clone())
                    .unwrap_or_default(),
                article
                    .fields
                    .as_ref()
                    .and_then(|f| f.trail_text.clone())
                    .unwrap_or_default(),
                article
                    .fields
                    .as_ref()
                    .and_then(|f| f.body_text.clone())
                    .unwrap_or_default(),
            );
            let article_tokens = tokenize(&article_text);
            if article_tokens.is_empty() {
                continue;
            }
            let sentiment = sentiment_score(&article_tokens);
            if sentiment.abs() < 0.05 {
                continue;
            }
            let recency = recency_weight(article.web_publication_date.as_deref());
            for (market, m_tokens) in &market_tokens {
                let relevance = overlap_score(m_tokens, &article_tokens);
                if relevance < 0.06 {
                    continue;
                }
                let score = relevance * sentiment * recency;
                *raw_scores.entry(market.clone()).or_insert(0.0) += score;
            }
        }

        let scaled = raw_scores
            .into_iter()
            .map(|(m, s)| {
                let bps = (s * self.cfg.max_bias_bps)
                    .clamp(-self.cfg.max_bias_bps, self.cfg.max_bias_bps);
                (m, bps)
            })
            .collect::<HashMap<_, _>>();
        Ok(scaled)
    }
}

fn tokenize(input: &str) -> HashSet<String> {
    input
        .to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter_map(|s| {
            if s.len() < 3 || STOPWORDS.contains(&s) {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect()
}

fn overlap_score(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let inter = a.intersection(b).count() as f64;
    inter / (a.len().max(1) as f64)
}

fn sentiment_score(tokens: &HashSet<String>) -> f64 {
    let pos = tokens
        .iter()
        .filter(|t| POSITIVE_WORDS.contains(&t.as_str()))
        .count() as f64;
    let neg = tokens
        .iter()
        .filter(|t| NEGATIVE_WORDS.contains(&t.as_str()))
        .count() as f64;
    if (pos + neg).abs() < f64::EPSILON {
        0.0
    } else {
        (pos - neg) / (pos + neg)
    }
}

fn recency_weight(ts: Option<&str>) -> f64 {
    let Some(raw) = ts else {
        return 1.0;
    };
    let Ok(dt) = DateTime::parse_from_rfc3339(raw) else {
        return 1.0;
    };
    let age_hours = (Utc::now() - dt.with_timezone(&Utc)).num_minutes().max(0) as f64 / 60.0;
    (1.0 / (1.0 + age_hours / 6.0)).clamp(0.2, 1.0)
}

const STOPWORDS: &[&str] = &[
    "the", "and", "for", "with", "from", "that", "this", "are", "was", "were", "has", "have",
    "had", "its", "not", "you", "your", "our", "their", "his", "her", "into", "about", "after",
    "before", "will", "would", "could", "should", "over", "under", "game", "match", "team",
];

const POSITIVE_WORDS: &[&str] = &[
    "win",
    "winning",
    "beat",
    "surge",
    "up",
    "rise",
    "gains",
    "growth",
    "strong",
    "approve",
    "approved",
    "bullish",
    "favorable",
];

const NEGATIVE_WORDS: &[&str] = &[
    "lose", "losing", "drop", "down", "fall", "decline", "weak", "ban", "banned", "lawsuit",
    "injury", "bearish", "risk", "warning",
];

#[derive(Debug, Deserialize)]
struct GuardianEnvelope {
    response: GuardianResponse,
}

#[derive(Debug, Deserialize)]
struct GuardianResponse {
    #[serde(default)]
    results: Vec<GuardianArticle>,
}

#[derive(Debug, Deserialize)]
struct GuardianArticle {
    #[serde(rename = "webTitle", default)]
    web_title: String,
    #[serde(rename = "webPublicationDate")]
    web_publication_date: Option<String>,
    fields: Option<GuardianFields>,
}

#[derive(Debug, Deserialize)]
struct GuardianFields {
    headline: Option<String>,
    #[serde(rename = "trailText")]
    trail_text: Option<String>,
    #[serde(rename = "bodyText")]
    body_text: Option<String>,
}
