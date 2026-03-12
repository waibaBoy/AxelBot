use std::{
    collections::VecDeque,
    convert::Infallible,
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use anyhow::Result;
use async_stream::stream;
use axum::{
    extract::State,
    response::{
        sse::{Event, Sse},
        Html,
    },
    routing::get,
    Json, Router,
};
use serde::Serialize;
use tokio::time::{self, Duration};

#[derive(Clone)]
struct DashboardState {
    log_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct DashboardSnapshot {
    pub mode: Option<String>,
    pub halted: Option<bool>,
    pub halt_reason: Option<String>,
    pub metrics: Option<serde_json::Value>,
    pub log_file: Option<String>,
    pub recent_events: Vec<DashboardEvent>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardEvent {
    pub ts: String,
    pub event: String,
}

pub async fn run_dashboard(log_dir: PathBuf, port: u16) -> Result<()> {
    let state = DashboardState { log_dir };
    let app = Router::new()
        .route("/", get(index))
        .route("/api/snapshot", get(snapshot))
        .route("/api/stream", get(stream_updates))
        .with_state(state);

    let bind_addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    println!("dashboard running at http://{bind_addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn snapshot(State(state): State<DashboardState>) -> Json<DashboardSnapshot> {
    Json(load_dashboard_snapshot(&state.log_dir))
}

async fn stream_updates(
    State(state): State<DashboardState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let stream = stream! {
        let mut ticker = time::interval(Duration::from_millis(300));
        loop {
            ticker.tick().await;
            let snap = load_dashboard_snapshot(&state.log_dir);
            let payload = serde_json::to_string(&snap).unwrap_or_else(|_| "{\"error\":\"serialize\"}".to_string());
            yield Ok(Event::default().event("snapshot").data(payload));
        }
    };
    Sse::new(stream)
}

fn load_dashboard_snapshot(log_dir: &Path) -> DashboardSnapshot {
    let latest_log = match latest_log_file(log_dir) {
        Some(p) => p,
        None => {
            return DashboardSnapshot {
                error: Some(format!(
                    "No .jsonl logs found in {}. Run paper/backtest/live first.",
                    log_dir.display()
                )),
                ..DashboardSnapshot::default()
            }
        }
    };

    match parse_snapshot_from_log(&latest_log) {
        Ok(mut snap) => {
            snap.log_file = latest_log
                .file_name()
                .map(|n| n.to_string_lossy().to_string());
            snap
        }
        Err(err) => DashboardSnapshot {
            log_file: latest_log
                .file_name()
                .map(|n| n.to_string_lossy().to_string()),
            error: Some(err.to_string()),
            ..DashboardSnapshot::default()
        },
    }
}

fn latest_log_file(log_dir: &Path) -> Option<PathBuf> {
    let entries = fs::read_dir(log_dir).ok()?;
    let mut logs: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "jsonl").unwrap_or(false))
        .collect();
    logs.sort_by_key(|p| fs::metadata(p).and_then(|m| m.modified()).ok());
    logs.pop()
}

fn parse_snapshot_from_log(path: &Path) -> Result<DashboardSnapshot> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);

    let mut mode: Option<String> = None;
    let mut halted: Option<bool> = None;
    let mut halt_reason: Option<String> = None;
    let mut metrics: Option<serde_json::Value> = None;
    let mut recent: VecDeque<DashboardEvent> = VecDeque::with_capacity(120);

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let event = row
            .get("event")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let ts = row
            .get("ts")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if event == "session_bootstrap" {
            mode = row
                .get("payload")
                .and_then(|p| p.get("mode"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }
        if event == "metrics" {
            metrics = row.get("payload").cloned();
        }
        if event.ends_with("_report") {
            mode = row
                .get("payload")
                .and_then(|p| p.get("mode"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or(mode);
            halted = row
                .get("payload")
                .and_then(|p| p.get("halted"))
                .and_then(|v| v.as_bool());
            halt_reason = row
                .get("payload")
                .and_then(|p| p.get("halt_reason"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
        }

        recent.push_back(DashboardEvent { ts, event });
        if recent.len() > 80 {
            recent.pop_front();
        }
    }

    Ok(DashboardSnapshot {
        mode,
        halted,
        halt_reason,
        metrics,
        log_file: None,
        recent_events: recent.into_iter().collect(),
        error: None,
    })
}

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>AxelBot Dashboard</title>
  <style>
    :root {
      --bg-a: #f8f6ef;
      --bg-b: #e9f2ef;
      --ink: #14221f;
      --muted: #5f6f68;
      --card: rgba(255,255,255,0.78);
      --line: rgba(20,34,31,0.12);
      --accent: #0f8b6d;
      --danger: #bf2f45;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 18% 12%, rgba(15,139,109,0.22), transparent 33%),
        radial-gradient(circle at 80% 78%, rgba(216,120,45,0.22), transparent 32%),
        linear-gradient(145deg, var(--bg-a), var(--bg-b));
      min-height: 100vh;
      padding: 28px;
    }
    .wrap { max-width: 1220px; margin: 0 auto; }
    .head {
      display: flex; justify-content: space-between; align-items: flex-start;
      gap: 18px; margin-bottom: 16px;
    }
    .title {
      font-size: clamp(1.35rem, 2.2vw, 2rem);
      margin: 0 0 6px;
      letter-spacing: -0.02em;
    }
    .sub { margin: 0; color: var(--muted); font-size: .95rem; }
    .status {
      border: 1px solid var(--line);
      background: var(--card);
      border-radius: 14px;
      padding: 12px 14px;
      min-width: 250px;
      backdrop-filter: blur(4px);
    }
    .dot {
      width: 10px; height: 10px; border-radius: 99px; display: inline-block; margin-right: 8px;
      background: var(--accent);
      box-shadow: 0 0 0 0 rgba(15,139,109,.5);
      animation: pulse 1.6s infinite;
    }
    .dot.halt { background: var(--danger); box-shadow: none; animation: none; }
    @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(15,139,109,.5); } 70% { box-shadow: 0 0 0 10px rgba(15,139,109,0); } 100% { box-shadow: 0 0 0 0 rgba(15,139,109,0);} }
    .grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(130px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px 13px;
      backdrop-filter: blur(4px);
    }
    .k { font-size: .72rem; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; margin-bottom: 6px; }
    .v { font-size: 1.18rem; font-weight: 700; }
    .v.neg { color: var(--danger); }
    .cols {
      display: grid;
      grid-template-columns: 1.1fr .9fr;
      gap: 12px;
    }
    .panel-title { margin: 0 0 10px; font-size: .86rem; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); }
    table { width: 100%; border-collapse: collapse; font-size: .9rem; }
    th, td { text-align: left; padding: 7px 6px; border-bottom: 1px solid var(--line); }
    th { color: var(--muted); font-weight: 600; font-size: .75rem; text-transform: uppercase; letter-spacing: .07em; }
    .events { max-height: 360px; overflow: auto; font-family: "IBM Plex Mono", Consolas, monospace; font-size: .8rem; }
    .evt { display: grid; grid-template-columns: 1fr auto; gap: 8px; padding: 6px 2px; border-bottom: 1px dashed var(--line); }
    .mono { font-family: "IBM Plex Mono", Consolas, monospace; }
    @media (max-width: 980px) {
      .grid { grid-template-columns: repeat(2, minmax(140px, 1fr)); }
      .cols { grid-template-columns: 1fr; }
      body { padding: 14px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="head">
      <div>
        <h1 class="title">AxelBot Control Surface</h1>
        <p class="sub">Live metrics stream from your latest JSONL run log.</p>
      </div>
      <div class="status">
        <div><span id="dot" class="dot"></span><strong id="state">Running</strong></div>
        <div class="sub mono" id="mode">mode: -</div>
        <div class="sub mono" id="log">log: -</div>
        <div class="sub mono" id="reason"></div>
      </div>
    </div>

    <div class="grid">
      <div class="card"><div class="k">Realized PnL</div><div class="v" id="realized">-</div></div>
      <div class="card"><div class="k">Unrealized PnL</div><div class="v" id="unrealized">-</div></div>
      <div class="card"><div class="k">Order Latency</div><div class="v" id="olat">-</div></div>
      <div class="card"><div class="k">Fill Latency</div><div class="v" id="flat">-</div></div>
      <div class="card"><div class="k">Slippage</div><div class="v" id="slip">-</div></div>
      <div class="card"><div class="k">Quote Uptime</div><div class="v" id="uptime">-</div></div>
    </div>

    <div class="cols">
      <div class="card">
        <h2 class="panel-title">Inventory by Market</h2>
        <table>
          <thead><tr><th>Market</th><th>Qty</th></tr></thead>
          <tbody id="inv"></tbody>
        </table>
      </div>
      <div class="card">
        <h2 class="panel-title">Recent Events</h2>
        <div id="events" class="events"></div>
      </div>
    </div>
  </div>

  <script>
    const ids = {
      dot: document.getElementById('dot'),
      state: document.getElementById('state'),
      mode: document.getElementById('mode'),
      log: document.getElementById('log'),
      reason: document.getElementById('reason'),
      realized: document.getElementById('realized'),
      unrealized: document.getElementById('unrealized'),
      olat: document.getElementById('olat'),
      flat: document.getElementById('flat'),
      slip: document.getElementById('slip'),
      uptime: document.getElementById('uptime'),
      inv: document.getElementById('inv'),
      events: document.getElementById('events'),
    };

    const fmt = (n, d=4) => Number.isFinite(n) ? n.toFixed(d) : '-';
    const fmtPct = (n) => Number.isFinite(n) ? (n * 100).toFixed(2) + '%' : '-';

    function setSigned(el, value, digits=4) {
      const n = Number(value);
      el.textContent = fmt(n, digits);
      el.classList.toggle('neg', n < 0);
    }

    function render(snapshot) {
      if (snapshot.error) {
        ids.state.textContent = 'Waiting for logs';
        ids.reason.textContent = snapshot.error;
        ids.dot.classList.add('halt');
      }
      const halted = snapshot.halted === true;
      ids.state.textContent = halted ? 'Halted' : 'Running';
      ids.dot.classList.toggle('halt', halted);
      ids.mode.textContent = 'mode: ' + (snapshot.mode || '-');
      ids.log.textContent = 'log: ' + (snapshot.log_file || '-');
      ids.reason.textContent = snapshot.halt_reason ? ('reason: ' + snapshot.halt_reason) : '';

      const m = snapshot.metrics || {};
      setSigned(ids.realized, m.realized_pnl, 5);
      setSigned(ids.unrealized, m.unrealized_pnl, 5);
      ids.olat.textContent = fmt(Number(m.avg_order_latency_ms), 2) + ' ms';
      ids.flat.textContent = fmt(Number(m.avg_fill_latency_ms), 2) + ' ms';
      ids.slip.textContent = fmt(Number(m.avg_slippage_bps), 3) + ' bps';
      ids.uptime.textContent = fmtPct(Number(m.quote_uptime_ratio));

      const inv = Object.entries(m.inventory_by_market || {})
        .sort((a,b) => Math.abs(b[1]) - Math.abs(a[1]));
      ids.inv.innerHTML = inv.map(([k,v]) => `<tr><td class="mono">${k}</td><td class="${v < 0 ? 'neg' : ''} mono">${fmt(Number(v), 4)}</td></tr>`).join('');
      if (!inv.length) ids.inv.innerHTML = '<tr><td colspan="2" class="sub">No inventory yet</td></tr>';

      const ev = (snapshot.recent_events || []).slice(-60);
      ids.events.innerHTML = ev.map(e => `<div class="evt"><span class="mono">${e.event}</span><span class="mono">${(e.ts || '').replace('T',' ').replace('Z','')}</span></div>`).join('');
    }

    async function prime() {
      try {
        const res = await fetch('/api/snapshot', { cache: 'no-store' });
        render(await res.json());
      } catch {}
    }

    prime();
    const es = new EventSource('/api/stream');
    es.addEventListener('snapshot', (evt) => {
      try { render(JSON.parse(evt.data)); } catch {}
    });
  </script>
</body>
</html>
"#;
