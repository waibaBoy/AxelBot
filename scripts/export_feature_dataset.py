#!/usr/bin/env python3
"""
Build a supervised learning dataset from `feature_sample` events in AxelBot JSONL logs.

Example:
  python scripts/export_feature_dataset.py \
    --input "logs/paper-live-*.jsonl" \
    --output data/feature_dataset.csv \
    --horizon-events 3 \
    --threshold-bps 2.0 \
    --max-book-spread 0.50
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List
from datetime import datetime


@dataclass
class FeatureRow:
    source_file: str
    ts: str
    market: str
    mid: float
    micro_price: float
    fair_value: float
    spread_bps: float
    imbalance: float
    order_flow_signal: float
    alpha_bps: float
    inventory: float
    news_bias_bps: float
    quote_bid_price: float
    quote_ask_price: float
    quote_bid_size: float
    quote_ask_size: float
    market_bid: float
    market_ask: float


def safe_float(v, default=0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def parse_feature_rows(path: str) -> List[FeatureRow]:
    rows: List[FeatureRow] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("event") != "feature_sample":
                continue
            payload = obj.get("payload") or {}
            market = payload.get("market")
            mid = safe_float(payload.get("mid"), default=-1.0)
            if not market or mid <= 0.0:
                continue
            rows.append(
                FeatureRow(
                    source_file=os.path.basename(path),
                    ts=str(payload.get("captured_at") or obj.get("ts") or ""),
                    market=str(market),
                    mid=mid,
                    micro_price=safe_float(payload.get("micro_price"), mid),
                    fair_value=safe_float(payload.get("fair_value"), mid),
                    spread_bps=safe_float(payload.get("spread_bps")),
                    imbalance=safe_float(payload.get("imbalance")),
                    order_flow_signal=safe_float(payload.get("order_flow_signal")),
                    alpha_bps=safe_float(payload.get("alpha_bps")),
                    inventory=safe_float(payload.get("inventory")),
                    news_bias_bps=safe_float(payload.get("news_bias_bps")),
                    quote_bid_price=safe_float(payload.get("quote_bid_price")),
                    quote_ask_price=safe_float(payload.get("quote_ask_price")),
                    quote_bid_size=safe_float(payload.get("quote_bid_size")),
                    quote_ask_size=safe_float(payload.get("quote_ask_size")),
                    market_bid=safe_float(payload.get("market_bid")),
                    market_ask=safe_float(payload.get("market_ask")),
                )
            )
    return rows


def build_dataset(
    rows: List[FeatureRow],
    horizon_events: int,
    threshold_bps: float,
    include_neutral: bool,
    max_book_spread: float,
    max_time_gap_seconds: float,
) -> List[Dict[str, float]]:
    grouped: Dict[str, List[FeatureRow]] = defaultdict(list)
    for r in rows:
        grouped[r.market].append(r)

    out: List[Dict[str, float]] = []
    for market, mrows in grouped.items():
        if len(mrows) <= horizon_events:
            continue
        for i in range(len(mrows) - horizon_events):
            cur = mrows[i]
            fut = mrows[i + horizon_events]
            if cur.mid <= 0.0 or fut.mid <= 0.0:
                continue

            # Prevent empty-book artifacts from generating fake labels
            cur_spread = cur.market_ask - cur.market_bid
            fut_spread = fut.market_ask - fut.market_bid
            if cur_spread > max_book_spread or fut_spread > max_book_spread:
                continue

            try:
                cur_dt = datetime.fromisoformat(cur.ts.replace('Z', '+00:00'))
                fut_dt = datetime.fromisoformat(fut.ts.replace('Z', '+00:00'))
                gap_seconds = abs((fut_dt - cur_dt).total_seconds())
                if gap_seconds > max_time_gap_seconds:
                    continue
            except ValueError:
                pass

            fwd_bps = ((fut.mid / cur.mid) - 1.0) * 10_000.0
            if fwd_bps > threshold_bps:
                label = 1
            elif fwd_bps < -threshold_bps:
                label = -1
            else:
                label = 0

            if not include_neutral and label == 0:
                continue

            out.append(
                {
                    "source_file": cur.source_file,
                    "ts": cur.ts,
                    "market": market,
                    "mid": cur.mid,
                    "micro_price": cur.micro_price,
                    "fair_value": cur.fair_value,
                    "spread_bps": cur.spread_bps,
                    "imbalance": cur.imbalance,
                    "order_flow_signal": cur.order_flow_signal,
                    "alpha_bps": cur.alpha_bps,
                    "inventory": cur.inventory,
                    "news_bias_bps": cur.news_bias_bps,
                    "quote_bid_price": cur.quote_bid_price,
                    "quote_ask_price": cur.quote_ask_price,
                    "quote_bid_size": cur.quote_bid_size,
                    "quote_ask_size": cur.quote_ask_size,
                    "market_bid": cur.market_bid,
                    "market_ask": cur.market_ask,
                    "future_mid": fut.mid,
                    "fwd_ret_bps": fwd_bps,
                    "label": label,
                }
            )
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Glob path, e.g. logs/paper-live-*.jsonl")
    ap.add_argument("--output", required=True, help="CSV output path")
    ap.add_argument(
        "--horizon-events",
        type=int,
        default=3,
        help="Forward label horizon in feature-sample events (default: 3)",
    )
    ap.add_argument(
        "--threshold-bps",
        type=float,
        default=2.0,
        help="Neutral zone threshold in bps (default: 2.0)",
    )
    ap.add_argument(
        "--include-neutral",
        action="store_true",
        help="Include label=0 rows in output",
    )
    ap.add_argument(
        "--max-book-spread",
        type=float,
        default=0.50,
        help="Drop samples where current/future market spread exceeds this absolute price width (default: 0.50)",
    )
    ap.add_argument(
        "--max-time-gap-seconds",
        type=float,
        default=3600.0,
        help="Drop samples where time gap between cur and fut exceeds this (default: 3600)",
    )
    args = ap.parse_args()

    if args.horizon_events < 1:
        raise SystemExit("--horizon-events must be >= 1")

    files = sorted(glob.glob(args.input))
    if not files:
        raise SystemExit(f"No files matched: {args.input}")

    rows: List[FeatureRow] = []
    for p in files:
        rows.extend(parse_feature_rows(p))
    if not rows:
        raise SystemExit("No feature_sample events found. Run paper-live with updated bot first.")

    ds = build_dataset(
        rows,
        horizon_events=args.horizon_events,
        threshold_bps=args.threshold_bps,
        include_neutral=args.include_neutral,
        max_book_spread=args.max_book_spread,
        max_time_gap_seconds=args.max_time_gap_seconds,
    )
    if not ds:
        raise SystemExit("Dataset empty after labeling. Increase logs or adjust threshold/horizon.")

    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    fieldnames = list(ds[0].keys())
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(ds)

    labels = defaultdict(int)
    for r in ds:
        labels[int(r["label"])] += 1

    print(f"files={len(files)} feature_rows={len(rows)} dataset_rows={len(ds)}")
    print(f"label_counts={dict(sorted(labels.items()))}")
    print(f"output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
