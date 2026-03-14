#!/usr/bin/env bash
set -uo pipefail

# Batch paper-live data collection for AxelBot.
# Run with: nohup bash scripts/ec2-batch-paper.sh > batch.log 2>&1 &
#
# Survives SSH disconnect. Check progress:
#   tail -f batch.log
#   cat logs/batch-summary.txt

SESSIONS="${1:-200}"
TICKS="${2:-300}"
CONFIG="${3:-config.toml}"
SUMMARY="logs/batch-summary.txt"
BINARY="target/release/axelbot"

mkdir -p logs

if [ ! -f "$BINARY" ]; then
    echo "Release binary not found. Building..."
    cargo build --release 2>&1
fi

echo "=== Batch paper-live: $SESSIONS sessions x $TICKS ticks ===" | tee "$SUMMARY"
echo "Started: $(date -u '+%Y-%m-%dT%H:%M:%SZ')" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"

succeeded=0
failed=0
total_ticks=0
total_features=0

for i in $(seq 1 "$SESSIONS"); do
    echo "--- Run $i/$SESSIONS ($(date -u '+%H:%M:%S')) ---"

    output=$(./"$BINARY" paper-live --config "$CONFIG" --ticks "$TICKS" 2>&1)
    exit_code=$?

    if [ $exit_code -eq 0 ]; then
        ticks_done=$(echo "$output" | grep -o '"ticks_executed":[0-9]*' | grep -o '[0-9]*' || echo "0")
        log_path=$(echo "$output" | grep -o '"log_path":"[^"]*"' | sed 's/"log_path":"//;s/"//' || echo "")
        total_ticks=$((total_ticks + ticks_done))

        if [ -n "$log_path" ] && [ -f "$log_path" ]; then
            feat=$(grep -c '"event":"feature_sample"' "$log_path" 2>/dev/null || echo "0")
            total_features=$((total_features + feat))
            echo "  OK: $ticks_done ticks, $feat features -> $log_path"
        else
            echo "  OK: $ticks_done ticks -> $log_path"
        fi
        succeeded=$((succeeded + 1))
    else
        echo "  FAIL (exit $exit_code)"
        failed=$((failed + 1))
        sleep 2
    fi

    if [ $((i % 10)) -eq 0 ]; then
        echo "" | tee -a "$SUMMARY"
        echo "Progress $i/$SESSIONS: ok=$succeeded fail=$failed ticks=$total_ticks features=$total_features" | tee -a "$SUMMARY"
    fi
done

echo "" | tee -a "$SUMMARY"
echo "=== Batch complete ===" | tee -a "$SUMMARY"
echo "Finished: $(date -u '+%Y-%m-%dT%H:%M:%SZ')" | tee -a "$SUMMARY"
echo "Sessions: $succeeded OK / $failed FAILED / $SESSIONS total" | tee -a "$SUMMARY"
echo "Total ticks: $total_ticks" | tee -a "$SUMMARY"
echo "Total feature_samples: $total_features" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"
echo "Next step: pull logs back to local machine and export dataset:" | tee -a "$SUMMARY"
echo "  scp -r ubuntu@\$(hostname -I | awk '{print \$1}'):~/AxelBot/logs/ ./logs/" | tee -a "$SUMMARY"
echo "  python scripts/export_feature_dataset.py --input 'logs/paper-live-*.jsonl' --output 'data/feature_dataset.csv' --horizon-events 3 --threshold-bps 2.0 --max-book-spread 0.50" | tee -a "$SUMMARY"
