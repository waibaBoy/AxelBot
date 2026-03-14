#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$HOME/AxelBot"

echo "=== AxelBot EC2 Bootstrap ==="
echo "Region: $(curl -s http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null || echo 'unknown')"

# System deps
echo "--- Installing system dependencies ---"
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential pkg-config libssl-dev git

# Rust toolchain
if ! command -v cargo &>/dev/null; then
    echo "--- Installing Rust ---"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    source "$HOME/.cargo/env"
else
    echo "--- Rust already installed: $(rustc --version) ---"
fi
source "$HOME/.cargo/env"

# Clone or update repo
if [ -d "$REPO_DIR/.git" ]; then
    echo "--- Updating existing repo ---"
    cd "$REPO_DIR"
    git pull --ff-only || echo "warning: git pull failed, using existing source"
else
    echo "--- Cloning repo ---"
    if [ -n "${AXELBOT_GIT_URL:-}" ]; then
        git clone "$AXELBOT_GIT_URL" "$REPO_DIR"
    else
        echo "No AXELBOT_GIT_URL set. Please either:"
        echo "  1. Set AXELBOT_GIT_URL and re-run, or"
        echo "  2. scp the source: scp -r /path/to/AxelBot ubuntu@<ec2-ip>:~/AxelBot"
        if [ ! -d "$REPO_DIR/src" ]; then
            echo "ERROR: $REPO_DIR/src not found. Upload source first."
            exit 1
        fi
    fi
fi

cd "$REPO_DIR"

# Check .env exists
if [ ! -f ".env" ]; then
    echo ""
    echo "WARNING: .env file not found. Copy it from your local machine:"
    echo "  scp .env ubuntu@<ec2-ip>:~/AxelBot/.env"
    echo ""
fi

# Build release binary
echo "--- Building release binary (this takes 2-3 minutes) ---"
cargo build --release 2>&1

echo ""
echo "=== Setup complete ==="
echo "Binary: $REPO_DIR/target/release/axelbot"
echo ""
echo "Quick test:"
echo "  cd $REPO_DIR && cargo run --release -- paper-live --config config.toml --ticks 30"
echo ""
echo "Batch data collection:"
echo "  nohup bash scripts/ec2-batch-paper.sh > batch.log 2>&1 &"
