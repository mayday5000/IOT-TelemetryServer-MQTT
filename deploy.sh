#!/usr/bin/env bash
# deploy.sh – idempotent package check + full build (Linux only)
set -euo pipefail

echo "[deploy] Checking build tools..."

need_install=0
if ! command -v g++ >/dev/null 2>&1; then need_install=1; fi
if ! command -v make >/dev/null 2>&1; then need_install=1; fi

if [[ $need_install -eq 1 ]]; then
    echo "[deploy] Installing missing packages..."
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update
        sudo apt-get install -y build-essential g++ make
    elif command -v dnf >/dev/null 2>&1; then
        sudo dnf install -y gcc-c++ make
    elif command -v yum >/dev/null 2>&1; then
        sudo yum install -y gcc-c++ make
    else
        echo "[deploy] Unsupported package manager. Install g++ and make manually."
        exit 1
    fi
else
    echo "[deploy] g++ and make already present."
fi

echo "[deploy] Building..."
g++ -std=c++23 -O2 -pthread -o server server.cpp
g++ -std=c++23 -O2 -pthread -o client client.cpp
g++ -std=c++23 -O2 -pthread -o tests tests.cpp

echo "[deploy] Running tests..."
./tests

echo "[deploy] Build complete: ./server  ./client  ./tests"
