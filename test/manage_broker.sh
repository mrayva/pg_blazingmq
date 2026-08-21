#!/usr/bin/env bash
#
# Starts/stops a scratch single-node BlazingMQ broker for `make test`.
#
# Reuses BlazingMQ's own docker/single-node/config (cluster + domain
# definitions), just with the /var/local/bmq paths rewritten to a scratch
# directory under test/ - no root, no Docker needed. The broker is
# deliberately started fresh (previous run's state wiped) so tests never
# see messages left over from an earlier run.

set -euo pipefail

BMQ_ROOT="${BMQ_ROOT:-$HOME/blazingmq}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRATCH_DIR="$SCRIPT_DIR/.broker_scratch"
PID_FILE="$SCRATCH_DIR/broker.pid"
LOG_FILE="$SCRATCH_DIR/broker.log"
CONFIG_DIR="$SCRATCH_DIR/config"
BROKER_BIN="$BMQ_ROOT/build/blazingmq/src/applications/bmqbrkr/bmqbrkr.tsk"

cmd="${1:-}"

start() {
    if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "manage_broker.sh: broker already running (pid $(cat "$PID_FILE"))" >&2
        return 0
    fi

    if [ ! -x "$BROKER_BIN" ]; then
        echo "manage_broker.sh: broker binary not found at $BROKER_BIN" >&2
        echo "  (see README's Building section - it needs its own ninja bmqbrkr.tsk build)" >&2
        exit 1
    fi

    rm -rf "$SCRATCH_DIR"
    mkdir -p "$SCRATCH_DIR/run/logs" "$SCRATCH_DIR/run/storage/archive"
    cp -r "$BMQ_ROOT/docker/single-node/config" "$CONFIG_DIR"
    sed -i "s#/var/local/bmq#$SCRATCH_DIR/run#g" \
        "$CONFIG_DIR/bmqbrkrcfg.json" "$CONFIG_DIR/clusters.json"

    nohup "$BROKER_BIN" "$CONFIG_DIR" > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    disown

    for _ in $(seq 1 50); do
        if grep -q "started successfully" "$LOG_FILE" 2>/dev/null; then
            echo "manage_broker.sh: broker started (pid $(cat "$PID_FILE"))"
            return 0
        fi
        sleep 0.2
    done

    echo "manage_broker.sh: broker did not start within 10s - see $LOG_FILE" >&2
    exit 1
}

stop() {
    if [ ! -f "$PID_FILE" ]; then
        echo "manage_broker.sh: no pid file, nothing to stop" >&2
        return 0
    fi
    pid="$(cat "$PID_FILE")"
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid"
        for _ in $(seq 1 50); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.2
        done
    fi
    rm -f "$PID_FILE"
    echo "manage_broker.sh: broker stopped"
}

case "$cmd" in
    start) start ;;
    stop) stop ;;
    *) echo "usage: $0 {start|stop}" >&2; exit 1 ;;
esac
