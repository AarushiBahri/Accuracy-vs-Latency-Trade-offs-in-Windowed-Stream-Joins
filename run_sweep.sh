#!/usr/bin/env bash
# Runs ONE full condition end-to-end: generate -> live replay -> Spark join -> score.
# This exists because manually running 4 terminals per condition (as in the
# Quickstart) doesn't scale to the dozens of condition/watermark/seed
# combinations Week 5-6 needs.
#
# Usage example:
#   ./run_sweep.sh \
#     --condition lateness_only --watermark "10 minutes" --join-window "24 hours" \
#     --seed 1 --lateness-fraction 0.15 --burst-fraction 0 --skew-amplify-factor 1 \
#     --num-trips 200000 --speed-multiplier 60
#
# All output lands under ./runs/<condition>/seed<seed>/wm<watermark_s>/
set -euo pipefail

TLC_PARQUET="./data/raw/yellow_tripdata_2026-01.parquet"
NUM_TRIPS=200000
SPAN_HOURS=48
MAX_TRIP_MIN=60
SPEED_MULT=60
LATENESS_FRACTION=0
LATENESS_SCALE=1800
BURST_FRACTION=0
BURST_WINDOW=120
SKEW_TOPK=5
SKEW_FACTOR=1
SKEW_DELAY_SHARE=0
BURST_SCATTER=""
SKEW_DELAY_SCALE=90
RESULTS_LOG="./output/results.csv"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --condition) CONDITION="$2"; shift 2;;
    --watermark) WATERMARK="$2"; shift 2;;
    --join-window) JOIN_WINDOW="$2"; shift 2;;
    --seed) SEED="$2"; shift 2;;
    --lateness-fraction) LATENESS_FRACTION="$2"; shift 2;;
    --lateness-scale|--lateness-scale-seconds) LATENESS_SCALE="$2"; shift 2;;
    --burst-fraction) BURST_FRACTION="$2"; shift 2;;
    --burst-window|--burst-window-seconds) BURST_WINDOW="$2"; shift 2;;
    --skew-topk|--skew-top-k) SKEW_TOPK="$2"; shift 2;;
    --skew-amplify-factor) SKEW_FACTOR="$2"; shift 2;;
    --skew-delay-hot-share) SKEW_DELAY_SHARE="$2"; shift 2;;
    --skew-delay-scale|--skew-delay-scale-seconds) SKEW_DELAY_SCALE="$2"; shift 2;;
    --burst-scatter) BURST_SCATTER="--burst-scatter"; shift;;
    --num-trips) NUM_TRIPS="$2"; shift 2;;
    --span-hours) SPAN_HOURS="$2"; shift 2;;
    --max-trip-minutes) MAX_TRIP_MIN="$2"; shift 2;;
    --tlc-parquet) TLC_PARQUET="$2"; shift 2;;
    --speed-multiplier) SPEED_MULT="$2"; shift 2;;
    --results-log) RESULTS_LOG="$2"; shift 2;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

for req in CONDITION WATERMARK JOIN_WINDOW SEED; do
  if [[ -z "${!req:-}" ]]; then echo "Missing required --${req,,}"; exit 1; fi
done

RUN_DIR="./runs/${CONDITION}/seed${SEED}/wm_${WATERMARK// /_}"
rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR"/{data,stream/pickups,stream/fares,output,chk}

echo "=== [$CONDITION seed=$SEED wm=$WATERMARK win=$JOIN_WINDOW] generating data ==="
python data/generate_taxi_streams.py \
  --tlc-parquet "$TLC_PARQUET" --num-trips "$NUM_TRIPS" --span-hours "$SPAN_HOURS" --max-trip-minutes "$MAX_TRIP_MIN" --seed "$SEED" \
  --lateness-fraction "$LATENESS_FRACTION" --lateness-scale-seconds "$LATENESS_SCALE" \
  --burst-fraction "$BURST_FRACTION" --burst-window-seconds "$BURST_WINDOW" \
  --skew-top-k "$SKEW_TOPK" --skew-amplify-factor "$SKEW_FACTOR" \
  --skew-delay-hot-share "$SKEW_DELAY_SHARE" --skew-delay-scale-seconds "$SKEW_DELAY_SCALE" $BURST_SCATTER \
  --out-dir "$RUN_DIR/data"

BASE_TIME=$(python -c "import json,sys; print(min(json.loads(l)['event_time'] for l in open('$RUN_DIR/data/pickups.jsonl')))")
echo "shared replay base time: $BASE_TIME"

echo "=== starting Spark join ==="
python spark/streaming_join.py \
  --pickups-dir "$RUN_DIR/stream/pickups" --fares-dir "$RUN_DIR/stream/fares" \
  --watermark "$WATERMARK" --join-window "$JOIN_WINDOW" \
  --output-path "$RUN_DIR/output/matches" --checkpoint-dir "$RUN_DIR/chk" \
  --metrics-out "$RUN_DIR/output/metrics.jsonl" --trigger-seconds 3 \
  > "$RUN_DIR/spark.log" 2>&1 &
SPARK_PID=$!
sleep 20  # JVM warmup before replay starts, so early records aren't missed by a cold query

echo "=== replaying pickups + fares ==="
python data/replay_stream.py --input "$RUN_DIR/data/pickups.jsonl" \
  --out-dir "$RUN_DIR/stream/pickups" --speed-multiplier "$SPEED_MULT" --base-event-time "$BASE_TIME" &
PICKUP_PID=$!
python data/replay_stream.py --input "$RUN_DIR/data/fares.jsonl" \
  --out-dir "$RUN_DIR/stream/fares" --delay-field arrival_delay_seconds \
  --speed-multiplier "$SPEED_MULT" --base-event-time "$BASE_TIME" &
FARE_PID=$!

wait $PICKUP_PID $FARE_PID
echo "=== replay finished, draining remaining watermark window before stopping Spark ==="
sleep 15  # let Spark catch up on the last few micro-batches

kill $SPARK_PID 2>/dev/null || true
wait $SPARK_PID 2>/dev/null || true

echo "=== scoring ==="
python analysis/score_run.py \
  --ground-truth "$RUN_DIR/data/ground_truth.jsonl" \
  --matches "$RUN_DIR/output/matches" \
  --condition "$CONDITION" --watermark "$WATERMARK" --join-window "$JOIN_WINDOW" \
  --seed "$SEED" --results-log "$RESULTS_LOG" --speed-multiplier "$SPEED_MULT"

echo "=== done: $RUN_DIR ==="
