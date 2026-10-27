"""
Scores one completed run: how many of the ground-truth trips actually made
it through the join before eviction. Appends one row to a shared results
log (results.csv) that compounding_report.py later reads across many runs.

Usage (per condition/config combination, e.g. after each streaming_join.py run):
  python score_run.py --ground-truth ./data/ground_truth.jsonl \
      --matches ./output/matches \
      --condition baseline --watermark "30 minutes" --join-window "40 minutes" \
      --seed 1 --results-log ./output/results.csv
"""
import argparse
import csv
import glob
import json
from pathlib import Path


def load_trip_ids(jsonl_glob_or_dir: str, field: str = "trip_id") -> set:
    ids = set()
    paths = glob.glob(jsonl_glob_or_dir) if "*" in jsonl_glob_or_dir else \
        [str(p) for p in Path(jsonl_glob_or_dir).glob("*.json")] or [jsonl_glob_or_dir]
    for path in paths:
        p = Path(path)
        if not p.exists() or p.is_dir():
            continue
        with p.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ids.add(json.loads(line)[field])
                except (json.JSONDecodeError, KeyError):
                    continue
    return ids


def load_latencies(matches_dir: str):
    """Per matched trip: emit_ts - max(pickup_arrival_ts, fare_arrival_ts), in wall-clock seconds."""
    lats = []
    for path in Path(matches_dir).glob("*.json"):
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    lats.append(r["emit_ts"] - max(r["pickup_arrival_ts"], r["fare_arrival_ts"]))
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
    return lats


def pct(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    return sorted_vals[min(len(sorted_vals) - 1, int(q * len(sorted_vals)))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ground-truth", required=True, help="ground_truth.jsonl for this run's condition")
    ap.add_argument("--matches", required=True, help="Spark join output directory (part files)")
    ap.add_argument("--condition", required=True, help="e.g. baseline, lateness_only, lateness+skew")
    ap.add_argument("--watermark", required=True)
    ap.add_argument("--join-window", required=True)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--results-log", default="./output/results.csv")
    ap.add_argument("--speed-multiplier", type=float, default=1.0, help="replay speed, to express latency in simulated seconds")
    args = ap.parse_args()

    truth_ids = load_trip_ids(args.ground_truth)
    matched_ids = load_trip_ids(args.matches.rstrip("/") + "/*.json")

    total = len(truth_ids)
    matched = len(truth_ids & matched_ids)
    completeness = matched / total if total else float("nan")
    loss = 1.0 - completeness

    lats = sorted(load_latencies(args.matches))
    p50, p99 = pct(lats, 0.5), pct(lats, 0.99)
    p50_sim, p99_sim = p50 * args.speed_multiplier, p99 * args.speed_multiplier

    log_path = Path(args.results_log)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not log_path.exists()
    with log_path.open("a", newline="") as f:
        w = csv.writer(f)
        if is_new:
            w.writerow(["condition", "watermark", "join_window", "seed", "total_trips", "matched_trips", "completeness", "loss", "lat_p50_wall", "lat_p99_wall", "lat_p50_sim", "lat_p99_sim"])
        w.writerow([args.condition, args.watermark, args.join_window, args.seed, total, matched, f"{completeness:.6f}", f"{loss:.6f}", f"{p50:.3f}", f"{p99:.3f}", f"{p50_sim:.1f}", f"{p99_sim:.1f}"])

    print(f"[{args.condition}] watermark={args.watermark} join_window={args.join_window} seed={args.seed} "
          f"-> completeness={completeness:.4f} (loss={loss:.4f}), {matched}/{total} matched, latency p50={p50:.1f}s p99={p99:.1f}s wall")


if __name__ == "__main__":
    main()
