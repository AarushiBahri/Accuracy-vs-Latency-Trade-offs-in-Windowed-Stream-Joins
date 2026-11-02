"""
Scores one completed run against ground truth and appends a row to a results CSV.

Works for both join types:
  inner      : every output row is a match.
  leftOuter  : rows with a fare are matches; rows WITHOUT a fare are 'declared unmatched' (Spark has ruled
               out a late fare). A truth trip that appears in neither group was still pending when we stopped.

Latencies (wall-clock seconds, also scaled by --speed-multiplier to simulated seconds):
  match latency     = emit_ts - max(pickup_arrival_ts, fare_arrival_ts)
  unmatched latency = emit_ts - pickup_arrival_ts   (time from the pickup arriving to Spark declaring it unmatched)
"""
import argparse, csv, json
from pathlib import Path


def read_rows(matches_dir):
    for path in Path(matches_dir).glob("*.json"):
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def load_truth(path):
    ids = set()
    with open(path) as f:
        for line in f:
            if line.strip():
                ids.add(json.loads(line)["trip_id"])
    return ids


def pct(vals, q):
    if not vals:
        return float("nan")
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(q * len(vals)))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ground-truth", required=True)
    ap.add_argument("--matches", required=True)
    ap.add_argument("--condition", required=True)
    ap.add_argument("--watermark", required=True)
    ap.add_argument("--join-window", required=True)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--results-log", default="./output/results.csv")
    ap.add_argument("--speed-multiplier", type=float, default=1.0)
    args = ap.parse_args()

    truth = load_truth(args.ground_truth)
    matched, unmatched = {}, {}
    for r in read_rows(args.matches):
        tid = r.get("trip_id")
        if tid not in truth:
            continue
        try:
            if r.get("fare_time") is not None:
                matched[tid] = r["emit_ts"] - max(r["pickup_arrival_ts"], r["fare_arrival_ts"])
            else:
                unmatched[tid] = r["emit_ts"] - r["pickup_arrival_ts"]
        except (KeyError, TypeError):
            continue

    total = len(truth)
    m, u = len(matched), len(unmatched)
    completeness = m / total if total else float("nan")
    ml, ul = sorted(matched.values()), sorted(unmatched.values())
    sp = args.speed_multiplier
    row = {
        "condition": args.condition, "watermark": args.watermark, "join_window": args.join_window, "seed": args.seed,
        "total_trips": total, "matched_trips": m, "completeness": f"{completeness:.6f}", "loss": f"{1 - completeness:.6f}",
        "lat_p50_wall": f"{pct(ml, .5):.3f}", "lat_p99_wall": f"{pct(ml, .99):.3f}",
        "lat_p50_sim": f"{pct(ml, .5) * sp:.1f}", "lat_p99_sim": f"{pct(ml, .99) * sp:.1f}",
        "declared_unmatched": u, "pending": total - m - u,
        "unm_lat_p50_wall": f"{pct(ul, .5):.3f}", "unm_lat_p99_wall": f"{pct(ul, .99):.3f}",
        "unm_lat_p50_sim": f"{pct(ul, .5) * sp:.1f}", "unm_lat_p99_sim": f"{pct(ul, .99) * sp:.1f}",
    }
    log = Path(args.results_log); log.parent.mkdir(parents=True, exist_ok=True)
    new = not log.exists()
    with log.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row)); 
        if new: w.writeheader()
        w.writerow(row)
    print(f"[{args.condition}] wm={args.watermark} win={args.join_window} seed={args.seed} -> completeness={completeness:.4f} "
          f"({m}/{total} matched, {u} declared unmatched, {total - m - u} pending); "
          f"match latency p50={pct(ml, .5):.1f}s wall; unmatched-declare p50={pct(ul, .5):.1f}s wall ({pct(ul, .5) * sp:.0f}s sim)")


if __name__ == "__main__":
    main()
