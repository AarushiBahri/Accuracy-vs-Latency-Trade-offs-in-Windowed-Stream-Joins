"""Static load test: same data joined keyed by trip_id vs by pickup zone, optionally with the busiest
zones amplified. Measures processing time, state size and completeness; prints a one-line summary.
Usage: python analysis/load_test.py --join-key zone --amplify 5 --name zone_amp5
"""
import argparse, glob, json, os, shutil, subprocess, sys, time, collections
ap = argparse.ArgumentParser()
ap.add_argument("--join-key", choices=["trip_id", "zone"], required=True)
ap.add_argument("--amplify", type=float, default=1.0)
ap.add_argument("--topk", type=int, default=3)
ap.add_argument("--num-trips", type=int, default=25000)
ap.add_argument("--span-hours", type=float, default=4)
ap.add_argument("--max-trip-minutes", type=float, default=30)
ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--name", required=True)
ap.add_argument("--timeout", type=int, default=230)
a = ap.parse_args()

out = f"./loadtest/{a.name}"
shutil.rmtree(out, ignore_errors=True)
for d in ("data", "stream/pickups", "stream/fares", "output"):
    os.makedirs(f"{out}/{d}")
t_start = time.time()
subprocess.run([sys.executable, "data/generate_taxi_streams.py", "--tlc-parquet", "./data/raw/yellow_tripdata_2026-01.parquet",
    "--num-trips", str(a.num_trips), "--span-hours", str(a.span_hours), "--max-trip-minutes", str(a.max_trip_minutes),
    "--seed", str(a.seed), "--skew-top-k", str(a.topk), "--skew-amplify-factor", str(a.amplify),
    "--out-dir", f"{out}/data"], check=True, capture_output=True)
shutil.copy(f"{out}/data/pickups.jsonl", f"{out}/stream/pickups/part-0.json")
shutil.copy(f"{out}/data/fares.jsonl", f"{out}/stream/fares/part-0.json")

zones = collections.Counter(json.loads(l)["PULocationID"] for l in open(f"{out}/data/pickups.jsonl"))
n = sum(zones.values())
work_zone = sum(c * c for c in zones.values())
top_zone, top_n = zones.most_common(1)[0]

t_spark = time.time()
with open(f"{out}/spark.log", "w") as log:
    subprocess.run([sys.executable, "spark/streaming_join.py", "--pickups-dir", f"{out}/stream/pickups",
        "--fares-dir", f"{out}/stream/fares", "--watermark", "10 minutes", "--join-window", "1 hour",
        "--join-key", a.join_key, "--static-test", "--output-path", f"{out}/output/matches",
        "--checkpoint-dir", f"{out}/chk", "--metrics-out", f"{out}/output/metrics.jsonl"],
        stdout=log, stderr=subprocess.STDOUT, timeout=a.timeout)
spark_wall = time.time() - t_spark

trigger_ms = add_ms = 0; peak_rows = peak_mem = 0
for l in open(f"{out}/output/metrics.jsonl"):
    m = json.loads(l)
    if m["numInputRows"] > 0:
        trigger_ms += m["durationMs"].get("triggerExecution", 0); add_ms += m["durationMs"].get("addBatch", 0)
    for o in m["stateOperators"]:
        peak_rows = max(peak_rows, o["numRowsTotal"]); peak_mem = max(peak_mem, o["memoryUsedBytes"])
matched = sum(1 for f in glob.glob(f"{out}/output/matches/*.json") for l in open(f) if l.strip())
total = sum(1 for _ in open(f"{out}/data/ground_truth.jsonl"))
plan = [l.strip() for l in open(f"{out}/spark.log") if "StreamingSymmetricHashJoin" in l]
print(f"[{a.name}] key={a.join_key} trips={total} hottest_zone={top_n} ({top_n/n:.1%}) "
      f"candidate_checks~{work_zone if a.join_key=='zone' else n:,} | join_time={add_ms/1000:.1f}s "
      f"(trigger {trigger_ms/1000:.1f}s) | state_rows={peak_rows} state_mem={peak_mem/1e6:.1f}MB | "
      f"matched={matched}/{total} ({matched/total:.1%}) | wall={spark_wall:.0f}s")
print("   plan:", (plan[0][:170] if plan else "(no join line found)"))
