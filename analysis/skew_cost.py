"""Summarises the COST of skew from each run's Spark metrics.jsonl: batch time and join state size.
Usage: python analysis/skew_cost.py runs/<cond>/seed*/wm_*/output/metrics.jsonl ...  (prints one row per file)"""
import json, statistics, sys
from pathlib import Path

def summarize(path):
    durs, rows, mem = [], [], []
    for line in Path(path).read_text().splitlines():
        try: r = json.loads(line)
        except json.JSONDecodeError: continue
        if r.get("numInputRows", 0) > 0:
            durs.append(r["durationMs"].get("triggerExecution", 0) / 1000)
        for s in r.get("stateOperators", []):
            rows.append(s["numRowsTotal"]); mem.append(s["memoryUsedBytes"])
    if not durs: return None
    d = sorted(durs)
    return dict(batches=len(d), mean_batch_s=statistics.mean(d), p95_batch_s=d[int(.95 * (len(d) - 1))],
                max_batch_s=d[-1], max_state_rows=max(rows or [0]), max_state_mb=max(mem or [0]) / 1e6)

if __name__ == "__main__":
    print("file,batches,mean_batch_s,p95_batch_s,max_batch_s,max_state_rows,max_state_mb")
    for f in sys.argv[1:]:
        s = summarize(f)
        if s: print(f"{f},{s['batches']},{s['mean_batch_s']:.2f},{s['p95_batch_s']:.2f},{s['max_batch_s']:.2f},{s['max_state_rows']},{s['max_state_mb']:.2f}")
