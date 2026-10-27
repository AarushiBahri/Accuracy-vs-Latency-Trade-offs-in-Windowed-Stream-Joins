"""Shows how Spark's join watermark (its 'clock') moved during a run, vs the period when fares were held back.
Usage: python analysis/watermark_timeline.py runs/burst_only/seed1/wm_15_seconds"""
import json, sys
from datetime import datetime
run = sys.argv[1]
pick = [json.loads(l) for l in open(f"{run}/data/pickups.jsonl")]
fares = [json.loads(l) for l in open(f"{run}/data/fares.jsonl")]
base = min(datetime.fromisoformat(r["event_time"]) for r in pick)
off = lambda iso: (datetime.fromisoformat(iso.replace("Z", "")).replace(tzinfo=None) - base.replace(tzinfo=None)).total_seconds()
held = [r for r in fares if r["arrival_delay_seconds"] > 0]
if held:
    t0 = min(off(r["event_time"]) for r in held); t1 = max(off(r["event_time"]) + r["arrival_delay_seconds"] for r in held)
    print(f"held-back fares: {len(held)} | event times {min(off(r['event_time']) for r in held):.0f}s-{max(off(r['event_time']) for r in held):.0f}s | released by ~{t1:.0f}s (event-time offsets)")
recs = []
for l in open(f"{run}/output/metrics.jsonl"):
    m = json.loads(l); wm = m.get("eventTime", {}).get("watermark")
    recs.append((m["batchId"], m["numInputRows"], off(wm) if wm else None))
active = [(b, n, w) for b, n, w in recs if n > 0 and w is not None]
print(f"{'batch':>5} {'rows in':>8} {'watermark (s from start)':>26}")
prev = None
for b, n, w in active:
    flag = "" if prev is None or w != prev else "   <- clock did not move"
    print(f"{b:>5} {n:>8} {w:>26.0f}{flag}")
    prev = w
longest = cur = rows = best_rows = 0; last = None
for b, n, w in active:
    if last is not None and w == last: cur += 1; rows += n
    else: cur = 0; rows = 0
    if cur > longest: longest, best_rows = cur, rows
    last = w
print(f"\nlongest stretch where rows kept arriving but the clock stayed frozen: {longest} batches ({best_rows} rows)")
