"""
Replays a generated stream (pickups.jsonl or fares.jsonl) into a directory
that a Spark file-streaming source polls, respecting each record's actual
scheduled arrival time -- not just writing everything at once.

This is what makes "lateness" and "burstiness" real for local testing: if we
just wrote all records into one static file, Spark's file source would read
them all in one micro-batch with no meaningful notion of "late arrival" at
all. Here, a record with arrival_delay_seconds=1800 genuinely doesn't land
in the watched directory until 1800 (simulated) seconds after its event
time -- so the watermark either has or hasn't advanced past it for real.

--speed-multiplier compresses wall-clock time so a --span-hours run from the
generator doesn't take literal hours to replay locally (e.g. 60x makes an
hour of simulated span replay in one minute).
"""
import argparse
import json
import time
from datetime import datetime
from pathlib import Path


def load_records(path: Path, delay_field: str | None):
    records = []
    with path.open() as f:
        for line in f:
            r = json.loads(line)
            r["_delay_s"] = float(r.get(delay_field, 0.0)) if delay_field else 0.0
            records.append(r)
    return records


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="pickups.jsonl or fares.jsonl from generate_taxi_streams.py")
    ap.add_argument("--out-dir", required=True, help="directory Spark's file-streaming source polls")
    ap.add_argument("--delay-field", default=None, help="e.g. arrival_delay_seconds for the fares stream; omit for pickups")
    ap.add_argument("--speed-multiplier", type=float, default=60.0, help="time compression factor")
    ap.add_argument("--base-event-time", default=None,
                    help="ISO time that maps to replay offset 0. MUST be identical for the pickups and fares replays; "
                         "otherwise each file uses its own min event time and the streams are misaligned.")
    ap.add_argument("--flush-interval-seconds", type=float, default=2.0, help="how often to write a new batch file")
    args = ap.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    records = load_records(Path(args.input), args.delay_field)
    base_event_time = (datetime.fromisoformat(args.base_event_time) if args.base_event_time
                       else min(datetime.fromisoformat(r["event_time"]) for r in records))

    # scheduled replay offset (in compressed wall-clock seconds) for each record
    for r in records:
        et = datetime.fromisoformat(r["event_time"])
        true_offset_s = (et - base_event_time).total_seconds()
        r["_replay_offset_s"] = (true_offset_s + r["_delay_s"]) / args.speed_multiplier
    records.sort(key=lambda r: r["_replay_offset_s"])

    print(f"Replaying {len(records)} records from {args.input} -> {args.out_dir} "
          f"(speed x{args.speed_multiplier}, flush every {args.flush_interval_seconds}s)")

    replay_start = time.monotonic()
    batch_idx = 0
    i = 0
    n = len(records)
    while i < n:
        now_offset = time.monotonic() - replay_start
        due = [r for r in records[i:] if r["_replay_offset_s"] <= now_offset]
        if due:
            batch_path = out / f"part-{batch_idx:06d}.json"
            write_ts = time.time()
            with batch_path.open("w") as f:
                for r in due:
                    clean = {k: v for k, v in r.items() if not k.startswith("_")}
                    clean["arrival_ts"] = write_ts  # wall-clock time this record landed in the watched dir
                    f.write(json.dumps(clean) + "\n")
            batch_idx += 1
            i += len(due)
            if i % 5000 < len(due):
                print(f"  ... {i}/{n} records replayed at t={now_offset:.1f}s")
        time.sleep(args.flush_interval_seconds)

    print(f"Done: {n} records replayed in {time.monotonic() - replay_start:.1f}s wall-clock")


if __name__ == "__main__":
    main()
