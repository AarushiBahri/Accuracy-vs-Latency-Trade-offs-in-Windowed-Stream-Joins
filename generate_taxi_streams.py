"""
Generates two event streams from NYC TLC Yellow Taxi trip data:
  - pickups : (trip_id, tpep_pickup_datetime, PULocationID)
  - fares   : (trip_id, tpep_dropoff_datetime, fare_amount, payment_type, PULocationID)

trip_id is assigned here (row index) because the raw TLC schema has no native
trip identifier -- this assigned ID is the ground-truth join key the whole
project's completeness metric depends on.

Real data path: pass --tlc-parquet pointing at a file downloaded from
  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
(e.g. yellow_tripdata_2026-01.parquet). Subsample with --num-trips.

No file given -> falls back to a schema-faithful SYNTHETIC generator (same
column names, a Zipf-skewed PULocationID distribution, lognormal trip
duration) so the pipeline is runnable and testable without the real file.
This is a development/testing fallback, not a substitute for the real run.

THE THREE STRESSORS ARE INDEPENDENT KNOBS, each defaults to "off":
  --lateness-fraction   (0 = off) heavy-tail extra ARRIVAL delay on top of
                         the true event time, for this fraction of fares.
  --burst-fraction      (0 = off) collapses this fraction of fares' arrival
                         times into a tight window (bursty ingestion).
  --skew-amplify-factor (1 = off) replicates trips at the top-K hottest
                         PULocationIDs by this factor, creating additional
                         join-key skew on top of whatever natural skew the
                         (real or synthetic) base data already has.

Critically: these knobs perturb ARRIVAL time (when the record is actually
published), never the event_time field itself. event_time stays truthful,
which is what makes Spark's watermark semantics meaningful to study, and
what makes ground truth well-defined regardless of stressor settings.
"""
import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


TLC_COLUMNS = [
    "trip_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "fare_amount", "payment_type",
]


def load_real(path: str, num_trips: int, span_hours: float, rng: np.random.Generator,
               max_trip_minutes: float = 60.0) -> pd.DataFrame:
    cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "fare_amount", "payment_type"]
    if path.endswith(".csv") or path.endswith(".csv.gz"):
        df = pd.read_csv(path, usecols=cols, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    else:
        df = pd.read_parquet(path, columns=cols)
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID"])
    df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

    # span_hours only bounds PICKUP time; dropoffs trail pickups by the real
    # trip duration, which varies (median ~12-15 min, occasional outliers
    # over an hour). Without a cap here, a narrow pickup window can still
    # produce a fares stream whose event-time spread is far wider than
    # span_hours -- which silently makes replay take far longer than
    # expected. Capping trip duration bounds the fares spread predictably
    # to roughly span_hours + max_trip_minutes.
    trip_minutes = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60.0
    df = df[trip_minutes <= max_trip_minutes]

    df = df.sort_values("tpep_pickup_datetime").reset_index(drop=True)

    window = pd.Timedelta(hours=span_hours)
    latest_start = df["tpep_pickup_datetime"].iloc[-1] - window
    earliest_start = df["tpep_pickup_datetime"].iloc[0]
    if latest_start <= earliest_start:
        window_start = earliest_start
    else:
        offset_s = rng.uniform(0, (latest_start - earliest_start).total_seconds())
        window_start = earliest_start + pd.Timedelta(seconds=offset_s)
    # Keep only trips fully contained in the window (pickup AND dropoff inside it) so both
    # streams cover the same event-time range. If pickups ended long before fares, the join's
    # watermark (min across both streams) would stall and late fares near the tail would
    # never be evicted -- biasing completeness upward for reasons unrelated to the stressors.
    window_end = window_start + window
    df = df[(df["tpep_pickup_datetime"] >= window_start) & (df["tpep_dropoff_datetime"] < window_end)]
    df = df.reset_index(drop=True)

    if len(df) > num_trips:
        idx = rng.choice(len(df), size=num_trips, replace=False)
        df = df.iloc[np.sort(idx)]
    elif len(df) < num_trips:
        print(f"[warn] only {len(df)} real trips (<= {max_trip_minutes}min) fall inside the {span_hours}h window "
              f"starting {window_start} -- requested {num_trips}. Using all {len(df)}.")

    df = df.reset_index(drop=True)
    df.insert(0, "trip_id", [f"t{i:08d}" for i in range(len(df))])
    return df[TLC_COLUMNS]


def generate_synthetic(num_trips: int, num_zones: int, zipf_alpha: float,
                        start: datetime, span_hours: float, rng: np.random.Generator) -> pd.DataFrame:
    """Schema-faithful stand-in for local dev/testing when no real parquet is supplied."""
    zone_ranks = np.arange(1, num_zones + 1)
    zone_weights = 1.0 / np.power(zone_ranks, zipf_alpha)
    zone_weights = zone_weights / zone_weights.sum()
    rng.shuffle(zone_weights)

    pu_zone = rng.choice(num_zones, size=num_trips, p=zone_weights) + 1
    pickup_offset_s = rng.uniform(0, span_hours * 3600, size=num_trips)
    pickup_time = [start + timedelta(seconds=float(s)) for s in pickup_offset_s]
    trip_duration_min = rng.lognormal(mean=2.5, sigma=0.6, size=num_trips)  # median ~12 min
    dropoff_time = [p + timedelta(minutes=float(m)) for p, m in zip(pickup_time, trip_duration_min)]
    fare = np.round(4.0 + trip_duration_min * 0.85 + rng.normal(0, 1.5, size=num_trips), 2).clip(min=3.5)
    payment = rng.choice([1, 2], size=num_trips, p=[0.7, 0.3])  # 1=card, 2=cash (TLC convention)

    df = pd.DataFrame({
        "trip_id": [f"t{i:08d}" for i in range(num_trips)],
        "tpep_pickup_datetime": pickup_time,
        "tpep_dropoff_datetime": dropoff_time,
        "PULocationID": pu_zone,
        "fare_amount": fare,
        "payment_type": payment,
    })
    return df.sort_values("tpep_pickup_datetime").reset_index(drop=True)


def apply_skew_amplification(df: pd.DataFrame, top_k: int, factor: float, rng: np.random.Generator) -> pd.DataFrame:
    """Replicate trips at the top-K hottest PULocationIDs `factor`x, as extra synthetic
    trips with new trip_ids. factor=1 is a no-op (independent knob, default off)."""
    if factor <= 1.0:
        return df
    hot_zones = df["PULocationID"].value_counts().nlargest(top_k).index.tolist()
    hot_rows = df[df["PULocationID"].isin(hot_zones)]
    extra_frames = [df]
    next_id = len(df)
    n_copies = int(round(factor - 1.0))
    for _ in range(n_copies):
        copy = hot_rows.copy()
        copy["trip_id"] = [f"t{next_id + i:08d}" for i in range(len(copy))]
        next_id += len(copy)
        # jitter pickup time slightly so amplified trips aren't exact duplicates in time
        jitter_s = rng.uniform(-30, 30, size=len(copy))
        copy["tpep_pickup_datetime"] = copy["tpep_pickup_datetime"] + pd.to_timedelta(jitter_s, unit="s")
        copy["tpep_dropoff_datetime"] = copy["tpep_dropoff_datetime"] + pd.to_timedelta(jitter_s, unit="s")
        extra_frames.append(copy)
    return pd.concat(extra_frames, ignore_index=True).sort_values("tpep_pickup_datetime").reset_index(drop=True)


def compute_arrival_offsets(n: int, lateness_fraction: float, lateness_scale_s: float,
                             burst_fraction: float, burst_window_s: float,
                             span_s: float, rng: np.random.Generator) -> np.ndarray:
    """Extra arrival delay (seconds) added on top of the true event time, for the
    FARE stream only. Baseline (all knobs off) = 0 for every record: the record
    arrives essentially at its true event time (clean case)."""
    extra = np.zeros(n)

    if lateness_fraction > 0:
        hit = rng.random(n) < lateness_fraction
        extra[hit] += rng.exponential(scale=lateness_scale_s, size=hit.sum())

    if burst_fraction > 0:
        hit = rng.random(n) < burst_fraction
        n_hit = int(hit.sum())
        if n_hit > 0:
            burst_epoch = rng.uniform(0, max(span_s - burst_window_s, 1))
            extra[hit] = burst_epoch + rng.uniform(0, burst_window_s, size=n_hit)

    return extra


def skew_delay_offsets(df: pd.DataFrame, hot_share: float, scale_s: float, rng: np.random.Generator):
    """EXPERIMENTAL skew-as-delay: pick the smallest set of pickup zones covering >= hot_share of
    trips, and delay ALL fares from those zones (exponential, mean scale_s). Same delay
    distribution as the lateness knob; only WHICH fares are delayed differs (concentrated in
    hot zones instead of random). Independent knob: 0 = off."""
    counts = df["PULocationID"].value_counts()
    cum = counts.cumsum() / counts.sum()
    hot_zones = set(cum[cum.shift(fill_value=0) < hot_share].index)
    is_hot = df["PULocationID"].isin(hot_zones).to_numpy()
    extra = np.zeros(len(df))
    extra[is_hot] = rng.exponential(scale=scale_s, size=int(is_hot.sum()))
    print(f"[skew-delay] {len(hot_zones)} hot zones cover {is_hot.mean():.1%} of trips; their fares are delayed")
    return extra


def burst_delay_offsets(fare_offset_s: np.ndarray, outage_fraction: float, flush_window_s: float,
                        rng: np.random.Generator, scatter: bool = False) -> np.ndarray:
    """Burst = an outage then a flush. Fares whose event time falls inside an outage interval
    (covering outage_fraction of the event span) are held back and released together within
    flush_window_s after the outage ends, so arrivals genuinely bunch. Delay = release - event time.
    scatter=True is the MATCHED CONTROL: identical multiset of delay values, but assigned to random
    fares, which destroys the bunching while keeping the amount of delay the same."""
    lo, hi = fare_offset_s.min(), fare_offset_s.max()
    outage = outage_fraction * (hi - lo)
    t0 = rng.uniform(lo, hi - outage)
    t1 = t0 + outage
    hit = (fare_offset_s >= t0) & (fare_offset_s < t1)
    extra = np.zeros(len(fare_offset_s))
    extra[hit] = (t1 + rng.uniform(0, flush_window_s, size=int(hit.sum()))) - fare_offset_s[hit]
    print(f"[burst] outage {outage:.0f}s holds {hit.mean():.1%} of fares; released within {flush_window_s:.0f}s"
          f"{' (SCATTERED control)' if scatter else ''}")
    return rng.permutation(extra) if scatter else extra


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tlc-parquet", default=None, help="Path to a real TLC Yellow Taxi parquet file")
    ap.add_argument("--num-trips", type=int, default=200_000)
    ap.add_argument("--num-zones", type=int, default=260, help="synthetic fallback only (NYC has 263 TLC zones)")
    ap.add_argument("--zipf-alpha", type=float, default=1.1, help="synthetic fallback natural-skew exponent")
    ap.add_argument("--span-hours", type=float, default=48.0)
    ap.add_argument("--max-trip-minutes", type=float, default=60.0,
                     help="cap on real trip duration when windowing, so the fares stream's spread stays bounded")
    ap.add_argument("--seed", type=int, default=42)

    ap.add_argument("--lateness-fraction", type=float, default=0.0, help="0 = off (independent knob)")
    ap.add_argument("--lateness-scale-seconds", type=float, default=1800.0, help="mean extra delay when triggered")

    ap.add_argument("--burst-fraction", type=float, default=0.0, help="0 = off (independent knob)")
    ap.add_argument("--burst-window-seconds", type=float, default=120.0)

    ap.add_argument("--skew-top-k", type=int, default=5)
    ap.add_argument("--skew-amplify-factor", type=float, default=1.0, help="1 = off (independent knob)")

    ap.add_argument("--skew-delay-hot-share", type=float, default=0.0,
                    help="EXPERIMENTAL: 0 = off. Delay all fares in the busiest zones covering this share of trips")
    ap.add_argument("--skew-delay-scale-seconds", type=float, default=90.0)
    ap.add_argument("--burst-scatter", action="store_true",
                    help="control: same burst delays assigned to random fares (no bunching)")
    ap.add_argument("--out-dir", default="./data")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    start = datetime.now(timezone.utc) - timedelta(hours=args.span_hours)

    if args.tlc_parquet:
        df = load_real(args.tlc_parquet, args.num_trips, args.span_hours, rng, args.max_trip_minutes)
        print(f"Loaded {len(df)} real trips from {args.tlc_parquet} "
              f"(within a {args.span_hours}h window, trips <= {args.max_trip_minutes}min)")
    else:
        df = generate_synthetic(args.num_trips, args.num_zones, args.zipf_alpha, start, args.span_hours, rng)
        print(f"[dev fallback] generated {len(df)} synthetic trips (no --tlc-parquet given)")

    df = apply_skew_amplification(df, args.skew_top_k, args.skew_amplify_factor, rng)
    n = len(df)
    span_s = args.span_hours * 3600

    # lateness only here (burst_fraction=0): burst has its own function + rng so adding it never
    # changes the lateness realization -- needed for a clean additive baseline in the compounding test.
    arrival_offset_s = compute_arrival_offsets(
        n, args.lateness_fraction, args.lateness_scale_seconds,
        0.0, args.burst_window_seconds, span_s, rng,
    )
    if args.burst_fraction > 0:
        base_t = pd.Timestamp(df["tpep_pickup_datetime"].min())
        fare_offset = (pd.to_datetime(df["tpep_dropoff_datetime"]) - base_t).dt.total_seconds().to_numpy()
        arrival_offset_s = arrival_offset_s + burst_delay_offsets(
            fare_offset, args.burst_fraction, args.burst_window_seconds,
            np.random.default_rng(args.seed + 2000), args.burst_scatter)

    if args.skew_delay_hot_share > 0:
        arrival_offset_s = arrival_offset_s + skew_delay_offsets(
            df, args.skew_delay_hot_share, args.skew_delay_scale_seconds,
            np.random.default_rng(args.seed + 1000))

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    pickups_path = out / "pickups.jsonl"
    fares_path = out / "fares.jsonl"
    truth_path = out / "ground_truth.jsonl"

    with pickups_path.open("w") as fp, fares_path.open("w") as ff, truth_path.open("w") as ft:
        for i in range(n):
            row = df.iloc[i]
            trip_id = row["trip_id"]
            pu_t = pd.Timestamp(row["tpep_pickup_datetime"]).isoformat()
            do_t = pd.Timestamp(row["tpep_dropoff_datetime"]).isoformat()

            fp.write(json.dumps({
                "trip_id": trip_id,
                "event_time": pu_t,
                "PULocationID": int(row["PULocationID"]),
            }) + "\n")

            ff.write(json.dumps({
                "trip_id": trip_id,
                "event_time": do_t,                      # TRUE event time -- never perturbed
                "arrival_delay_seconds": float(arrival_offset_s[i]),  # how late it's actually published
                "fare_amount": float(row["fare_amount"]),
                "payment_type": int(row["payment_type"]),
                "PULocationID": int(row["PULocationID"]),
            }) + "\n")

            ft.write(json.dumps({"trip_id": trip_id, "true_dropoff_time": do_t}) + "\n")

    print(f"Wrote {n} trips -> {pickups_path}, {fares_path}, {truth_path}")
    print(f"Condition: lateness_fraction={args.lateness_fraction}, "
          f"burst_fraction={args.burst_fraction}, skew_amplify_factor={args.skew_amplify_factor}")


if __name__ == "__main__":
    main()
