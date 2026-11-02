# Accuracy-vs-Latency Trade-offs in Windowed Stream Joins Under Combined Real-World Imperfections

AMS 560 (Big Data Systems, Algorithms and Networks), Group 11.

We join two streams of NYC taxi events in Spark Structured Streaming (a pickup record and its fare record,
matched on a trip ID) and measure how many trips are lost, and how long results take, when records are
**late**, **bursty** or **skewed**, alone and combined. Trips come from the real TLC Yellow Taxi file
(January 2026). A trip ID is assigned at ingestion, so every true match is known and completeness is scored
against ground truth. The **watermark delay** and the **join window** are separate settings everywhere, and
**compounding** is computed, not eyeballed: `interaction = L(AB) - (L(A) + L(B))`, where `L` is the share of
trips lost (positive beyond seed noise = compounding, negative = masking).

Accuracy comes from the inner-join sweep. Latency comes from a left-outer-join variant, because an inner join
cannot time trips that never match.

## Status
Done: data pipeline, three stressors, Spark join (inner and left outer), scoring, 61 inner-join runs,
12 zone-keyed skew runs, 9 outer-join runs, compounding and sensitivity analysis.
Not done: Kafka source (file replay is used), adaptive watermark (conditional stretch goal), join-window sweep at
full scale, more seeds on the outer join, final report.

## Setup (macOS / Linux; on Windows use WSL or Git Bash, the runners are bash scripts)
```bash
java -version                       # Java 17 or 21 is required by Spark
python3 -m venv .venv && source .venv/bin/activate
python -m pip install -r requirements.txt
chmod +x *.sh
# download the TLC file and put it at: data/raw/yellow_tripdata_2026-01.parquet
```
The TLC file (about 64 MB) is not in this repo. It is the "Yellow Taxi Trip Records" parquet for January 2026
from the NYC TLC trip record data page.

## Run one condition (generate, replay, Spark join, score), about 3 to 5 minutes
```bash
./run_sweep.sh --condition lateness_only --watermark "5 minutes" --join-window "2 hours" \
  --seed 1 --num-trips 3000 --span-hours 1 --speed-multiplier 60 \
  --lateness-fraction 0.5 --lateness-scale 600 --results-log ./output/test_results.csv
```
Left outer join (adds unscored trips after the window so the watermark keeps moving):
```bash
./run_sweep.sh --condition lateness_only --watermark "5 minutes" --join-window "1 hour" \
  --seed 1 --num-trips 3000 --span-hours 1 --speed-multiplier 60 \
  --lateness-fraction 0.5 --lateness-scale 600 --join-type leftOuter --tail-hours 2.5 \
  --results-log ./output/test_outer.csv
```
Condition names must be `<stressor>_only` or `<a>+<b>` (for example `lateness_only`, `lateness+burst`).
Use a separate `--results-log` for tests. Output lands in `runs/<condition>/seed<N>/wm_<watermark>/`.

## Run the full experiments (run ONE at a time; they share the CPU and would distort latency)
| script | what it runs |
|---|---|
| `run_matrix.sh` | 7 conditions x 2 seeds x watermarks 5 and 20 minutes (inner join) |
| `run_matrix2.sh` | watermarks 8, 10, 14 minutes, seed 3 (resumable) |
| `run_skew_cost.sh` | zone-keyed join with skew amplified x1, x5, x10, with and without lateness |
| `run_matrix3.sh`, `run_matrix3b.sh` | left-outer-join grid, and a priority subset of it |

## Analysis
```bash
python analysis/compounding_report.py --results-log output/merged_results.csv   # compounding + sensitivity ranking
python analysis/pooled_interaction.py output/merged_results.csv                 # pooled test of lateness+burst interaction
python analysis/skew_cost.py runs/*/seed*/wm_*/output/metrics.jsonl             # batch time and state size per run
python analysis/watermark_timeline.py <metrics.jsonl>                           # watermark per micro-batch (shows freezes)
python analysis/load_test.py --join-key zone --amplify 5 --topk 3 --name test   # one-batch skew load test
```
Saved results are in `results/` (`merged_results.csv` = inner-join runs, `outer_results.csv` = outer-join runs).

## Stressors
- **lateness**: a random share of fares (`--lateness-fraction`) gets an exponential extra delay (`--lateness-scale`).
- **burst**: an outage covering `--burst-fraction` of the span, then the held fares are released within
  `--burst-window` seconds. `--burst-scatter` is the control (same delays on random fares).
- **skew** (`--skew-amplify-factor`, `--skew-top-k`): extra trips from the busiest pickup zones.
- When two stressors hit the same fare their delays are summed. That is a design choice and can create
  compounding mechanically.

## Results so far (small samples; see the progress report for caveats)
Inner join, mean lost trips, 3 seeds, 2-hour join window:

| watermark | lateness | burst | sum | both | interaction |
|---|---|---|---|---|---|
| 5 min | 15.8% | 7.2% | 23.0% | 20.6% | -2.4 pts (masking) |
| 8 min | 10.9% | 5.3% | 16.2% | 20.9% | +4.7 pts |
| 10 min | 8.1% | 4.3% | 12.4% | 13.9% | +1.5 pts |
| 14 min | 4.6% | 1.1% | 5.7% | 9.8% | +4.2 pts |
| 20 min | 2.0% | 0.0% | 2.0% | 4.1% | +2.1 pts |

- Pooled over 8 to 20 minutes the interaction is +3.1 points (p = 0.002, split chosen after seeing the data);
  over all watermarks +2.0 points (p = 0.07). Seeds are reused across watermarks, so treat this as a pattern.
- Lateness costs 1.9x more lost trips per second of watermark tightening than burst. Skew alone loses 0%.
- Skew never changed completeness: with a unique trip ID it cannot, and a zone-keyed join (up to 10x
  amplification) changed only state size, not lost trips or batch time. In the real data busy zones do not
  have systematically different trip lengths.

Left outer join, lateness only, 1 seed, 1-hour join window:

| watermark | lost trips | time to declare a trip unmatched (simulated) |
|---|---|---|
| 5 min | 19.2% | about 69 min |
| 10 min | 12.1% | about 75 min |
| 20 min | 4.2% | about 85 min |

Match latency stays flat (about 1 to 2 s) at every watermark. Most of the 69 to 85 minutes is the join window
itself; the trade-off shows in the differences between watermarks.

## Rules the testing taught us
- Sweeping the **watermark**: hold the join window at least as long as the longest trip (`--max-trip-minutes`),
  otherwise loss comes from the window, not the watermark. Sweeping the **join window**: hold the watermark wide.
- Both replays share one time base (`--base-event-time`; `run_sweep.sh` does this). Separate bases misalign streams.
- Lateness only registers if `lateness_scale / speed_multiplier` is clearly larger than the 3 s Spark trigger.
  The effective watermark is blurred by about `trigger_seconds x speed_multiplier` (3 minutes at x60).
- Replay time is about (event span + max delay) / speed. The generator keeps only trips fully inside the window.
- A finite replay stalls the watermark at the end. The outer join therefore needs `--tail-hours` extra unscored trips.
- Burst loss varies run to run (flush timing against micro-batch boundaries), so report it over several seeds.
- Spark pitfalls: `trip_id >= / <=` pairs in the join condition break state-cleanup analysis; a post-join filter on
  `trip_id` is pushed back into the join as a second key; Spark 4 ANSI arithmetic overflows on `xxhash64 - xxhash64`
  (cast to `decimal(21,0)` first).

## Layout
```
data/generate_taxi_streams.py   real TLC trips -> pickups, fares, ground truth, with stressors
data/replay_stream.py           replays records into a watched folder at their arrival times
spark/streaming_join.py         Spark join (inner or leftOuter), watermark and window as separate flags
analysis/                       scoring, compounding, pooled test, skew cost, watermark timeline
run_sweep.sh                    one condition end to end
run_matrix*.sh, run_skew_cost.sh  experiment grids
results/                        saved result CSVs
```
