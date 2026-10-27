# Streaming join project: implementation

Spark Structured Streaming join of NYC TLC Yellow Taxi pickups and fare confirmations
(real January 2026 file), with injectable stressors and ground-truth completeness scoring.
Per professor feedback: **watermark delay** and **join window** are separate flags everywhere,
and "compounding" is computed (`L(AB) > L(A) + L(B)`), not eyeballed.

## Setup (VS Code integrated terminal; needs bash, so on Windows use WSL or Git Bash)
```bash
java -version                      # Java 17 or 21 required by Spark
python -m venv .venv && source .venv/bin/activate
pip install pyspark numpy pandas pyarrow
chmod +x run_sweep.sh
# real data goes at: data/raw/yellow_tripdata_2026-01.parquet
```

## Run one condition end to end (generate, live replay, Spark join, score)
```bash
./run_sweep.sh --condition lateness_only --watermark "15 seconds" --join-window "1 hour" \
  --seed 1 --num-trips 250 --span-hours 0.3 --max-trip-minutes 10 --speed-multiplier 15 \
  --lateness-fraction 0.5 --lateness-scale 90 --results-log ./output/test_results.csv
```
~2.5 min for this size. Output goes to `runs/<condition>/seed<N>/wm_<watermark>/`; one scored row is
appended to the results log. **Use a separate `--results-log` for tests** so they never pollute
`output/results.csv`. Aggregate real runs with `python analysis/compounding_report.py`.
Condition names must be `<stressor>_only` or `<a>+<b>` (e.g. `lateness_only`, `lateness+skew`).

## What is verified (live replay, real Jan 2026 data, seed 1, 250 trips)
| watermark | join window | what it isolates | completeness |
|---|---|---|---|
| 15 sec | 1 hour | narrow watermark + lateness (50% of fares, mean 90s) | **0.836** (209/250) |
| 10 min | 1 hour | same data, wide watermark | **1.000** (250/250) |
| 10 min | 3 min | wide watermark, tight window | **0.104**; 89.6% of trips are longer than 3 min, so the window alone explains the loss |
| 15 sec | 1 hour | skew stressor only (no lateness) | **1.000** (see open issue) |

Single seeds on small samples: these prove the *mechanisms*, they are not results.

## Rules the testing taught us (these protect the attribution the professor asked for)
- Sweeping the **watermark**: hold join window >= max trip duration (cap set by `--max-trip-minutes`),
  otherwise loss comes from the window, not the watermark.
- Sweeping the **join window**: hold the watermark wide.
- Lateness only registers if `lateness_scale / speed_multiplier` is clearly larger than the 3s Spark
  trigger (use >= ~6s). At x300 compression a 120s delay is 0.4s of wall clock and is invisible.
- Replay time ~ (event span + max delay) / speed. A pickup-only window is NOT enough: dropoffs trail
  pickups by trip duration, so the generator keeps only trips fully inside the window.
- Both replays must share one time base (`--base-event-time`; `run_sweep.sh` does this). Separate bases
  replayed fares early and silently weakened lateness.
- Finite replays stall the watermark at the end (min across streams), so late fares at the very tail are
  not evicted. Same bias in every condition; keep the window long relative to the lateness scale.

## PILOT RESULTS (seed 1 only, 250 trips, watermark 15s, join window 1h, real Jan 2026 window; NOT final results)
| condition | what it is | completeness | loss |
|---|---|---|---|
| lateness_only | 50% of fares delayed (mean 90s), random | 0.836 | 0.164 |
| skew_only (delay-based, `--skew-delay-hot-share 0.5`) | all fares from busiest zones (~51% of trips) delayed | 0.872 | 0.128 |
| burst_only (outage then flush, `--burst-fraction 0.3`) | fares held back ~300s then released together | **1.000** (run 1), **0.920** (identical rerun) | **0.000 / 0.080** |
| burst_scatter_control (`--burst-scatter`) | SAME delay values as burst, assigned to random fares | 0.784 | 0.216 |
| lateness+burst | both | 0.728 | 0.272 |
| lateness+skew | both | 0.704 | 0.296 |

Compounding check (additive baseline = L(A)+L(B)): lateness+burst observed 0.272 vs 0.164 (**+0.108**);
lateness+skew observed 0.296 vs 0.292 (**+0.004**). The script prints "n/a (need >=2 seeds)" instead of a
verdict until each condition has 2+ seeds; with one seed the numbers are suggestive only.

## What the pilot suggests (hypotheses to confirm with more seeds)
- **Skew-as-delay is lateness.** Lost % by fare-delay size is identical in both runs (100s+ late: 85% vs 84%).
  Which zone a fare belongs to does not matter; only how late it is.
- **Burst is a different mechanism from lateness (clock freeze VERIFIED).** Same delay values: bunched loses 0-8%,
  scattered loses 21.6%. Logged Spark's watermark (`analysis/watermark_timeline.py`): during the outage the fares
  stream is silent, the join watermark (min across both streams) froze at 351s for 6 batches while pickups kept
  arriving, then jumped to 661s at the flush. Held fares are therefore not late when released.
- **Burst is NOT exactly zero loss, and varies run to run.** An identical rerun lost 8% (20 trips), not 0%. All 20
  were held fares released in the last 12s of the flush (none from the first half): the first part of the flush
  advances the clock, so stragglers that land in a later micro-batch are dropped. Loss depends on how the flush
  straddles micro-batch boundaries (flush window vs trigger_seconds x speed_multiplier), so burst results must be
  reported over several runs. The earlier "+10.8 point" lateness+burst gap used burst = 0 and is probably overstated
  (additive baseline is between 16.4% and ~24%).
- **Compounding appears between the mechanistically different pair** (lateness+burst), not the equivalent pair
  (lateness+skew). One seed; needs repeats.
- Caveat built into the generator: when two stressors hit the same fare their delays are SUMMED. That is a design
  choice and can create compounding mechanically; say so in the report.
- Noise: two runs with the same mechanism differed by 9 fares (3.6 points) from delay-draw luck alone. Treat gaps
  under ~4 points as indistinguishable until there are repeats.
- State-store size was identical (287 rows) in every run because the 1-hour join window keeps all pickups; it does not
  discriminate conditions in this setup.

## SKEW AS LOAD (Option 1 test): `--join-key zone` + `analysis/load_test.py`
Static one-batch test on real Jan 2026 data (4h window, 25,000 trips; amplified = busiest 3 zones x5 = 36,860 trips).
Same data joined two ways. Run: `python analysis/load_test.py --join-key zone --amplify 5 --topk 3 --name D_zone_amp5`
| join keyed by | normal data | busiest 3 zones x5 | candidate checks (normal -> amplified) |
|---|---|---|---|
| trip_id (current) | 11.6s | 12.8s | 25k -> 37k |
| pickup zone | 20.4s | **60.3s** | 14M -> 85M |
All cases matched 100% of trips. Single run each, 1 CPU core, no lateness: shows skew costs TIME when the key is
non-unique and does nothing when the key is unique. Untested: effect on lost trips when data is also late (a slower
join might even lose fewer, since it works in bigger batches), and the multi-core "one overloaded worker" effect.
Spark pitfalls hit while building it (do not repeat): (1) `trip_id >= / <=` pairs in the join condition make Spark fail
to derive its state-cleanup rule from the time range (warning "Error trying to extract state constraint");
(2) a post-join `.where(trip_id == trip_id)` is pushed back into the join and becomes a second key, silently
removing the skew; (3) Spark 4 ANSI arithmetic overflows on `xxhash64 - xxhash64`, so the hashes are cast to decimal first.
The final form is `(xxhash64(a) - xxhash64(b)) == 0`, which cannot become a join key; check the physical plan shows
`StreamingSymmetricHashJoin [PULocationID], [PULocationID]` and no state-constraint warnings.

## Stressor definitions as implemented
- lateness: random `--lateness-fraction` of fares get an exponential extra delay (`--lateness-scale-seconds`).
- skew (original `--skew-amplify-factor`): extra trips from hot zones. Cannot affect completeness (unique join key,
  verified 100%). Kept only for reference.
- skew (`--skew-delay-hot-share`): fares from the smallest set of zones covering that share of trips are delayed.
- burst: `--burst-fraction` = share of the event span covered by an outage; fares in it are released within
  `--burst-window-seconds` after it ends. `--burst-scatter` is the matched control.

Observed (likely micro-batch granularity): at x15 speed with a 3s trigger, fares up to ~45s past the nominal
15s watermark were still matched. Treat watermarks smaller than roughly `trigger_seconds * speed_multiplier` as blurred.

## Not done / not tested
Kafka sink (file replay only so far), the full condition x watermark x seed matrix, and the adaptive
watermark. `run_matrix.sh` is intentionally not written yet because it depends on the skew decision.
