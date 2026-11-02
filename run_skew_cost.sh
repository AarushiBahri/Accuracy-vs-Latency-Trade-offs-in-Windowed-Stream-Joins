#!/usr/bin/env bash
# Option 2: zone-keyed join. Skew amplification x1/x5/x10 on the 3 busiest zones, with and without lateness.
# Waits for the earlier grid so CPU contention doesn't distort batch timings.
cd "$(dirname "$0")"
until grep -q "MATRIX2 DONE" output/matrix2.log 2>/dev/null; do sleep 30; done
mkdir -p output
for seed in 1 2; do for f in 1 5 10; do for lat in 0 1; do
  c="zoneskew_x${f}"; extra=(); [ $lat = 1 ] && { c="${c}_late"; extra=(--lateness-fraction 0.5 --lateness-scale 600); }
  if grep -q "^$c,5 minutes,2 hours,$seed," output/skewcost_results.csv 2>/dev/null; then continue; fi  # resume
  echo "##### $c seed=$seed $(date +%T)"
  ./run_sweep.sh --condition "$c" --watermark "5 minutes" --join-window "2 hours" --seed $seed --num-trips 6000 \
    --span-hours 1 --speed-multiplier 60 --skew-top-k 3 --skew-amplify-factor $f --join-key zone \
    --results-log ./output/skewcost_results.csv "${extra[@]}" > "output/log3_${c}_s${seed}.txt" 2>&1 || echo "FAILED $c $seed" >> output/skewcost_failures.txt
  d="runs/$c/seed$seed/wm_5_minutes"; rm -rf "$d/stream" "$d/chk"; pkill -f streaming_join.py 2>/dev/null; sleep 2
done; done; done
echo "SKEWCOST DONE $(date +%T)"
