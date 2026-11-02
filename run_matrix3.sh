#!/usr/bin/env bash
# Left-outer-join grid: completeness AND time-to-declare-unmatched, per watermark. Resumable.
cd "$(dirname "$0")"
LOG=./output/outer_results.csv
COMMON=(--join-window "1 hour" --num-trips 3000 --span-hours 1 --speed-multiplier 60 --join-type leftOuter --tail-hours 2.5 --results-log $LOG)
L=(--lateness-fraction 0.5 --lateness-scale 600)
B=(--burst-fraction 0.3 --burst-window 300)
declare -A COND=([baseline]="" [lateness_only]="${L[*]}" [burst_only]="${B[*]}" [lateness+burst]="${L[*]} ${B[*]}")
mkdir -p output
run() { # cond wm seed
  c="$1"; wm="$2"; seed="$3"
  if grep -q "^$c,$wm,1 hour,$seed," $LOG 2>/dev/null; then return; fi
  echo "##### $c seed=$seed wm=$wm $(date +%T)"
  ./run_sweep.sh --condition "$c" --watermark "$wm" --seed "$seed" "${COMMON[@]}" ${COND[$c]} > "output/log4_${c}_s${seed}_${wm// /_}.txt" 2>&1 \
    || echo "FAILED: $c seed=$seed wm=$wm" | tee -a output/outer_failures.txt
  d="runs/$c/seed$seed/wm_${wm// /_}"; rm -rf "$d/stream" "$d/chk"; pkill -f streaming_join.py 2>/dev/null; sleep 2
}
for wm in "5 minutes" "20 minutes"; do run baseline "$wm" 1; done
for seed in 1 2; do for wm in "5 minutes" "10 minutes" "20 minutes"; do for c in lateness_only burst_only lateness+burst; do
  run "$c" "$wm" "$seed"
done; done; done
echo "OUTER DONE $(date +%T)"
