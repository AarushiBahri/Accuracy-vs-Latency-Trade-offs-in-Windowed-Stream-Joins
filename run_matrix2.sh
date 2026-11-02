#!/usr/bin/env bash
# Follow-up grid: intermediate watermarks + seed 3, focused on the lateness+burst pair (the only pair with a non-zero interaction).
cd "$(dirname "$0")"
COMMON=(--join-window "2 hours" --num-trips 3000 --span-hours 1 --speed-multiplier 60 --results-log ./output/matrix2_results.csv)
L=(--lateness-fraction 0.5 --lateness-scale 600)
B=(--burst-fraction 0.3 --burst-window 300)
declare -A COND=([lateness_only]="${L[*]}" [burst_only]="${B[*]}" [lateness+burst]="${L[*]} ${B[*]}")
ORDER=(lateness_only burst_only lateness+burst)
# (watermark, seeds): new watermarks get seeds 1-3; the original two get the missing seed 3
JOBS=("8 minutes:1 2 3" "10 minutes:1 2 3" "14 minutes:1 2 3" "5 minutes:3" "20 minutes:3")
mkdir -p output
for job in "${JOBS[@]}"; do
  wm="${job%%:*}"; seeds="${job##*:}"
  for seed in $seeds; do for c in "${ORDER[@]}"; do
    if grep -q "^$c,$wm,2 hours,$seed," output/matrix2_results.csv 2>/dev/null; then continue; fi  # resume: skip finished runs
    echo "##### $c seed=$seed wm=$wm $(date +%T)"
    ./run_sweep.sh --condition "$c" --watermark "$wm" --seed "$seed" "${COMMON[@]}" ${COND[$c]} > "output/log2_${c}_s${seed}_${wm// /_}.txt" 2>&1 \
      || echo "FAILED: $c seed=$seed wm=$wm" | tee -a output/matrix2_failures.txt
    d="runs/$c/seed$seed/wm_${wm// /_}"; rm -rf "$d/stream" "$d/chk"
    pkill -f streaming_join.py 2>/dev/null; sleep 2
  done; done
done
echo "MATRIX2 DONE $(date +%T)"
