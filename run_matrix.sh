#!/usr/bin/env bash
# Full experiment grid: 7 conditions x seeds x watermarks. Never aborts on a failed run; logs failures.
cd "$(dirname "$0")"
SEEDS=(1 2)
WMS=("5 minutes" "20 minutes")
COMMON=(--join-window "2 hours" --num-trips 3000 --span-hours 1 --speed-multiplier 60 --results-log ./output/matrix_results.csv)
L=(--lateness-fraction 0.5 --lateness-scale 600)
B=(--burst-fraction 0.3 --burst-window 300)
S=(--skew-amplify-factor 4 --skew-top-k 5)
declare -A COND=(
  [baseline]=""
  [lateness_only]="${L[*]}" [burst_only]="${B[*]}" [skew_only]="${S[*]}"
  [lateness+burst]="${L[*]} ${B[*]}" [lateness+skew]="${L[*]} ${S[*]}" [skew+burst]="${S[*]} ${B[*]}"
)
ORDER=(baseline lateness_only burst_only skew_only lateness+burst lateness+skew skew+burst)
mkdir -p output
for seed in "${SEEDS[@]}"; do for wm in "${WMS[@]}"; do for c in "${ORDER[@]}"; do
  echo "##### $c seed=$seed wm=$wm $(date +%T)"
  ./run_sweep.sh --condition "$c" --watermark "$wm" --seed "$seed" "${COMMON[@]}" ${COND[$c]} > "output/log_${c}_s${seed}_${wm// /_}.txt" 2>&1 \
    || echo "FAILED: $c seed=$seed wm=$wm" | tee -a output/matrix_failures.txt
  d="runs/$c/seed$seed/wm_${wm// /_}"; rm -rf "$d/stream" "$d/chk"   # keep data, output, metrics
  pkill -f streaming_join.py 2>/dev/null; sleep 2
done; done; done
echo "MATRIX DONE $(date +%T)"
