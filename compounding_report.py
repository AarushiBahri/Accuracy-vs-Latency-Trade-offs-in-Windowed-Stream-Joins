"""
Reads the accumulated results.csv (one row per run, written by score_run.py)
and produces two things:

1. COMPOUNDING CHECK (formal definition, matching the proposal exactly):
   For each combined condition "A+B", let L(A) and L(B) be the mean
   completeness loss of the two single-stressor conditions at the SAME
   watermark/join_window, and L(AB) the mean loss of the combined condition.
   additive_baseline = L(A) + L(B)
   interaction        = L(AB) - additive_baseline
   Flagged COMPOUNDING only if interaction exceeds a noise threshold derived
   from the run-to-run standard deviation across seeds (a lightweight
   heuristic, not a formal hypothesis test -- swap in a proper t-test once
   real seed counts are large enough to support one).

2. SENSITIVITY RANKING:
   For each single-stressor condition, the slope of completeness loss
   against watermark duration (loss lost per second of WATERMARK
   tightening -- explicitly the watermark parameter, not join_window,
   per reviewer feedback), computed between the widest and narrowest
   watermark tested for that condition. Conditions are ranked by |slope|.

Usage:
  python compounding_report.py --results-log ./output/results.csv
"""
import argparse
import csv
import re
from collections import defaultdict
from statistics import mean, pstdev


def parse_interval_seconds(s: str) -> float:
    s = s.strip().lower()
    m = re.match(r"([\d.]+)\s*(second|minute|hour)s?", s)
    if not m:
        raise ValueError(f"Can't parse interval: {s!r}")
    val, unit = float(m.group(1)), m.group(2)
    mult = {"second": 1, "minute": 60, "hour": 3600}[unit]
    return val * mult


def load_results(path: str):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            r["watermark_s"] = parse_interval_seconds(r["watermark"])
            r["join_window_s"] = parse_interval_seconds(r["join_window"])
            r["loss"] = float(r["loss"])
            rows.append(r)
    return rows


def group_mean_std(rows, keyfn):
    groups = defaultdict(list)
    for r in rows:
        groups[keyfn(r)].append(r["loss"])
    return {k: (mean(v), pstdev(v) if len(v) > 1 else 0.0, len(v)) for k, v in groups.items()}


def compounding_check(rows):
    print("\n=== COMPOUNDING CHECK ===")
    print(f"{'combined':<16} {'@ wm/win':<22} {'L(A)':>8} {'L(B)':>8} {'additive':>9} {'L(AB)':>8} {'interaction':>12}  verdict")

    combined_conditions = sorted({r["condition"] for r in rows if "+" in r["condition"]})
    by_condition_config = group_mean_std(rows, lambda r: (r["condition"], r["watermark"], r["join_window"]))

    for combo in combined_conditions:
        a_name, b_name = combo.split("+")
        configs = {(c, wm, jw) for (c, wm, jw) in by_condition_config if c == combo}
        for (_, wm, jw) in configs:
            key_ab = (combo, wm, jw)
            key_a = (f"{a_name}_only", wm, jw)
            key_b = (f"{b_name}_only", wm, jw)
            if key_a not in by_condition_config or key_b not in by_condition_config:
                continue
            la_mean, la_std, la_n = by_condition_config[key_a]
            lb_mean, lb_std, lb_n = by_condition_config[key_b]
            lab_mean, lab_std, lab_n = by_condition_config[key_ab]

            additive = la_mean + lb_mean
            interaction = lab_mean - additive
            noise = (la_std + lb_std + lab_std) or 1e-9
            threshold = 2 * noise / max(min(la_n, lb_n, lab_n), 1) ** 0.5

            if min(la_n, lb_n, lab_n) < 2:
                verdict = "n/a (need >=2 seeds)"
            elif interaction > threshold:
                verdict = "COMPOUNDING"
            elif interaction < -threshold:
                verdict = "sub-additive"
            else:
                verdict = "~additive"

            print(f"{combo:<16} {wm}/{jw:<12} {la_mean:>8.4f} {lb_mean:>8.4f} {additive:>9.4f} "
                  f"{lab_mean:>8.4f} {interaction:>+12.4f}  {verdict}")


def sensitivity_ranking(rows):
    print("\n=== SENSITIVITY RANKING (loss per second of WATERMARK tightening) ===")
    singles = [r for r in rows if r["condition"].endswith("_only")]
    by_condition = defaultdict(list)
    for r in singles:
        by_condition[r["condition"]].append(r)

    results = []
    for cond, rs in by_condition.items():
        by_wm = group_mean_std(rs, lambda r: r["watermark_s"])
        if len(by_wm) < 2:
            print(f"  {cond}: need >=2 distinct watermark values tested, have {len(by_wm)} -- skipping")
            continue
        wm_seconds = sorted(by_wm.keys())
        wide_wm, narrow_wm = wm_seconds[-1], wm_seconds[0]
        loss_wide = by_wm[wide_wm][0]
        loss_narrow = by_wm[narrow_wm][0]
        slope = (loss_narrow - loss_wide) / (wide_wm - narrow_wm)  # loss gained per second removed
        results.append((cond, slope))

    results.sort(key=lambda x: -abs(x[1]))
    for rank, (cond, slope) in enumerate(results, 1):
        print(f"  {rank}. {cond:<14} {slope:+.6f} completeness-loss per second of watermark tightening")

    if len(results) >= 2:
        worst, second = results[0], results[1]
        if second[1] != 0:
            ratio = abs(worst[1] / second[1])
            print(f"\n  -> {worst[0]} is {ratio:.1f}x more damaging than {second[0]} at this watermark range.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-log", default="./output/results.csv")
    args = ap.parse_args()
    rows = load_results(args.results_log)
    print(f"Loaded {len(rows)} runs from {args.results_log}")
    compounding_check(rows)
    sensitivity_ranking(rows)


if __name__ == "__main__":
    main()
