"""Pooled test of the lateness+burst interaction I = L(AB) - L(A) - L(B), per (watermark, seed).
Usage: python analysis/pooled_interaction.py output/merged_results.csv   (needs pandas, scipy)"""
import sys, numpy as np, pandas as pd
from scipy import stats
d = pd.read_csv(sys.argv[1]); d["wm"] = d.watermark.str.split().str[0].astype(int)
p = d.pivot_table(index=["wm", "seed"], columns="condition", values="loss")
p["I"] = p["lateness+burst"] - p["lateness_only"] - p["burst_only"]; pr = p.reset_index()
def test(sub, label):
    x = sub.I.values; rng = np.random.default_rng(0)
    b = [np.mean([rng.choice(g.I.values, len(g)).mean() for _, g in sub.groupby("wm")]) for _ in range(5000)]
    print(f"{label}: n={len(x)} mean={x.mean():+.4f} t p={stats.ttest_1samp(x,0).pvalue:.4f} "
          f"wilcoxon p={stats.wilcoxon(x).pvalue:.4f} boot95=[{np.percentile(b,2.5):+.4f},{np.percentile(b,97.5):+.4f}]")
test(pr[pr.wm >= 8], "wm>=8 (post hoc split)"); test(pr, "all watermarks")
print(pr.groupby("wm").I.mean().round(4).to_dict())
