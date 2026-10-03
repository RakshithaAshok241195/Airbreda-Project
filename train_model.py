"""
train_model.py - Day 4 Lab 1: train the NO2 regression on training_data.csv.

    python train_model.py                     # compare candidates, keep the best
    python train_model.py --candidate course  # force the course's exact model
Outputs:
    model.pkl                     the chosen model (baked into the dashboard image)
    model_card.json               what it is, what it was trained on, how good it is
    reports/model_comparison.csv  every candidate's scores - nothing is hidden

HOW WE EVALUATE ON TINY DATA (-> ADR-006)
With fewer than ~20 rows a train/test split is meaningless (a test set of 1-4
rows says almost nothing). Leave-one-out cross-validation (LOO) instead trains on
all rows but one, predicts the one left out, and repeats for every row: every
prediction is on a row the model did NOT see. Each candidate is compared with a
baseline that always predicts the average - a model is only useful if it beats it.

HOW WE "IMPROVE" WITHOUT FOOLING OURSELVES
- Only a few SIMPLE candidates, all linear (the course's model is one of them).
- Every candidate is scored the same way, on unseen rows, and ALL scores are saved.
- The winner is the lowest LOO error; on near-ties the SIMPLER model wins.
- With this little data the ranking itself is noisy: re-run when there's more data.
More complex models (random forests, neural nets) are deliberately NOT tried:
on a handful of rows they memorise noise (Day 4 wrap-up question 1).
"""
import argparse
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.linear_model import LinearRegression, RidgeCV
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import LeaveOneOut, train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

import features

INPUTS = ["total_intensity_veh_per_hr", "hour_of_day"]   # what predict() receives
TARGET = "no2_ug_m3"
MIN_ROWS_FOR_SPLIT = 20
NEAR_TIE = 0.05   # within 5% MAE of the best -> prefer the simpler model

# name -> (factory, complexity rank, description). All take [intensity, hour].
CANDIDATES = {
    "traffic_only": (
        lambda: make_pipeline(FunctionTransformer(features.traffic_only), LinearRegression()),
        1, "Linear: NO2 ~ traffic"),
    "course": (
        lambda: LinearRegression(),
        2, "Linear: NO2 ~ traffic + hour_of_day (the course's model)"),
    "traffic_rush_hour": (
        lambda: make_pipeline(FunctionTransformer(features.traffic_and_rush_hour), LinearRegression()),
        2, "Linear: NO2 ~ traffic + rush-hour flag"),
    "ridge": (
        lambda: make_pipeline(StandardScaler(), RidgeCV(alphas=np.logspace(-2, 3, 30))),
        3, "Ridge (regularised): NO2 ~ traffic + hour_of_day"),
}


def clip(pred):
    """A concentration can't be negative; serving clips too, so we score it the same way."""
    return np.clip(pred, 0, None)


def held_out_scores(factory, X, y):
    """Out-of-sample predictions + MAE/R². LOO below MIN_ROWS_FOR_SPLIT, else a 75/25 split."""
    if len(y) >= MIN_ROWS_FOR_SPLIT:
        X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=42)
        pred = clip(factory().fit(X_tr, y_tr).predict(X_te))
        base = np.full(len(y_te), y_tr.mean())
        return {"method": "train/test split 75/25", "mae": mean_absolute_error(y_te, pred),
                "r2": r2_score(y_te, pred), "baseline_mae": mean_absolute_error(y_te, base)}
    pred, base = np.empty(len(y)), np.empty(len(y))
    for train, test in LeaveOneOut().split(X):
        pred[test] = clip(factory().fit(X.iloc[train], y.iloc[train]).predict(X.iloc[test]))
        base[test] = y.iloc[train].mean()
    return {"method": f"leave-one-out cross-validation (n={len(y)} < {MIN_ROWS_FOR_SPLIT})",
            "mae": mean_absolute_error(y, pred), "r2": r2_score(y, pred),
            "baseline_mae": mean_absolute_error(y, base)}


def choose(results):
    """Lowest held-out MAE; within NEAR_TIE of the best, the simplest wins."""
    best = min(r["held_out"]["mae"] for r in results.values())
    near = [name for name, r in results.items() if r["held_out"]["mae"] <= best * (1 + NEAR_TIE)]
    return min(near, key=lambda name: (CANDIDATES[name][1], results[name]["held_out"]["mae"]))


def describe(model, name):
    """Readable coefficients of the fitted model (last step of a pipeline)."""
    est = model.steps[-1][1] if hasattr(model, "steps") else model
    labels = {"traffic_only": ["total_intensity_veh_per_hr"],
              "traffic_rush_hour": ["total_intensity_veh_per_hr", "rush_hour"],
              "course": INPUTS,
              "ridge": [f"{c} (standardised)" for c in INPUTS]}[name]
    return {"intercept": float(est.intercept_),
            **{label: float(c) for label, c in zip(labels, np.ravel(est.coef_))}}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="training_data.csv")
    parser.add_argument("--candidate", choices=CANDIDATES, help="force one candidate instead of choosing")
    args = parser.parse_args(argv)

    df = pd.read_csv(args.data)
    n = len(df)
    print(f"Training rows: {n}  ({df['timestamp'].min()} -> {df['timestamp'].max()})")
    if n < 4:
        print("Too few rows for a meaningful comparison. Collect more hours first.")
        return 1
    X, y = df[INPUTS], df[TARGET]

    # ---------------------------------------------------------------- compare
    results = {}
    for name, (factory, rank, text) in CANDIDATES.items():
        fitted = factory().fit(X, y)
        results[name] = {
            "description": text, "complexity": rank,
            "in_sample": {"r2": r2_score(y, clip(fitted.predict(X))),
                          "mae": mean_absolute_error(y, clip(fitted.predict(X)))},
            "held_out": held_out_scores(factory, X, y),
        }

    print(f"\n=== Candidates, scored on UNSEEN rows ({results['course']['held_out']['method']}) ===")
    print(f"{'candidate':20} {'held-out MAE':>13} {'held-out R²':>12} {'in-sample R²':>13}   description")
    for name, r in sorted(results.items(), key=lambda kv: kv[1]["held_out"]["mae"]):
        print(f"{name:20} {r['held_out']['mae']:13.2f} {r['held_out']['r2']:12.2f} "
              f"{r['in_sample']['r2']:13.2f}   {r['description']}")
    baseline = results["course"]["held_out"]["baseline_mae"]
    print(f"{'baseline (average)':20} {baseline:13.2f} {'':>12} {'':>13}   always predict the mean")

    chosen = args.candidate or choose(results)
    r = results[chosen]
    beats = r["held_out"]["mae"] < baseline
    print(f"\n→ Chosen: {chosen} ({'forced' if args.candidate else 'lowest held-out error, simplest on ties'})")
    print(f"  Held-out MAE {r['held_out']['mae']:.2f} vs baseline {baseline:.2f} µg/m³: "
          + ("BEATS the baseline." if beats else "does NOT beat the baseline yet - more data needed."))

    # ---------------------------------------------------------------- final fit + save
    model = CANDIDATES[chosen][0]().fit(X, y)       # final model: ALL rows
    coefs = describe(model, chosen)
    print("\n=== Coefficients of the chosen model ===")
    for label, value in coefs.items():
        print(f"  {label:42} {value:10.5f}")
    traffic = coefs.get("total_intensity_veh_per_hr",
                        coefs.get("total_intensity_veh_per_hr (standardised)"))
    if traffic is not None:
        print("  ✓ More traffic → higher predicted NO2, as expected." if traffic > 0 else
              "  ⚠ Traffic coefficient is not positive: report it in the reflection, don't hide it.")

    Path("reports").mkdir(exist_ok=True)
    pd.DataFrame([{"candidate": k, "description": v["description"], "complexity": v["complexity"],
                   "held_out_mae": v["held_out"]["mae"], "held_out_r2": v["held_out"]["r2"],
                   "in_sample_mae": v["in_sample"]["mae"], "in_sample_r2": v["in_sample"]["r2"],
                   "baseline_mae": baseline, "chosen": k == chosen}
                  for k, v in results.items()]).to_csv("reports/model_comparison.csv", index=False)
    joblib.dump(model, "model.pkl")
    card = {
        "chosen_candidate": chosen, "description": r["description"],
        "inputs": INPUTS, "target": TARGET,
        "trained_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "training_rows": n, "training_period_utc": [df["timestamp"].min(), df["timestamp"].max()],
        "coefficients": coefs,
        "metrics": {"in_sample": r["in_sample"], "held_out": r["held_out"],
                    "beats_baseline": bool(beats)},
        "all_candidates": {k: {"held_out_mae": v["held_out"]["mae"], "held_out_r2": v["held_out"]["r2"]}
                           for k, v in results.items()},
        "serving": {"predictions_clipped_at": 0},
        "versions": {"python": platform.python_version(), "scikit-learn": sklearn.__version__,
                     "numpy": np.__version__, "pandas": pd.__version__, "joblib": joblib.__version__},
        "limitations": [f"only {n} training rows; not production-grade",
                        "candidate ranking is itself noisy at this size; re-run with more data",
                        "one 1-minute traffic snapshot per hour",
                        "traffic and time of day are correlated; effects can't be separated yet",
                        "no weather features (wind, temperature), which strongly drive NO2"],
    }
    with open("model_card.json", "w", encoding="utf-8") as f:
        json.dump(card, f, indent=2, default=float)
    print("\nSaved model.pkl, model_card.json, reports/model_comparison.csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())