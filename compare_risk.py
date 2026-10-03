"""
compare_risk.py - Day 4 stretch goal: two ways to get an exceedance risk.

  A. Regression + sigmoid   (what predict.py does)
  B. LogisticRegression trained directly on "did this hour exceed the threshold?"

    python compare_risk.py
Prints both risks for every real row, and saves reports/risk_comparison.csv.
"""
import sys
from pathlib import Path

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from predict import INPUTS, THRESHOLD_UG_M3, predict


def main(path="training_data.csv"):
    df = pd.read_csv(path)
    df["exceeded"] = (df["no2_ug_m3"] > THRESHOLD_UG_M3).astype(int)
    n_pos, n = int(df["exceeded"].sum()), len(df)
    print(f"Threshold {THRESHOLD_UG_M3:g} µg/m³: {n_pos} of {n} hours exceeded it.\n")

    df["risk_regression_sigmoid"] = [predict(r.total_intensity_veh_per_hr, int(r.hour_of_day))["no2_exceedance_risk"]
                                     for r in df.itertuples()]
    if min(n_pos, n - n_pos) < 2:
        df["risk_logistic"] = None
        print("LogisticRegression NOT trained: it needs at least 2 hours on EACH side of the threshold\n"
              "(here: {} above, {} below). A classifier can't learn a boundary from 0 or 1 examples.\n"
              .format(n_pos, n - n_pos))
    else:
        clf = make_pipeline(StandardScaler(), LogisticRegression()).fit(df[INPUTS], df["exceeded"])
        df["risk_logistic"] = clf.predict_proba(df[INPUTS])[:, 1].round(4)

    cols = ["timestamp", "hour_of_day", "total_intensity_veh_per_hr", "no2_ug_m3", "exceeded",
            "risk_regression_sigmoid", "risk_logistic"]
    print(df[cols].to_string(index=False))
    Path("reports").mkdir(exist_ok=True)
    df[cols].to_csv("reports/risk_comparison.csv", index=False)
    print("\nSaved reports/risk_comparison.csv")
    print("\nWhich to trust (✏️ ADR-006): the regression uses every row's actual NO2 value;\n"
          "the classifier only sees above/below, throwing information away - with this\n"
          f"little data ({n} rows, {n_pos} exceedances) that matters a lot.")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
