"""
explore_training_data.py - answer the Day 4 questions honestly before training.

  python explore_training_data.py            -> prints the answers, saves the plot
Output: reports/no2_vs_traffic.png
"""
import sys
from pathlib import Path

import pandas as pd


def main(path="training_data.csv"):
    df = pd.read_csv(path)
    print(f"Joined rows: {len(df)}")
    if df.empty:
        print("No rows yet - let the pipeline collect more hours, then re-run build_training_data.py.")
        return 0
    print(f"Hours covered: {df['timestamp'].min()} -> {df['timestamp'].max()} (UTC window starts)")
    print(f"Distinct local hours of day: {sorted(df['hour_of_day'].unique().tolist())}")
    print(df[["no2_ug_m3", "total_intensity_veh_per_hr"]].describe().round(1).to_string())
    if len(df) >= 3:
        r = df["no2_ug_m3"].corr(df["total_intensity_veh_per_hr"])
        print(f"\nPearson correlation NO2 vs total intensity: {r:.2f}  (n = {len(df)})")
        print("With this few points, treat the sign and size of r as a hint, not a finding.")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    Path("reports").mkdir(exist_ok=True)
    fig, ax = plt.subplots(figsize=(7, 4.5))
    points = ax.scatter(df["total_intensity_veh_per_hr"], df["no2_ug_m3"],
                        c=df["hour_of_day"], cmap="viridis", s=60, edgecolors="black")
    fig.colorbar(points, ax=ax, label="hour of day (local)")
    ax.set_xlabel("Total traffic intensity, 4 A27 sites (veh/h)")
    ax.set_ylabel("NO₂ at NL10240 (µg/m³)")
    ax.set_title(f"NO₂ vs traffic at the A27/Breda interchange (n = {len(df)})")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig("reports/no2_vs_traffic.png", dpi=150)
    print("\nSaved reports/no2_vs_traffic.png")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
