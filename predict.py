"""
predict.py - turn the trained model into an NO2 prediction + exceedance risk (Day 4).

    from predict import predict
    predict(total_intensity_veh_per_hr=4200, hour_of_day=8)
    -> {"no2_ug_m3_predicted": 27.3, "no2_exceedance_risk": 0.61}

Imported directly by the dashboard: no separate model server for a model this size.
model.pkl is loaded once, the first time it's needed, and then reused.

EXCEEDANCE THRESHOLD: 25 µg/m³  (required reasoning, Day 4) 
Candidates considered:
  - EU limit value, 200 µg/m³ as a 1-hour mean: designed for short pollution
    episodes. Our hourly values are 10-35 µg/m³, so the risk would always be ~0.
  - EU limit value, 40 µg/m³ as an annual mean (20 µg/m³ from 2030).
  - WHO 2021 guideline, 25 µg/m³ as a 24-hour mean, and 10 µg/m³ as an annual mean.
We use 25 µg/m³: health-based, and in the middle of the values this station
actually measures, so the risk score varies meaningfully instead of being stuck
near 0 or 1. Honest caveat: it's a 24-HOUR guideline applied to HOURLY
predictions. An hour above 25 does not mean the guideline was breached; it means
"this hour contributes towards a breach". The score is an indicator, not a
legal or medical judgement.

RISK = sigmoid centred on the threshold (the course's formula, steepness 0.2):
    predicted 15 -> 0.12    predicted 25 -> 0.50    predicted 35 -> 0.88
"""
import math
import os
from functools import lru_cache
from pathlib import Path

import pandas as pd

MODEL_PATH = Path(os.environ.get("MODEL_PATH", Path(__file__).with_name("model.pkl")))
INPUTS = ["total_intensity_veh_per_hr", "hour_of_day"]   # same order the model was trained on

THRESHOLD_UG_M3 = float(os.environ.get("NO2_THRESHOLD", "25"))
STEEPNESS = 0.2


@lru_cache(maxsize=1)
def load_model():
    """Load model.pkl once per process (the dashboard calls predict() on every request)."""
    import joblib
    return joblib.load(MODEL_PATH)


def exceedance_risk(predicted_no2, threshold=THRESHOLD_UG_M3, steepness=STEEPNESS):
    """The course's sigmoid: 0.5 exactly at the threshold, -> 0 below, -> 1 above."""
    return float(1 / (1 + math.exp(-steepness * (predicted_no2 - threshold))))


def predict(total_intensity_veh_per_hr, hour_of_day):
    """Predicted NO2 (µg/m³, never below 0) and the 0-1 risk of exceeding the threshold.

    Raises ValueError on impossible inputs, so the caller (the dashboard) can
    decide how to degrade instead of serving a nonsense prediction.
    """
    if total_intensity_veh_per_hr is None or not math.isfinite(float(total_intensity_veh_per_hr)):
        raise ValueError("total_intensity_veh_per_hr must be a number")
    if float(total_intensity_veh_per_hr) < 0:
        raise ValueError("total_intensity_veh_per_hr can't be negative")
    if int(hour_of_day) != hour_of_day or not 0 <= int(hour_of_day) <= 23:
        raise ValueError("hour_of_day must be an integer 0-23")

    X = pd.DataFrame([[float(total_intensity_veh_per_hr), int(hour_of_day)]], columns=INPUTS)
    raw = float(load_model().predict(X)[0])
    # A linear model can go below zero outside the hours it was trained on (the
    # hour_of_day term, at night). A concentration can't: clip, as in training.
    no2 = max(0.0, raw)
    return {"no2_ug_m3_predicted": round(no2, 2),
            "no2_exceedance_risk": round(exceedance_risk(no2), 4)}
