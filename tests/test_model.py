"""Day 4: tests for model.pkl and predict(). The first two are the course's."""
import math

import pytest

import predict as p


def test_model_pkl_loads_without_error():                          # course test 1
    model = p.load_model()
    assert hasattr(model, "predict")


def test_predict_returns_plausible_no2_and_risk_between_0_and_1():  # course test 2
    result = p.predict(total_intensity_veh_per_hr=3500, hour_of_day=10)
    assert set(result) == {"no2_ug_m3_predicted", "no2_exceedance_risk"}
    assert isinstance(result["no2_ug_m3_predicted"], float)
    assert 0 <= result["no2_ug_m3_predicted"] <= 200
    assert 0 <= result["no2_exceedance_risk"] <= 1


@pytest.mark.parametrize("intensity", [0, 60, 1000, 4000, 8000])
@pytest.mark.parametrize("hour", [0, 3, 8, 12, 17, 23])
def test_predictions_stay_plausible_at_every_hour_and_traffic_level(intensity, hour):
    """Covers night hours the model never saw: clipping keeps NO2 >= 0."""
    result = p.predict(intensity, hour)
    assert 0 <= result["no2_ug_m3_predicted"] <= 200
    assert 0 <= result["no2_exceedance_risk"] <= 1


def test_risk_is_one_half_exactly_at_the_threshold():
    assert p.exceedance_risk(p.THRESHOLD_UG_M3) == pytest.approx(0.5)


def test_risk_rises_with_predicted_no2():
    risks = [p.exceedance_risk(v) for v in (5, 15, 25, 35, 45)]
    assert risks == sorted(risks) and risks[0] < 0.1 and risks[-1] > 0.9


@pytest.mark.parametrize("intensity, hour", [(-10, 8), (float("nan"), 8), (None, 8), (1000, 24), (1000, 7.5)])
def test_impossible_inputs_are_rejected(intensity, hour):
    with pytest.raises(ValueError):
        p.predict(intensity, hour)
