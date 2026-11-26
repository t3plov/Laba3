"""Unit-тесты ключевой бизнес-логики: обнаружение аномалий и прогноз."""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from shared import ml


def test_detects_spike():
    timestamps = [f"2026-01-01T00:{i:02d}:00Z" for i in range(40)]
    values = [50.0 + (i % 3) for i in range(40)]
    values[35] = 500.0
    anomalies = ml.detect_anomalies(timestamps, values, window=20, threshold=3.0)
    assert anomalies, "всплеск должен быть обнаружен"
    assert anomalies[0].index == 35
    assert anomalies[0].reason == "value_deviation"
    assert anomalies[0].score > 3.0


def test_stable_series_has_no_anomalies():
    timestamps = [f"2026-01-01T00:{i:02d}:00Z" for i in range(40)]
    values = [42.0 for _ in range(40)]
    assert ml.detect_anomalies(timestamps, values) == []


def test_forecast_follows_trend():
    values = [float(i) for i in range(30)]
    points = ml.forecast(values, horizon=5)
    assert len(points) == 5
    assert points[0] > values[-1]
    assert abs(points[0] - 30.0) < 1e-6


def test_train_model_statistics():
    model = ml.train_model([1.0, 2.0, 3.0, 4.0])
    assert model["mean"] == 2.5
    assert model["window"] == 20