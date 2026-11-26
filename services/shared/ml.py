"""ML-часть сервиса анализа: обнаружение аномалий и прогноз временного ряда.

Используются методы, работающие без размеченных данных:

* аномалии — z-score по скользящему окну (адаптивный порог, а не жёстко заданный);
* прогноз — линейная регрессия по последним точкам ряда.

Модель сериализуется в JSON-артефакт, чтобы продемонстрировать жизненный цикл модели.
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from typing import Any


@dataclass
class AnomalyPoint:
    index: int
    timestamp: str
    value: float
    score: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "value": self.value,
            "score": round(self.score, 4),
            "reason": self.reason,
        }


def _window_stats(values: list[float], start: int, end: int) -> tuple[float, float]:
    window = values[start:end]
    if not window:
        return 0.0, 0.0
    mean = sum(window) / len(window)
    variance = sum((v - mean) ** 2 for v in window) / len(window)
    return mean, math.sqrt(variance)


def detect_anomalies(
    timestamps: list[str],
    values: list[float],
    window: int = 20,
    threshold: float = 3.0,
) -> list[AnomalyPoint]:
    """Z-score относительно скользящего окна истории перед точкой."""
    anomalies: list[AnomalyPoint] = []
    for i in range(min(window, len(values)), len(values)):
        mean, std = _window_stats(values, max(0, i - window), i)
        if std <= 1e-9:
            continue
        score = abs(values[i] - mean) / std
        if score >= threshold:
            reason = "value_deviation" if values[i] > mean else "value_drop"
            anomalies.append(AnomalyPoint(i, timestamps[i], values[i], score, reason))
    return anomalies


def forecast(values: list[float], horizon: int, tail: int = 30) -> list[float]:
    """Линейный прогноз по последним точкам ряда (метод наименьших квадратов)."""
    if horizon <= 0 or not values:
        return []
    series = values[-tail:]
    n = len(series)
    if n < 2:
        return [series[0] if series else 0.0] * horizon
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(series) / n
    denominator = sum((x - mean_x) ** 2 for x in xs)
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, series)) / denominator if denominator else 0.0
    intercept = mean_y - slope * mean_x
    return [intercept + slope * (n + step) for step in range(horizon)]


def train_model(values: list[float], window: int = 20, threshold: float = 3.0) -> dict[str, Any]:
    mean, std = _window_stats(values, 0, len(values))
    return {"algorithm": "rolling-zscore", "window": window, "threshold": threshold, "mean": mean, "std": std}


def save_model(artifact_dir: str, analysis_id: str, model: dict[str, Any]) -> str:
    os.makedirs(artifact_dir, exist_ok=True)
    path = os.path.join(artifact_dir, f"{analysis_id}.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(model, handle, ensure_ascii=False, indent=2)
    return path


def load_model(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)
