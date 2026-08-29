"""Time-series anomaly detection with multi-algorithm voting.

Algorithms:
- 3-Sigma: detects sudden spikes for near-normal distributions.
- EWMA: detects gradual drift and trend changes.
- Isolation Forest: detects multivariate anomalies without distribution
  assumptions.
"""

import logging
from dataclasses import dataclass

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class AnomalyResult:
    is_anomaly: bool
    score: float
    algorithm: str
    threshold: float
    current_value: float
    expected_value: float
    detail: str = ""


class ThreeSigmaDetector:
    def __init__(self, sigma_multiplier: float = 3.0):
        self._sigma = sigma_multiplier

    def detect(self, values: list[float], current: float) -> AnomalyResult:
        if len(values) < 10:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                algorithm="3-sigma",
                threshold=self._sigma,
                current_value=current,
                expected_value=current,
                detail="Insufficient data points",
            )

        arr = np.array(values)
        mean = float(arr.mean())
        std = float(arr.std())

        if std == 0:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                algorithm="3-sigma",
                threshold=self._sigma,
                current_value=current,
                expected_value=mean,
            )

        z_score = abs(current - mean) / std
        return AnomalyResult(
            is_anomaly=z_score > self._sigma,
            score=round(z_score, 3),
            algorithm="3-sigma",
            threshold=self._sigma,
            current_value=current,
            expected_value=round(mean, 3),
            detail=f"Z-score={z_score:.3f}, mean={mean:.3f}, std={std:.3f}",
        )


class EWMADetector:
    def __init__(self, alpha: float = 0.3, threshold: float = 3.0):
        self._alpha = alpha
        self._threshold = threshold

    def detect(self, values: list[float], current: float) -> AnomalyResult:
        if len(values) < 5:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                algorithm="ewma",
                threshold=self._threshold,
                current_value=current,
                expected_value=current,
                detail="Insufficient data points",
            )

        ewma = values[0]
        ewma_var = 0.0
        for v in values[1:]:
            diff = v - ewma
            ewma = self._alpha * v + (1 - self._alpha) * ewma
            ewma_var = self._alpha * diff**2 + (1 - self._alpha) * ewma_var

        ewma_std = float(np.sqrt(ewma_var))
        if ewma_std == 0:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                algorithm="ewma",
                threshold=self._threshold,
                current_value=current,
                expected_value=round(ewma, 3),
            )

        deviation = abs(current - ewma) / ewma_std
        return AnomalyResult(
            is_anomaly=deviation > self._threshold,
            score=round(deviation, 3),
            algorithm="ewma",
            threshold=self._threshold,
            current_value=current,
            expected_value=round(ewma, 3),
            detail=f"deviation={deviation:.3f}, ewma={ewma:.3f}, ewma_std={ewma_std:.3f}",
        )


class IsolationForestDetector:
    def __init__(self, contamination: float = 0.05):
        self._contamination = contamination

    def detect(self, values: list[float], current: float) -> AnomalyResult:
        if len(values) < 20:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                algorithm="isolation_forest",
                threshold=0.0,
                current_value=current,
                expected_value=current,
                detail="Insufficient data points (need >= 20)",
            )

        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                algorithm="isolation_forest",
                threshold=0.0,
                current_value=current,
                expected_value=current,
                detail="scikit-learn not installed",
            )

        data = np.array(values).reshape(-1, 1)
        current_arr = np.array([[current]])

        clf = IsolationForest(
            contamination=self._contamination,
            random_state=42,
            n_estimators=100,
        )
        clf.fit(data)

        prediction = clf.predict(current_arr)
        score = float(-clf.decision_function(current_arr)[0])

        return AnomalyResult(
            is_anomaly=prediction[0] == -1,
            score=round(score, 3),
            algorithm="isolation_forest",
            threshold=self._contamination,
            current_value=current,
            expected_value=round(float(np.median(values)), 3),
            detail=f"anomaly_score={score:.3f}, prediction={prediction[0]}",
        )


class MultiDimensionalIFDetector:
    def __init__(self, contamination: float = 0.05):
        self._contamination = contamination

    def detect(
        self, feature_matrix: list[list[float]], current_features: list[float]
    ) -> AnomalyResult:
        if len(feature_matrix) < 20:
            return AnomalyResult(
                is_anomaly=False,
                score=0.0,
                algorithm="multi_dim_if",
                threshold=0.0,
                current_value=0.0,
                expected_value=0.0,
                detail="Insufficient data",
            )

        from sklearn.ensemble import IsolationForest

        data = np.array(feature_matrix)
        current_arr = np.array([current_features])

        clf = IsolationForest(
            contamination=self._contamination,
            random_state=42,
        )
        clf.fit(data)

        prediction = clf.predict(current_arr)
        score = float(-clf.decision_function(current_arr)[0])

        return AnomalyResult(
            is_anomaly=prediction[0] == -1,
            score=round(score, 3),
            algorithm="multi_dim_if",
            threshold=self._contamination,
            current_value=current_features[0] if current_features else 0.0,
            expected_value=0.0,
            detail=f"multi-dim score={score:.3f}, dims={len(current_features)}",
        )


class EnsembleDetector:
    """Voting-based detector: at least `min_votes` algorithms must agree."""

    def __init__(self, min_votes: int = 2):
        self._detectors = [
            ThreeSigmaDetector(sigma_multiplier=3.0),
            EWMADetector(alpha=0.3, threshold=3.0),
            IsolationForestDetector(contamination=0.05),
        ]
        self._min_votes = min_votes

    def detect(
        self, values: list[float], current: float
    ) -> tuple[bool, float, list[AnomalyResult]]:
        results = [d.detect(values, current) for d in self._detectors]
        votes = sum(1 for r in results if r.is_anomaly)
        max_score = max((r.score for r in results), default=0.0)
        is_anomaly = votes >= self._min_votes

        if is_anomaly:
            logger.info(
                "[EnsembleDetector] ANOMALY: votes=%d/%d, score=%.3f, value=%s",
                votes,
                len(self._detectors),
                max_score,
                current,
            )

        return is_anomaly, max_score, results


def generate_demo_metrics(
    n_points: int = 200, inject_anomaly: bool = True
) -> tuple[list[float], list[float]]:
    """Generate sine + trend + noise metrics, optionally with injected spikes."""
    np.random.seed(42)
    t = np.arange(n_points)

    trend = 50 + 0.05 * t
    seasonal = 10 * np.sin(2 * np.pi * t / 24)
    noise = np.random.normal(0, 2, n_points)

    values = trend + seasonal + noise

    anomalies = []
    if inject_anomaly:
        for idx in [150, 170, 185]:
            if idx < n_points:
                values[idx] += np.random.choice([-1, 1]) * 30
                anomalies.append(idx)

    labels = [1.0 if i in anomalies else 0.0 for i in range(n_points)]
    return values.tolist(), labels
