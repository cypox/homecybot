from __future__ import annotations

from dataclasses import asdict, dataclass
from itertools import combinations
from typing import Any, Callable, Iterable

import numpy as np

try:
    from statsmodels.tsa.stattools import coint
except Exception:  # pragma: no cover - optional dependency compatibility
    coint = None


@dataclass(slots=True)
class PairStats:
    symbol_a: str
    symbol_b: str
    latest_price_a: float
    latest_price_b: float
    correlation: float
    return_correlation: float
    cointegration_pvalue: float | None
    hedge_ratio: float
    intercept: float
    spread_mean: float
    spread_std: float
    spread_last: float
    zscore: float
    ratio_mean: float
    ratio_std: float
    ratio_last: float
    ratio_zscore: float
    half_life: float | None
    mean_crossings: int
    momentum_score: float
    liquidity_score: float
    signal: str
    score: float

    def to_dict(self) -> dict[str, float | int | str | None]:
        return asdict(self)


def _to_array(values: Iterable[float]) -> np.ndarray:
    array = np.asarray(list(values), dtype=float)
    if array.size < 5:
        raise ValueError("At least 5 price points are required")
    return array


def _safe_corrcoef(series_a: np.ndarray, series_b: np.ndarray) -> float:
    if np.std(series_a) == 0 or np.std(series_b) == 0:
        return 0.0
    corr = float(np.corrcoef(series_a, series_b)[0, 1])
    return 0.0 if np.isnan(corr) else corr


def _mean_crossings(spread: np.ndarray) -> int:
    centered = spread - spread.mean()
    signs = np.sign(centered)
    crossings = 0
    for current, nxt in zip(signs[:-1], signs[1:]):
        if current == 0 or nxt == 0:
            continue
        if current != nxt:
            crossings += 1
    return crossings


def _estimate_half_life(spread: np.ndarray) -> float | None:
    if spread.size < 3:
        return None

    lagged = spread[:-1]
    delta = np.diff(spread)
    denominator = float(np.dot(lagged, lagged))
    if denominator == 0:
        return None

    beta = float(np.dot(lagged, delta) / denominator)
    if beta >= 0:
        return None

    half_life = -np.log(2) / beta
    if not np.isfinite(half_life) or half_life <= 0:
        return None

    return float(half_life)


def _signal_from_zscore(zscore: float, entry_threshold: float = 1.5, watch_threshold: float = 1.0) -> str:
    if zscore >= entry_threshold:
        return "short_a_long_b"
    if zscore <= -entry_threshold:
        return "long_a_short_b"
    if abs(zscore) >= watch_threshold:
        return "watch"
    return "flat"


def _estimate_cointegration_pvalue(series_a: np.ndarray, series_b: np.ndarray, spread: np.ndarray) -> float:
    if coint is not None:
        try:
            pvalue = float(coint(series_a, series_b)[1])
            if np.isfinite(pvalue):
                return max(0.0, min(1.0, pvalue))
        except Exception:
            pass

    if spread.size < 3:
        return 1.0

    lagged = spread[:-1]
    delta = np.diff(spread)
    denominator = float(np.dot(lagged, lagged))
    reversion_strength = 0.0 if denominator == 0 else max(0.0, -float(np.dot(lagged, delta) / denominator))
    corr_strength = abs(_safe_corrcoef(series_a, series_b))
    normalized_reversion = min(reversion_strength * 5.0, 1.0)
    synthetic_strength = (0.65 * corr_strength) + (0.35 * normalized_reversion)
    return round(max(0.0, min(1.0, 1.0 - synthetic_strength)), 6)


def _coerce_series_payload(payload: Any) -> tuple[np.ndarray, np.ndarray | None, dict[str, Any]]:
    if isinstance(payload, dict):
        prices = _to_array(payload.get("closes", []))
        volumes_raw = payload.get("volumes")
        volumes = None if volumes_raw is None else np.asarray(list(volumes_raw), dtype=float)
        extras = {
            "quote": payload.get("quote", {}),
        }
        return prices, volumes, extras
    return _to_array(payload), None, {"quote": {}}


def _compute_momentum_score(series_a: np.ndarray, series_b: np.ndarray, zscore: float) -> float:
    window = min(20, len(series_a) - 1, len(series_b) - 1)
    if window < 2:
        return 0.0
    ret_a = float(series_a[-1] / series_a[-window] - 1.0)
    ret_b = float(series_b[-1] / series_b[-window] - 1.0)
    relative_momentum = ret_a - ret_b
    aligned = (zscore > 0 and relative_momentum > 0) or (zscore < 0 and relative_momentum < 0)
    magnitude = min(abs(relative_momentum) * 10.0, 5.0)
    return magnitude if aligned else magnitude / 3.0


def _compute_liquidity_score(
    series_a: np.ndarray,
    series_b: np.ndarray,
    volumes_a: np.ndarray | None,
    volumes_b: np.ndarray | None,
    quote_a: dict[str, Any],
    quote_b: dict[str, Any],
) -> float:
    adv_score = 0.0
    if volumes_a is not None and volumes_b is not None and len(volumes_a) and len(volumes_b):
        adv_a = float(np.mean(volumes_a[-min(20, len(volumes_a)):]) * series_a[-1])
        adv_b = float(np.mean(volumes_b[-min(20, len(volumes_b)):]) * series_b[-1])
        adv_score = min(np.log10(max(adv_a, 1.0)) + np.log10(max(adv_b, 1.0)), 20.0) / 2.0

    def spread_bps(quote: dict[str, Any]) -> float:
        bid = quote.get("bid")
        ask = quote.get("ask")
        market = quote.get("market_price") or quote.get("last") or quote.get("close")
        if bid is None or ask is None or market in (None, 0):
            return 25.0
        return float(((ask - bid) / market) * 10000.0)

    spread_penalty = (spread_bps(quote_a) + spread_bps(quote_b)) / 2.0
    return max(0.0, round(adv_score - min(spread_penalty / 10.0, 5.0), 6))


def compute_pair_stats(
    symbol_a: str,
    prices_a: Iterable[float] | dict[str, Any],
    symbol_b: str,
    prices_b: Iterable[float] | dict[str, Any],
    entry_threshold: float = 1.5,
    watch_threshold: float = 1.0,
) -> PairStats:
    series_a, volumes_a, extras_a = _coerce_series_payload(prices_a)
    series_b, volumes_b, extras_b = _coerce_series_payload(prices_b)

    min_length = min(series_a.size, series_b.size)
    series_a = series_a[-min_length:]
    series_b = series_b[-min_length:]

    correlation = _safe_corrcoef(series_a, series_b)

    returns_a = np.diff(np.log(series_a))
    returns_b = np.diff(np.log(series_b))
    return_correlation = _safe_corrcoef(returns_a, returns_b) if returns_a.size >= 2 else 0.0

    design = np.column_stack([series_b, np.ones(min_length)])
    hedge_ratio, intercept = np.linalg.lstsq(design, series_a, rcond=None)[0]

    spread = series_a - (hedge_ratio * series_b + intercept)
    cointegration_pvalue = _estimate_cointegration_pvalue(series_a, series_b, spread)
    spread_mean = float(spread.mean())
    spread_std = float(spread.std(ddof=0))
    spread_last = float(spread[-1])
    zscore = 0.0 if spread_std == 0 else float((spread_last - spread_mean) / spread_std)

    ratio = series_a / series_b
    ratio_mean = float(ratio.mean())
    ratio_std = float(ratio.std(ddof=0))
    ratio_last = float(ratio[-1])
    ratio_zscore = 0.0 if ratio_std == 0 else float((ratio_last - ratio_mean) / ratio_std)

    mean_crossings = _mean_crossings(spread)
    half_life = _estimate_half_life(spread)
    signal = _signal_from_zscore(zscore, entry_threshold=entry_threshold, watch_threshold=watch_threshold)
    momentum_score = _compute_momentum_score(series_a, series_b, zscore)
    liquidity_score = _compute_liquidity_score(series_a, series_b, volumes_a, volumes_b, extras_a["quote"], extras_b["quote"])

    stability_bonus = mean_crossings / max(min_length - 1, 1)
    half_life_bonus = 0.0 if half_life is None else max(0.0, 10.0 - min(half_life, 20.0)) / 2.0
    opportunity_bonus = max(0.0, 2.5 - abs(zscore))
    cointegration_bonus = 0.0 if cointegration_pvalue is None else max(0.0, 1.0 - cointegration_pvalue) * 20.0
    score = float(
        (max(correlation, 0.0) * 35.0)
        + (max(return_correlation, 0.0) * 20.0)
        + (stability_bonus * 12.0)
        + half_life_bonus
        + opportunity_bonus
        + momentum_score
        + liquidity_score
        + cointegration_bonus
    )

    return PairStats(
        symbol_a=symbol_a,
        symbol_b=symbol_b,
        latest_price_a=round(float(series_a[-1]), 6),
        latest_price_b=round(float(series_b[-1]), 6),
        correlation=round(correlation, 6),
        return_correlation=round(return_correlation, 6),
        cointegration_pvalue=None if cointegration_pvalue is None else round(cointegration_pvalue, 6),
        hedge_ratio=round(float(hedge_ratio), 6),
        intercept=round(float(intercept), 6),
        spread_mean=round(spread_mean, 6),
        spread_std=round(spread_std, 6),
        spread_last=round(spread_last, 6),
        zscore=round(zscore, 6),
        ratio_mean=round(ratio_mean, 6),
        ratio_std=round(ratio_std, 6),
        ratio_last=round(ratio_last, 6),
        ratio_zscore=round(ratio_zscore, 6),
        half_life=None if half_life is None else round(half_life, 6),
        mean_crossings=mean_crossings,
        momentum_score=round(momentum_score, 6),
        liquidity_score=round(liquidity_score, 6),
        signal=signal,
        score=round(score, 6),
    )


def rank_pairs(
    price_map: dict[str, Iterable[float]],
    top_n: int = 5,
    min_correlation: float = 0.8,
    entry_threshold: float = 1.5,
    watch_threshold: float = 1.0,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> list[PairStats]:
    ranked: list[PairStats] = []
    symbols = sorted(price_map)
    total_pairs = (len(symbols) * (len(symbols) - 1)) // 2 if len(symbols) >= 2 else 0
    scanned_pairs = 0
    top_correlation = 0.0
    opportunities_found = 0

    for symbol_a, symbol_b in combinations(symbols, 2):
        scanned_pairs += 1
        try:
            stats = compute_pair_stats(
                symbol_a,
                price_map[symbol_a],
                symbol_b,
                price_map[symbol_b],
                entry_threshold=entry_threshold,
                watch_threshold=watch_threshold,
            )
        except ValueError:
            continue

        top_correlation = max(top_correlation, abs(stats.correlation))

        if abs(stats.correlation) >= min_correlation:
            ranked.append(stats)
            if stats.signal != "flat":
                opportunities_found += 1

        if progress_callback is not None:
            progress_callback(
                {
                    "scanned_pairs": scanned_pairs,
                    "total_pairs": total_pairs,
                    "opportunities_found": opportunities_found,
                    "top_correlation": round(top_correlation, 6),
                    "current_pair": f"{symbol_a}/{symbol_b}",
                }
            )

    ranked.sort(key=lambda item: (item.score, abs(item.zscore), item.correlation), reverse=True)
    return ranked[:top_n]
