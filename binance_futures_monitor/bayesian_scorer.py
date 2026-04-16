#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bayesian dynamic scorer for squeeze/ignition quality estimation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Mapping


def _clamp(v: float, lo: float, hi: float) -> float:
    return min(hi, max(lo, float(v)))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


@dataclass(frozen=True)
class BayesianScorerConfig:
    # Prior P(H): linear decay by fake_breakout hits in rolling window
    prior_max: float = 0.50
    prior_min: float = 0.15
    prior_decay_hits: float = 100.0

    # Evidence thresholds
    evidence_a_pump20s_pct: float = 3.0
    evidence_b_funding_rate_lt: float = 0.0
    # NOTE: funding_rate_delta is decimal in engine (e.g. -0.0001 == -0.01%)
    evidence_b_funding_delta_lt: float = -0.0001
    evidence_c_oi5m_pct_le: float = 0.0

    # Likelihoods P(E|H)
    p_ea_given_h: float = 0.90
    p_eb_given_h: float = 0.85
    p_ec_given_h: float = 0.90
    # Likelihoods P(E|~H)
    p_ea_given_not_h: float = 0.10
    p_eb_given_not_h: float = 0.20
    p_ec_given_not_h: float = 0.20

    def normalized(self) -> "BayesianScorerConfig":
        pmin = _clamp(self.prior_min, 0.01, 0.99)
        pmax = _clamp(self.prior_max, 0.01, 0.99)
        if pmax < pmin:
            pmin, pmax = pmax, pmin
        eps = 1e-6
        return BayesianScorerConfig(
            prior_max=pmax,
            prior_min=pmin,
            prior_decay_hits=max(1.0, float(self.prior_decay_hits)),
            evidence_a_pump20s_pct=float(self.evidence_a_pump20s_pct),
            evidence_b_funding_rate_lt=float(self.evidence_b_funding_rate_lt),
            evidence_b_funding_delta_lt=float(self.evidence_b_funding_delta_lt),
            evidence_c_oi5m_pct_le=float(self.evidence_c_oi5m_pct_le),
            p_ea_given_h=_clamp(self.p_ea_given_h, eps, 1.0 - eps),
            p_eb_given_h=_clamp(self.p_eb_given_h, eps, 1.0 - eps),
            p_ec_given_h=_clamp(self.p_ec_given_h, eps, 1.0 - eps),
            p_ea_given_not_h=_clamp(self.p_ea_given_not_h, eps, 1.0 - eps),
            p_eb_given_not_h=_clamp(self.p_eb_given_not_h, eps, 1.0 - eps),
            p_ec_given_not_h=_clamp(self.p_ec_given_not_h, eps, 1.0 - eps),
        )

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> "BayesianScorerConfig":
        if not isinstance(raw, Mapping):
            return cls().normalized()
        base = cls()
        data: Dict[str, Any] = {}
        for key in base.__dataclass_fields__.keys():
            if key in raw:
                data[key] = raw.get(key)
        try:
            cfg = cls(**data)  # type: ignore[arg-type]
        except Exception:
            cfg = cls()
        return cfg.normalized()


class BayesianScorer:
    """Naive-Bayes scorer for real squeeze/ignition posterior probability."""

    def __init__(self, cfg: BayesianScorerConfig | Mapping[str, Any] | None = None) -> None:
        if isinstance(cfg, BayesianScorerConfig):
            self.cfg = cfg.normalized()
        else:
            self.cfg = BayesianScorerConfig.from_mapping(cfg)

    def update_prior(self, fake_breakout_hits_4h: float) -> float:
        """Map rolling fake_breakout hits to prior P(H)."""
        hits = max(0.0, _safe_float(fake_breakout_hits_4h, 0.0))
        ratio = _clamp(hits / max(1.0, self.cfg.prior_decay_hits), 0.0, 1.0)
        prior = self.cfg.prior_max - (self.cfg.prior_max - self.cfg.prior_min) * ratio
        return _clamp(prior, self.cfg.prior_min, self.cfg.prior_max)

    def _extract_evidence(self, payload: Mapping[str, Any]) -> Dict[str, bool]:
        pump_20s = _safe_float(payload.get("pump_20s_pct", 0.0), 0.0)
        funding_rate = _safe_float(payload.get("funding_rate", 0.0), 0.0)
        funding_delta = _safe_float(payload.get("funding_rate_delta", 0.0), 0.0)
        oi5m = _safe_float(payload.get("oi_change_5m_pct", 0.0), 0.0)

        return {
            "A_extreme_pump20s": bool(pump_20s > self.cfg.evidence_a_pump20s_pct),
            "B_funding_short_div": bool(
                funding_rate < self.cfg.evidence_b_funding_rate_lt
                and funding_delta < self.cfg.evidence_b_funding_delta_lt
            ),
            "C_oi_stall_or_drop": bool(oi5m <= self.cfg.evidence_c_oi5m_pct_le),
        }

    @staticmethod
    def _bernoulli_log_likelihood(hit: bool, p_true: float) -> float:
        p = _clamp(p_true, 1e-9, 1.0 - 1e-9)
        return math.log(p if hit else (1.0 - p))

    def calculate_posterior(
        self,
        event_payload: Mapping[str, Any],
        fake_breakout_hits_4h: float,
    ) -> Dict[str, float]:
        """Return posterior P(H|E) and intermediate diagnostics."""
        prior = self.update_prior(fake_breakout_hits_4h=fake_breakout_hits_4h)
        not_prior = 1.0 - prior
        ev = self._extract_evidence(event_payload)

        log_h = math.log(_clamp(prior, 1e-9, 1.0 - 1e-9))
        log_not_h = math.log(_clamp(not_prior, 1e-9, 1.0 - 1e-9))

        log_h += self._bernoulli_log_likelihood(ev["A_extreme_pump20s"], self.cfg.p_ea_given_h)
        log_h += self._bernoulli_log_likelihood(ev["B_funding_short_div"], self.cfg.p_eb_given_h)
        log_h += self._bernoulli_log_likelihood(ev["C_oi_stall_or_drop"], self.cfg.p_ec_given_h)

        log_not_h += self._bernoulli_log_likelihood(ev["A_extreme_pump20s"], self.cfg.p_ea_given_not_h)
        log_not_h += self._bernoulli_log_likelihood(ev["B_funding_short_div"], self.cfg.p_eb_given_not_h)
        log_not_h += self._bernoulli_log_likelihood(ev["C_oi_stall_or_drop"], self.cfg.p_ec_given_not_h)

        m = max(log_h, log_not_h)
        exp_h = math.exp(log_h - m)
        exp_not_h = math.exp(log_not_h - m)
        denom = exp_h + exp_not_h
        posterior = exp_h / denom if denom > 1e-12 else prior

        return {
            "prior": float(prior),
            "posterior": float(_clamp(posterior, 0.0, 1.0)),
            "fake_breakout_hits_4h": float(max(0.0, _safe_float(fake_breakout_hits_4h, 0.0))),
            "ev_a": 1.0 if ev["A_extreme_pump20s"] else 0.0,
            "ev_b": 1.0 if ev["B_funding_short_div"] else 0.0,
            "ev_c": 1.0 if ev["C_oi_stall_or_drop"] else 0.0,
            "log_likelihood_h": float(log_h),
            "log_likelihood_not_h": float(log_not_h),
        }
