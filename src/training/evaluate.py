"""Held-out ranker metrics.

Precision, recall, lift, PR-AUC, and ROC-AUC are computed inside training and
written to the rank report. This function is the untrained result used when
the taxpayer table is not on disk yet.
"""

from __future__ import annotations


def evaluate(frame=None, config: dict | None = None) -> dict:
    del frame, config
    return {"rows": 0}
