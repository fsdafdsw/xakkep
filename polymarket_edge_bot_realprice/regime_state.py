from thesis_trade_policy import thesis_identity
from utils import clamp, safe_float


def _group_candidates(*candidate_groups):
    grouped = {}
    for group in candidate_groups:
        for candidate in group or []:
            grouped.setdefault(thesis_identity(candidate), []).append(candidate)
    return grouped


def _positive_residual_signal(candidate):
    residual = safe_float(candidate.get("consistency_residual"), default=0.0)
    if residual <= 0.0:
        return 0.0
    return clamp(residual / 0.12, 0.0, 1.0)


def _family_urgency_signal(candidate):
    conflict = safe_float(candidate.get("repricing_conflict_urgency_score"), default=0.0)
    release = safe_float(candidate.get("repricing_release_legitimacy_score"), default=0.0)
    return max(conflict, release, 0.0)


def _candidate_actual_activation(candidate):
    fresh = safe_float(candidate.get("repricing_fresh_catalyst_score"), default=0.0)
    underreaction = safe_float(candidate.get("repricing_underreaction_score"), default=0.0)
    attention_gap = safe_float(candidate.get("repricing_attention_gap"), default=0.0)
    optionality = safe_float(candidate.get("repricing_optionality_score"), default=0.0)
    confidence = safe_float(candidate.get("confidence"), default=0.0)
    consistency_signal = _positive_residual_signal(candidate)
    urgency = _family_urgency_signal(candidate)
    return clamp(
        (fresh * 0.28)
        + (underreaction * 0.18)
        + (attention_gap * 0.18)
        + (consistency_signal * 0.18)
        + (urgency * 0.10)
        + (optionality * 0.04)
        + (confidence * 0.04)
    , 0.0, 1.0)


def _candidate_implied_activation(candidate):
    entry = safe_float(candidate.get("entry"), default=0.0)
    recent_runup = safe_float(candidate.get("repricing_recent_runup"), default=0.0)
    already_priced = safe_float(candidate.get("repricing_already_priced_penalty"), default=0.0)
    stale = safe_float(candidate.get("repricing_stale_score"), default=0.0)
    entry_activation = clamp(entry / 0.30, 0.0, 1.0)
    return clamp(
        (entry_activation * 0.38)
        + (recent_runup * 0.24)
        + (already_priced * 0.28)
        + (stale * 0.10)
    , 0.0, 1.0)


def _regime_label(score):
    score = clamp(score, 0.0, 1.0)
    if score < 0.22:
        return "dormant"
    if score < 0.48:
        return "warming_up"
    if score < 0.86:
        return "active"
    return "late"


def _trade_window(candidate, actual_score, implied_score, gap_score):
    trend_chase = safe_float(candidate.get("repricing_trend_chase_penalty"), default=0.0)
    already_priced = safe_float(candidate.get("repricing_already_priced_penalty"), default=0.0)

    if actual_score < 0.30:
        return "dormant"
    if implied_score >= 0.76 or already_priced >= 0.36 or trend_chase >= 0.34:
        return "late"
    if gap_score >= 0.14 and trend_chase <= 0.22:
        return "active"
    if gap_score >= 0.08 and implied_score < 0.46:
        return "early"
    if gap_score <= -0.06:
        return "exhausted"
    return "monitor"


def annotate_regime_state(*candidate_groups):
    grouped = _group_candidates(*candidate_groups)
    summaries = []

    for thesis_id, rows in grouped.items():
        actual_candidates = []
        for candidate in rows:
            candidate["regime_actual_score"] = None
            candidate["regime_actual_state"] = None
            candidate["regime_implied_score"] = None
            candidate["regime_implied_state"] = None
            candidate["regime_gap_score"] = None
            candidate["regime_transition_quality"] = None
            candidate["regime_trade_window"] = None
            candidate["regime_selected"] = False
            actual_candidates.append(_candidate_actual_activation(candidate))

        actual_score = max(actual_candidates) if actual_candidates else 0.0
        actual_state = _regime_label(actual_score)

        selected = None
        ranked = []
        for candidate in rows:
            implied_score = _candidate_implied_activation(candidate)
            gap_score = clamp(actual_score - implied_score, -1.0, 1.0)
            trend_chase = safe_float(candidate.get("repricing_trend_chase_penalty"), default=0.0)
            confidence = safe_float(candidate.get("confidence"), default=0.0)
            transition_quality = clamp(
                (gap_score * 0.62) + ((1.0 - trend_chase) * 0.24) + (confidence * 0.14),
                0.0,
                1.0,
            )
            trade_window = _trade_window(candidate, actual_score, implied_score, gap_score)

            candidate["regime_actual_score"] = actual_score
            candidate["regime_actual_state"] = actual_state
            candidate["regime_implied_score"] = implied_score
            candidate["regime_implied_state"] = _regime_label(implied_score)
            candidate["regime_gap_score"] = gap_score
            candidate["regime_transition_quality"] = transition_quality
            candidate["regime_trade_window"] = trade_window
            ranked.append(candidate)

        ranked.sort(
            key=lambda row: (
                -(safe_float(row.get("regime_gap_score"), default=-1.0)),
                -(safe_float(row.get("regime_transition_quality"), default=0.0)),
                -(1 if row.get("consistency_selected") else 0),
                -(safe_float(row.get("repricing_score"), default=0.0)),
                str(row.get("question") or ""),
            )
        )

        for row in ranked:
            gap_score = safe_float(row.get("regime_gap_score"), default=0.0)
            window = str(row.get("regime_trade_window") or "")
            if gap_score > 0.0 and window in {"early", "active"}:
                selected = row
                row["regime_selected"] = True
                break

        summaries.append(
            {
                "thesis_id": thesis_id,
                "thesis_type": rows[0].get("thesis_type"),
                "cluster_size": len(rows),
                "actual_regime": actual_state,
                "actual_score": actual_score,
                "selected_market_key": selected.get("market_key") if selected else None,
                "selected_question": selected.get("question") if selected else None,
                "selected_gap_score": selected.get("regime_gap_score") if selected else None,
                "selected_trade_window": selected.get("regime_trade_window") if selected else None,
                "members": [
                    {
                        "market_key": row.get("market_key"),
                        "question": row.get("question"),
                        "implied_regime": row.get("regime_implied_state"),
                        "implied_score": row.get("regime_implied_score"),
                        "gap_score": row.get("regime_gap_score"),
                        "transition_quality": row.get("regime_transition_quality"),
                        "trade_window": row.get("regime_trade_window"),
                        "selected": row.get("regime_selected"),
                    }
                    for row in ranked
                ],
            }
        )

    summaries.sort(
        key=lambda row: (
            -(safe_float(row.get("selected_gap_score"), default=-1.0)),
            -(safe_float(row.get("actual_score"), default=0.0)),
            -int(row.get("cluster_size") or 0),
            str(row.get("thesis_id") or ""),
        )
    )
    return summaries
