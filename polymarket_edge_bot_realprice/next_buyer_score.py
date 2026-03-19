from thesis_trade_policy import thesis_identity
from utils import clamp, safe_float


def _group_candidates(*candidate_groups):
    grouped = {}
    for group in candidate_groups:
        for candidate in group or []:
            grouped.setdefault(thesis_identity(candidate), []).append(candidate)
    return grouped


def _positive_consistency_signal(candidate):
    residual = safe_float(candidate.get("consistency_residual"), default=0.0)
    if residual <= 0.0:
        return 0.0
    return clamp(residual / 0.12, 0.0, 1.0)


def _positive_regime_signal(candidate):
    gap = max(0.0, safe_float(candidate.get("regime_gap_score"), default=0.0))
    quality = safe_float(candidate.get("regime_transition_quality"), default=0.0)
    return clamp((gap * 2.8 * 0.70) + (quality * 0.30), 0.0, 1.0)


def _price_heat(candidate):
    entry = clamp(safe_float(candidate.get("entry"), default=0.0) / 0.25, 0.0, 1.0)
    already_priced = safe_float(candidate.get("repricing_already_priced_penalty"), default=0.0)
    trend_chase = safe_float(candidate.get("repricing_trend_chase_penalty"), default=0.0)
    return clamp(
        (entry * 0.52)
        + (already_priced * 0.28)
        + (trend_chase * 0.20),
        0.0,
        1.0,
    )


def annotate_next_buyer_scores(*candidate_groups):
    grouped = _group_candidates(*candidate_groups)
    summaries = []

    for thesis_id, rows in grouped.items():
        cluster_size = len(rows)
        supported = cluster_size > 1 and any(
            row.get("default_contract_supported")
            or row.get("attention_flow_supported")
            or row.get("consistency_engine_supported")
            or row.get("regime_actual_score") is not None
            for row in rows
        )

        for candidate in rows:
            candidate["next_buyer_supported"] = supported
            candidate["next_buyer_score"] = None
            candidate["next_buyer_edge"] = None
            candidate["next_buyer_rank"] = None
            candidate["next_buyer_selected"] = False

        if not supported:
            continue

        ranked = []
        for row in rows:
            default_signal = safe_float(row.get("default_contract_score"), default=0.0)
            attention_signal = safe_float(row.get("attention_capture_score"), default=0.0)
            consistency_signal = _positive_consistency_signal(row)
            regime_signal = _positive_regime_signal(row)
            price_heat = _price_heat(row)

            score = clamp(
                (default_signal * 0.28)
                + (attention_signal * 0.30)
                + (consistency_signal * 0.24)
                + (regime_signal * 0.18),
                0.0,
                1.0,
            )
            edge = clamp(score - (price_heat * 0.72), -1.0, 1.0)

            row["next_buyer_score"] = score
            row["next_buyer_edge"] = edge
            ranked.append(row)

        ranked.sort(
            key=lambda row: (
                -safe_float(row.get("next_buyer_edge"), default=-1.0),
                -safe_float(row.get("next_buyer_score"), default=0.0),
                -(1 if row.get("attention_flow_selected") else 0),
                -(1 if row.get("default_contract_selected") else 0),
                -(1 if row.get("consistency_selected") else 0),
                -safe_float(row.get("repricing_lane_prior"), default=0.0),
                safe_float(row.get("entry"), default=1.0),
                str(row.get("question") or ""),
            )
        )

        selected = None
        for idx, row in enumerate(ranked, start=1):
            row["next_buyer_rank"] = idx
            if selected is None and safe_float(row.get("next_buyer_edge"), default=-1.0) > 0.0:
                selected = row
                row["next_buyer_selected"] = True

        summaries.append(
            {
                "thesis_id": thesis_id,
                "thesis_type": rows[0].get("thesis_type"),
                "cluster_size": cluster_size,
                "selected_market_key": selected.get("market_key") if selected else None,
                "selected_question": selected.get("question") if selected else None,
                "selected_next_buyer_score": selected.get("next_buyer_score") if selected else None,
                "selected_next_buyer_edge": selected.get("next_buyer_edge") if selected else None,
                "members": [
                    {
                        "market_key": row.get("market_key"),
                        "question": row.get("question"),
                        "next_buyer_score": row.get("next_buyer_score"),
                        "next_buyer_edge": row.get("next_buyer_edge"),
                        "next_buyer_rank": row.get("next_buyer_rank"),
                        "default_contract_selected": row.get("default_contract_selected"),
                        "attention_flow_selected": row.get("attention_flow_selected"),
                        "consistency_selected": row.get("consistency_selected"),
                        "regime_selected": row.get("regime_selected"),
                        "selected": row.get("next_buyer_selected"),
                    }
                    for row in ranked
                ],
            }
        )

    summaries.sort(
        key=lambda row: (
            -safe_float(row.get("selected_next_buyer_edge"), default=-1.0),
            -safe_float(row.get("selected_next_buyer_score"), default=0.0),
            -int(row.get("cluster_size") or 0),
            str(row.get("thesis_id") or ""),
        )
    )
    return summaries
