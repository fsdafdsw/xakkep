from utils import clamp, safe_float


def _positive_consistency_signal(candidate):
    residual = safe_float(candidate.get("consistency_residual"), default=0.0)
    if residual <= 0.0:
        return 0.0
    return clamp(residual / 0.12, 0.0, 1.0)


def _positive_next_buyer_signal(candidate):
    edge = max(0.0, safe_float(candidate.get("next_buyer_edge"), default=0.0))
    score = safe_float(candidate.get("next_buyer_score"), default=0.0)
    return clamp((edge * 2.4 * 0.60) + (score * 0.40), 0.0, 1.0)


def _latent_state_mapping(candidate):
    action_family = str(candidate.get("domain_action_family") or "").strip().lower()
    catalyst_type = str(candidate.get("catalyst_type") or "").strip().lower()
    meeting_subtype = str(candidate.get("meeting_subtype") or "").strip().lower()

    if action_family == "conflict":
        return ("regional_escalation_state", 1.0)
    if action_family == "regime_shift":
        return ("leadership_instability_state", 1.0)
    if action_family == "release" and catalyst_type in {"hearing", "court_ruling", "appeal"}:
        return ("legal_activation_state", 1.0)
    if action_family == "release" and catalyst_type == "hostage_release":
        return ("hostage_release_state", 1.0)
    if action_family == "diplomacy":
        if catalyst_type in {"ceasefire", "negotiation", "summit"}:
            return ("negotiation_state", 1.0)
        if catalyst_type == "call_or_meeting":
            if meeting_subtype in {"talk_call", "meeting", "resume_talks"}:
                return ("negotiation_state", 1.0)
            return ("high_level_contact_state", 1.0)
    return (None, 0.0)


def _candidate_actual_activation(candidate):
    fresh = safe_float(candidate.get("repricing_fresh_catalyst_score"), default=0.0)
    underreaction = safe_float(candidate.get("repricing_underreaction_score"), default=0.0)
    attention_gap = safe_float(candidate.get("repricing_attention_gap"), default=0.0)
    confidence = safe_float(candidate.get("confidence"), default=0.0)
    regime_actual = safe_float(candidate.get("regime_actual_score"), default=0.0)
    next_buyer_signal = _positive_next_buyer_signal(candidate)
    consistency_signal = _positive_consistency_signal(candidate)
    return clamp(
        (fresh * 0.22)
        + (underreaction * 0.14)
        + (attention_gap * 0.14)
        + (regime_actual * 0.18)
        + (next_buyer_signal * 0.16)
        + (consistency_signal * 0.10)
        + (confidence * 0.06),
        0.0,
        1.0,
    )


def _candidate_implied_activation(candidate):
    entry = clamp(safe_float(candidate.get("entry"), default=0.0) / 0.30, 0.0, 1.0)
    already_priced = safe_float(candidate.get("repricing_already_priced_penalty"), default=0.0)
    trend_chase = safe_float(candidate.get("repricing_trend_chase_penalty"), default=0.0)
    recent_runup = safe_float(candidate.get("repricing_recent_runup"), default=0.0)
    regime_implied = safe_float(candidate.get("regime_implied_score"), default=0.0)
    return clamp(
        (entry * 0.30)
        + (already_priced * 0.24)
        + (trend_chase * 0.18)
        + (recent_runup * 0.12)
        + (regime_implied * 0.16),
        0.0,
        1.0,
    )


def annotate_latent_states(*candidate_groups):
    grouped = {}
    for group in candidate_groups:
        for candidate in group or []:
            state_name, exposure = _latent_state_mapping(candidate)
            candidate["latent_state_supported"] = bool(state_name)
            candidate["latent_state_name"] = state_name
            candidate["latent_state_exposure"] = exposure
            candidate["latent_state_actual_score"] = None
            candidate["latent_state_implied_score"] = None
            candidate["latent_state_gap_score"] = None
            candidate["latent_state_rank"] = None
            candidate["latent_state_selected"] = False
            if state_name:
                grouped.setdefault(state_name, []).append(candidate)

    summaries = []
    for state_name, rows in grouped.items():
        actual_candidates = [_candidate_actual_activation(row) for row in rows]
        actual_score = max(actual_candidates) if actual_candidates else 0.0
        implied_candidates = [_candidate_implied_activation(row) for row in rows]
        state_implied_score = sum(implied_candidates) / max(1, len(implied_candidates))

        ranked = []
        selected = None
        for row in rows:
            candidate_implied = _candidate_implied_activation(row)
            gap_score = clamp(actual_score - candidate_implied, -1.0, 1.0)
            row["latent_state_actual_score"] = actual_score
            row["latent_state_implied_score"] = candidate_implied
            row["latent_state_gap_score"] = gap_score
            ranked.append(row)

        ranked.sort(
            key=lambda row: (
                -safe_float(row.get("latent_state_gap_score"), default=-1.0),
                -safe_float(row.get("next_buyer_edge"), default=-1.0),
                -(1 if row.get("next_buyer_selected") else 0),
                -(1 if row.get("consistency_selected") else 0),
                -(safe_float(row.get("repricing_score"), default=0.0)),
                str(row.get("question") or ""),
            )
        )

        for idx, row in enumerate(ranked, start=1):
            row["latent_state_rank"] = idx
            if selected is None and safe_float(row.get("latent_state_gap_score"), default=-1.0) > 0.0:
                selected = row
                row["latent_state_selected"] = True

        summaries.append(
            {
                "latent_state_name": state_name,
                "state_actual_score": actual_score,
                "state_implied_score": state_implied_score,
                "cluster_size": len(rows),
                "selected_market_key": selected.get("market_key") if selected else None,
                "selected_question": selected.get("question") if selected else None,
                "selected_gap_score": selected.get("latent_state_gap_score") if selected else None,
                "members": [
                    {
                        "market_key": row.get("market_key"),
                        "question": row.get("question"),
                        "latent_state_gap_score": row.get("latent_state_gap_score"),
                        "latent_state_rank": row.get("latent_state_rank"),
                        "next_buyer_selected": row.get("next_buyer_selected"),
                        "consistency_selected": row.get("consistency_selected"),
                        "selected": row.get("latent_state_selected"),
                    }
                    for row in ranked
                ],
            }
        )

    summaries.sort(
        key=lambda row: (
            -safe_float(row.get("selected_gap_score"), default=-1.0),
            -safe_float(row.get("state_actual_score"), default=0.0),
            -int(row.get("cluster_size") or 0),
            str(row.get("latent_state_name") or ""),
        )
    )
    return summaries
