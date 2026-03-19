from thesis_trade_policy import thesis_identity
from utils import safe_float


_SUPPORTED_LADDERS = {
    "threshold_ladder",
    "deadline_ladder",
}


def _group_candidates(*candidate_groups):
    grouped = {}
    for group in candidate_groups:
        for candidate in group or []:
            thesis_id = thesis_identity(candidate)
            grouped.setdefault(thesis_id, []).append(candidate)
    return grouped


def _sorted_members(rows):
    return sorted(
        rows,
        key=lambda row: (
            safe_float(row.get("thesis_dimension_value"), default=-1.0),
            str(row.get("question") or ""),
        ),
    )


def _centrality_score(index, cluster_size):
    if cluster_size <= 1:
        return 1.0
    if cluster_size == 2:
        return 0.5

    center = (cluster_size - 1) / 2.0
    max_distance = max(center, 1.0)
    distance = abs(index - center)
    return max(0.0, 1.0 - (distance / max_distance))


def _question_complexity_penalty(question):
    text = str(question or "").strip()
    word_count = len(text.split())
    punctuation_count = sum(1 for ch in text if ch in ",;:()[]")
    comparator_penalty = 0.04 if " or more " in text.lower() else 0.0
    length_penalty = max(0.0, min(0.18, (word_count - 11) * 0.018))
    punctuation_penalty = min(0.10, punctuation_count * 0.025)
    return length_penalty + punctuation_penalty + comparator_penalty


def _headline_fit_score(question):
    word_count = len(str(question or "").split())
    ideal = 9.0
    return max(0.0, 1.0 - (abs(word_count - ideal) / ideal))


def _narrative_clarity_score(question):
    text = str(question or "").strip()
    if not text:
        return 0.0
    starts_clean = 1.0 if text.lower().startswith(("will ", "is ", "does ", "can ")) else 0.88
    complexity_penalty = _question_complexity_penalty(text)
    return max(0.0, min(1.0, starts_clean - complexity_penalty))


def annotate_default_contracts(*candidate_groups):
    grouped = _group_candidates(*candidate_groups)
    route_summaries = []

    for thesis_id, rows in grouped.items():
        thesis_type = str(rows[0].get("thesis_type") or "")
        cluster_size = len(rows)
        supported = cluster_size > 1 and thesis_type in _SUPPORTED_LADDERS

        for candidate in rows:
            candidate["default_contract_supported"] = supported
            candidate["headline_fit_score"] = None
            candidate["narrative_clarity_score"] = None
            candidate["defaultness_score"] = None
            candidate["threshold_centrality_score"] = None
            candidate["deadline_centrality_score"] = None
            candidate["question_complexity_penalty"] = None
            candidate["default_contract_score"] = None
            candidate["default_contract_rank"] = None
            candidate["default_contract_selected"] = False

        if not supported:
            continue

        sorted_rows = _sorted_members(rows)
        ranked = []
        for idx, row in enumerate(sorted_rows):
            centrality = _centrality_score(idx, cluster_size)
            threshold_centrality = centrality if thesis_type == "threshold_ladder" else 0.0
            deadline_centrality = centrality if thesis_type == "deadline_ladder" else 0.0
            complexity_penalty = _question_complexity_penalty(row.get("question"))
            headline_fit = _headline_fit_score(row.get("question"))
            narrative_clarity = _narrative_clarity_score(row.get("question"))

            defaultness = (
                (centrality * 0.55)
                + (headline_fit * 0.25)
                + (narrative_clarity * 0.20)
            )
            score = max(0.0, defaultness - complexity_penalty)

            row["headline_fit_score"] = headline_fit
            row["narrative_clarity_score"] = narrative_clarity
            row["defaultness_score"] = defaultness
            row["threshold_centrality_score"] = threshold_centrality
            row["deadline_centrality_score"] = deadline_centrality
            row["question_complexity_penalty"] = complexity_penalty
            row["default_contract_score"] = score
            ranked.append(row)

        ranked.sort(
            key=lambda row: (
                -safe_float(row.get("default_contract_score"), default=0.0),
                -safe_float(row.get("defaultness_score"), default=0.0),
                -safe_float(row.get("repricing_lane_prior"), default=0.0),
                safe_float(row.get("entry"), default=1.0),
                str(row.get("question") or ""),
            )
        )

        selected = ranked[0]
        for idx, row in enumerate(ranked, start=1):
            row["default_contract_rank"] = idx
            row["default_contract_selected"] = row is selected

        route_summaries.append(
            {
                "thesis_id": thesis_id,
                "thesis_type": thesis_type,
                "cluster_size": cluster_size,
                "selected_market_key": selected.get("market_key"),
                "selected_question": selected.get("question"),
                "selected_dimension_label": selected.get("thesis_dimension_label"),
                "selected_default_contract_score": selected.get("default_contract_score"),
                "members": [
                    {
                        "market_key": row.get("market_key"),
                        "question": row.get("question"),
                        "dimension_label": row.get("thesis_dimension_label"),
                        "default_contract_score": row.get("default_contract_score"),
                        "defaultness_score": row.get("defaultness_score"),
                        "headline_fit_score": row.get("headline_fit_score"),
                        "narrative_clarity_score": row.get("narrative_clarity_score"),
                        "question_complexity_penalty": row.get("question_complexity_penalty"),
                        "default_contract_rank": row.get("default_contract_rank"),
                        "selected": row.get("default_contract_selected"),
                    }
                    for row in ranked
                ],
            }
        )

    route_summaries.sort(
        key=lambda row: (
            -safe_float(row.get("selected_default_contract_score"), default=0.0),
            -int(row.get("cluster_size") or 0),
            str(row.get("thesis_id") or ""),
        )
    )
    return route_summaries
