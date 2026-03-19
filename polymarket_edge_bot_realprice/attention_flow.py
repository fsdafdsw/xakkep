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
    comparator_penalty = 0.05 if " or more " in text.lower() else 0.0
    length_penalty = max(0.0, min(0.20, (word_count - 10) * 0.02))
    punctuation_penalty = min(0.10, punctuation_count * 0.025)
    return length_penalty + punctuation_penalty + comparator_penalty


def _clickability_score(question):
    text = str(question or "").strip()
    word_count = len(text.split())
    ideal = 8.0
    punctuation_penalty = min(0.14, sum(1 for ch in text if ch in ",;:()[]") * 0.035)
    base = max(0.0, 1.0 - (abs(word_count - ideal) / ideal))
    return max(0.0, base - punctuation_penalty)


def _retail_legibility_score(question):
    text = str(question or "").strip()
    if not text:
        return 0.0
    starts_clean = 1.0 if text.lower().startswith(("will ", "is ", "does ", "can ")) else 0.88
    return max(0.0, min(1.0, starts_clean - (_question_complexity_penalty(text) * 1.05)))


def _narrative_portability_score(question):
    text = str(question or "").strip()
    complexity = _question_complexity_penalty(text)
    word_count = len(text.split())
    brevity = max(0.0, min(1.0, 1.0 - max(0.0, word_count - 12) * 0.05))
    question_mark_bonus = 0.04 if text.endswith("?") else 0.0
    return max(0.0, min(1.0, (brevity * 0.55) + ((1.0 - complexity) * 0.45) + question_mark_bonus))


def _sibling_confusion_penalty(sorted_rows, idx):
    if len(sorted_rows) <= 2:
        return 0.0

    current = safe_float(sorted_rows[idx].get("thesis_dimension_value"))
    values = [safe_float(row.get("thesis_dimension_value")) for row in sorted_rows]
    finite_values = [value for value in values if value is not None]
    if current is None or len(finite_values) < 2:
        return 0.0

    full_range = max(finite_values) - min(finite_values)
    if full_range <= 0:
        return 0.0

    gaps = []
    if idx > 0:
        left = safe_float(sorted_rows[idx - 1].get("thesis_dimension_value"))
        if left is not None:
            gaps.append(abs(current - left))
    if idx < len(sorted_rows) - 1:
        right = safe_float(sorted_rows[idx + 1].get("thesis_dimension_value"))
        if right is not None:
            gaps.append(abs(current - right))

    if not gaps:
        return 0.0

    nearest_gap = min(gaps)
    normalized_gap = max(0.0, min(1.0, nearest_gap / full_range))
    return max(0.0, 0.18 * (1.0 - normalized_gap))


def annotate_attention_flow(*candidate_groups):
    grouped = _group_candidates(*candidate_groups)
    route_summaries = []

    for thesis_id, rows in grouped.items():
        thesis_type = str(rows[0].get("thesis_type") or "")
        cluster_size = len(rows)
        supported = cluster_size > 1 and thesis_type in _SUPPORTED_LADDERS

        for candidate in rows:
            candidate["attention_flow_supported"] = supported
            candidate["retail_legibility_score"] = None
            candidate["clickability_score"] = None
            candidate["narrative_portability_score"] = None
            candidate["sibling_confusion_penalty"] = None
            candidate["attention_capture_score"] = None
            candidate["attention_flow_rank"] = None
            candidate["attention_flow_selected"] = False

        if not supported:
            continue

        sorted_rows = _sorted_members(rows)
        ranked = []
        for idx, row in enumerate(sorted_rows):
            centrality = _centrality_score(idx, cluster_size)
            retail_legibility = _retail_legibility_score(row.get("question"))
            clickability = _clickability_score(row.get("question"))
            portability = _narrative_portability_score(row.get("question"))
            sibling_confusion = _sibling_confusion_penalty(sorted_rows, idx)
            default_contract_score = safe_float(row.get("default_contract_score"), default=0.0)

            attention_capture = max(
                0.0,
                (
                    (retail_legibility * 0.26)
                    + (clickability * 0.22)
                    + (portability * 0.18)
                    + (centrality * 0.10)
                    + (default_contract_score * 0.30)
                    - (sibling_confusion * 0.90)
                ),
            )

            row["retail_legibility_score"] = retail_legibility
            row["clickability_score"] = clickability
            row["narrative_portability_score"] = portability
            row["sibling_confusion_penalty"] = sibling_confusion
            row["attention_capture_score"] = attention_capture
            ranked.append(row)

        ranked.sort(
            key=lambda row: (
                -safe_float(row.get("attention_capture_score"), default=0.0),
                -safe_float(row.get("default_contract_score"), default=0.0),
                -safe_float(row.get("repricing_lane_prior"), default=0.0),
                safe_float(row.get("entry"), default=1.0),
                str(row.get("question") or ""),
            )
        )

        selected = ranked[0]
        for idx, row in enumerate(ranked, start=1):
            row["attention_flow_rank"] = idx
            row["attention_flow_selected"] = row is selected

        route_summaries.append(
            {
                "thesis_id": thesis_id,
                "thesis_type": thesis_type,
                "cluster_size": cluster_size,
                "selected_market_key": selected.get("market_key"),
                "selected_question": selected.get("question"),
                "selected_dimension_label": selected.get("thesis_dimension_label"),
                "selected_attention_capture_score": selected.get("attention_capture_score"),
                "members": [
                    {
                        "market_key": row.get("market_key"),
                        "question": row.get("question"),
                        "dimension_label": row.get("thesis_dimension_label"),
                        "retail_legibility_score": row.get("retail_legibility_score"),
                        "clickability_score": row.get("clickability_score"),
                        "narrative_portability_score": row.get("narrative_portability_score"),
                        "sibling_confusion_penalty": row.get("sibling_confusion_penalty"),
                        "attention_capture_score": row.get("attention_capture_score"),
                        "attention_flow_rank": row.get("attention_flow_rank"),
                        "selected": row.get("attention_flow_selected"),
                    }
                    for row in ranked
                ],
            }
        )

    route_summaries.sort(
        key=lambda row: (
            -safe_float(row.get("selected_attention_capture_score"), default=0.0),
            -int(row.get("cluster_size") or 0),
            str(row.get("thesis_id") or ""),
        )
    )
    return route_summaries
