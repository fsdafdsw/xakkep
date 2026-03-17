from thesis_trade_policy import thesis_identity
from utils import safe_float


_SUPPORTED_LADDERS = {
    "threshold_ladder": "descending",
    "deadline_ladder": "ascending",
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


def _edge_violation(left_price, right_price, direction):
    if direction == "descending":
        return max(0.0, right_price - left_price)
    if direction == "ascending":
        return max(0.0, left_price - right_price)
    return 0.0


def annotate_consistency_graphs(*candidate_groups):
    grouped = _group_candidates(*candidate_groups)
    graph_summaries = []

    for thesis_id, rows in grouped.items():
        thesis_type = str(rows[0].get("thesis_type") or "")
        direction = _SUPPORTED_LADDERS.get(thesis_type)
        cluster_size = len(rows)

        for candidate in rows:
            candidate["consistency_supported"] = cluster_size > 1 and direction is not None
            candidate["consistency_direction"] = direction
            candidate["consistency_edge_count"] = 0
            candidate["consistency_local_violation_score"] = 0.0
            candidate["consistency_directional_violation_score"] = 0.0
            candidate["consistency_total_violation_score"] = 0.0
            candidate["consistency_max_edge_violation"] = 0.0
            candidate["consistency_violation_rank"] = None

        if cluster_size <= 1 or direction is None:
            continue

        sorted_rows = _sorted_members(rows)
        edges = []
        for left_idx, left in enumerate(sorted_rows[:-1]):
            left_price = safe_float(left.get("entry"), default=0.0)
            for right_idx, right in enumerate(sorted_rows[left_idx + 1 :], start=left_idx + 1):
                right_price = safe_float(right.get("entry"), default=0.0)
                violation = _edge_violation(left_price, right_price, direction)
                gap = left_price - right_price
                edge = {
                    "left_market_key": left.get("market_key"),
                    "left_question": left.get("question"),
                    "left_dimension_label": left.get("thesis_dimension_label"),
                    "left_entry": left_price,
                    "right_market_key": right.get("market_key"),
                    "right_question": right.get("question"),
                    "right_dimension_label": right.get("thesis_dimension_label"),
                    "right_entry": right_price,
                    "direction": direction,
                    "expected_relation": ">=" if direction == "descending" else "<=",
                    "observed_gap": gap,
                    "violation_score": violation,
                }
                edges.append(edge)

                left["consistency_edge_count"] += 1
                right["consistency_edge_count"] += 1
                left["consistency_total_violation_score"] += violation
                right["consistency_total_violation_score"] += violation
                left["consistency_max_edge_violation"] = max(left["consistency_max_edge_violation"], violation)
                right["consistency_max_edge_violation"] = max(right["consistency_max_edge_violation"], violation)

                if right_idx == left_idx + 1:
                    left["consistency_local_violation_score"] += violation
                    right["consistency_local_violation_score"] += violation

                if violation > 0.0:
                    offender = right if direction == "descending" else left
                    offender["consistency_directional_violation_score"] += violation

        ranked = sorted(
            sorted_rows,
            key=lambda row: (
                -safe_float(row.get("consistency_directional_violation_score"), default=0.0),
                -safe_float(row.get("consistency_total_violation_score"), default=0.0),
                -safe_float(row.get("consistency_max_edge_violation"), default=0.0),
                str(row.get("question") or ""),
            ),
        )
        for idx, row in enumerate(ranked, start=1):
            row["consistency_violation_rank"] = idx

        total_violation = sum(edge["violation_score"] for edge in edges)
        max_violation = max((edge["violation_score"] for edge in edges), default=0.0)
        graph_summaries.append(
            {
                "thesis_id": thesis_id,
                "thesis_type": thesis_type,
                "direction": direction,
                "cluster_size": cluster_size,
                "constraint_count": len(edges),
                "total_violation_score": total_violation,
                "max_violation_score": max_violation,
                "worst_market_key": ranked[0].get("market_key") if ranked else None,
                "worst_question": ranked[0].get("question") if ranked else None,
                "members": [
                    {
                        "market_key": row.get("market_key"),
                        "question": row.get("question"),
                        "dimension_label": row.get("thesis_dimension_label"),
                        "entry": row.get("entry"),
                        "directional_violation_score": row.get("consistency_directional_violation_score"),
                        "total_violation_score": row.get("consistency_total_violation_score"),
                        "local_violation_score": row.get("consistency_local_violation_score"),
                        "max_edge_violation": row.get("consistency_max_edge_violation"),
                        "violation_rank": row.get("consistency_violation_rank"),
                    }
                    for row in ranked
                ],
                "edges": edges[:20],
            }
        )

    graph_summaries.sort(
        key=lambda row: (
            -safe_float(row.get("total_violation_score"), default=0.0),
            -int(row.get("constraint_count") or 0),
            str(row.get("thesis_id") or ""),
        )
    )
    return graph_summaries
