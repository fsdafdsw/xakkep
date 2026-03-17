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


def _isotonic_fit(values, *, increasing):
    if not values:
        return []

    working = list(values if increasing else [-value for value in values])
    blocks = []
    for idx, value in enumerate(working):
        blocks.append(
            {
                "sum": value,
                "weight": 1.0,
                "start": idx,
                "end": idx,
            }
        )
        while len(blocks) >= 2:
            left = blocks[-2]
            right = blocks[-1]
            left_mean = left["sum"] / left["weight"]
            right_mean = right["sum"] / right["weight"]
            if left_mean <= right_mean:
                break
            blocks[-2:] = [
                {
                    "sum": left["sum"] + right["sum"],
                    "weight": left["weight"] + right["weight"],
                    "start": left["start"],
                    "end": right["end"],
                }
            ]

    fitted = [0.0] * len(working)
    for block in blocks:
        mean = block["sum"] / block["weight"]
        for idx in range(block["start"], block["end"] + 1):
            fitted[idx] = mean

    if increasing:
        return fitted
    return [-value for value in fitted]


def annotate_consistency_engine(*candidate_groups):
    grouped = _group_candidates(*candidate_groups)
    engine_summaries = []

    for thesis_id, rows in grouped.items():
        thesis_type = str(rows[0].get("thesis_type") or "")
        direction = _SUPPORTED_LADDERS.get(thesis_type)
        cluster_size = len(rows)

        for candidate in rows:
            candidate["consistency_engine_supported"] = cluster_size > 1 and direction is not None
            candidate["consistency_repaired_price"] = None
            candidate["consistency_residual"] = 0.0
            candidate["consistency_abs_residual"] = 0.0
            candidate["consistency_residual_rank"] = None
            candidate["consistency_selected"] = False
            candidate["consistency_bias"] = None

        if cluster_size <= 1 or direction is None:
            continue

        sorted_rows = _sorted_members(rows)
        observed_prices = [safe_float(row.get("entry"), default=0.0) for row in sorted_rows]
        repaired_prices = _isotonic_fit(observed_prices, increasing=(direction == "ascending"))

        for idx, row in enumerate(sorted_rows):
            repaired = repaired_prices[idx]
            observed = observed_prices[idx]
            residual = repaired - observed
            row["consistency_repaired_price"] = repaired
            row["consistency_residual"] = residual
            row["consistency_abs_residual"] = abs(residual)
            if residual > 1e-9:
                row["consistency_bias"] = "underpriced_yes"
            elif residual < -1e-9:
                row["consistency_bias"] = "overpriced_yes"
            else:
                row["consistency_bias"] = "fair"

        ranked = sorted(
            sorted_rows,
            key=lambda row: (
                -safe_float(row.get("consistency_residual"), default=0.0),
                -safe_float(row.get("consistency_abs_residual"), default=0.0),
                str(row.get("question") or ""),
            ),
        )
        selected = None
        for idx, row in enumerate(ranked, start=1):
            row["consistency_residual_rank"] = idx
            if selected is None and safe_float(row.get("consistency_residual"), default=0.0) > 0.0:
                selected = row

        if selected is not None:
            selected["consistency_selected"] = True

        most_overpriced = min(
            sorted_rows,
            key=lambda row: (
                safe_float(row.get("consistency_residual"), default=0.0),
                -safe_float(row.get("consistency_abs_residual"), default=0.0),
                str(row.get("question") or ""),
            ),
        )

        engine_summaries.append(
            {
                "thesis_id": thesis_id,
                "thesis_type": thesis_type,
                "direction": direction,
                "cluster_size": cluster_size,
                "selected_market_key": selected.get("market_key") if selected else None,
                "selected_question": selected.get("question") if selected else None,
                "selected_dimension_label": selected.get("thesis_dimension_label") if selected else None,
                "selected_residual": selected.get("consistency_residual") if selected else None,
                "most_overpriced_market_key": most_overpriced.get("market_key"),
                "most_overpriced_question": most_overpriced.get("question"),
                "most_overpriced_residual": most_overpriced.get("consistency_residual"),
                "members": [
                    {
                        "market_key": row.get("market_key"),
                        "question": row.get("question"),
                        "dimension_label": row.get("thesis_dimension_label"),
                        "entry": row.get("entry"),
                        "repaired_price": row.get("consistency_repaired_price"),
                        "residual": row.get("consistency_residual"),
                        "abs_residual": row.get("consistency_abs_residual"),
                        "bias": row.get("consistency_bias"),
                        "residual_rank": row.get("consistency_residual_rank"),
                        "selected": row.get("consistency_selected"),
                    }
                    for row in ranked
                ],
            }
        )

    engine_summaries.sort(
        key=lambda row: (
            -(safe_float(row.get("selected_residual"), default=0.0)),
            -int(row.get("cluster_size") or 0),
            str(row.get("thesis_id") or ""),
        )
    )
    return engine_summaries
