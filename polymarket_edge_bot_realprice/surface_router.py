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
            merged = {
                "sum": left["sum"] + right["sum"],
                "weight": left["weight"] + right["weight"],
                "start": left["start"],
                "end": right["end"],
            }
            blocks[-2:] = [merged]

    fitted = [0.0] * len(working)
    for block in blocks:
        mean = block["sum"] / block["weight"]
        for idx in range(block["start"], block["end"] + 1):
            fitted[idx] = mean

    if increasing:
        return fitted
    return [-value for value in fitted]


def _interpolated_surface_value(sorted_rows, fitted_prices, idx):
    if not fitted_prices:
        return None
    if idx <= 0 or idx >= len(sorted_rows) - 1:
        return fitted_prices[idx]

    prev_value = safe_float(sorted_rows[idx - 1].get("thesis_dimension_value"))
    current_value = safe_float(sorted_rows[idx].get("thesis_dimension_value"))
    next_value = safe_float(sorted_rows[idx + 1].get("thesis_dimension_value"))
    if prev_value is None or current_value is None or next_value is None or next_value <= prev_value:
        return fitted_prices[idx]

    alpha = (current_value - prev_value) / (next_value - prev_value)
    alpha = max(0.0, min(1.0, alpha))
    return fitted_prices[idx - 1] + ((fitted_prices[idx + 1] - fitted_prices[idx - 1]) * alpha)


def _surface_score(candidate, *, residual, monotonic_residual):
    repricing_score = safe_float(candidate.get("repricing_score"), default=0.0)
    watch_score = safe_float(candidate.get("repricing_watch_score"), default=0.0)
    lane_prior = safe_float(candidate.get("repricing_lane_prior"), default=0.0)
    confidence = safe_float(candidate.get("confidence"), default=0.0)
    entry = safe_float(candidate.get("entry"), default=1.0)
    verdict = str(candidate.get("repricing_verdict") or "")
    verdict_bonus = {
        "buy_now": 0.12,
        "watch_high_upside": 0.08,
        "watch": 0.04,
        "watch_late": -0.02,
    }.get(verdict, 0.0)
    return (
        (repricing_score * 0.55)
        + (watch_score * 0.35)
        + (lane_prior * 0.20)
        + (confidence * 0.12)
        + (max(0.0, residual) * 1.40)
        + (max(0.0, monotonic_residual) * 0.90)
        + verdict_bonus
        - (entry * 0.08)
    )


def _sorted_members(rows):
    return sorted(
        rows,
        key=lambda row: (
            safe_float(row.get("thesis_dimension_value"), default=-1.0),
            str(row.get("question") or ""),
        ),
    )


def annotate_surface_routes(*candidate_groups):
    grouped = _group_candidates(*candidate_groups)
    route_summaries = []

    for thesis_id, rows in grouped.items():
        thesis_type = str(rows[0].get("thesis_type") or "")
        direction = _SUPPORTED_LADDERS.get(thesis_type)
        cluster_size = len(rows)

        for candidate in rows:
            candidate["thesis_surface_selected"] = cluster_size <= 1 or direction is None
            candidate["thesis_surface_score"] = None
            candidate["thesis_surface_fit_price"] = None
            candidate["thesis_surface_interp_price"] = None
            candidate["thesis_surface_residual"] = 0.0
            candidate["thesis_surface_monotonic_residual"] = 0.0
            candidate["thesis_surface_rank"] = 1 if candidate["thesis_surface_selected"] else None

        if cluster_size <= 1 or direction is None:
            continue

        sorted_rows = _sorted_members(rows)
        observed_prices = [safe_float(row.get("entry"), default=0.0) for row in sorted_rows]
        fitted_prices = _isotonic_fit(observed_prices, increasing=(direction == "ascending"))

        ranked = []
        for idx, row in enumerate(sorted_rows):
            observed = observed_prices[idx]
            fitted = fitted_prices[idx]
            interp = _interpolated_surface_value(sorted_rows, fitted_prices, idx)
            monotonic_residual = fitted - observed
            residual = (interp - observed) if interp is not None else monotonic_residual
            score = _surface_score(row, residual=residual, monotonic_residual=monotonic_residual)
            row["thesis_surface_fit_price"] = fitted
            row["thesis_surface_interp_price"] = interp
            row["thesis_surface_residual"] = residual
            row["thesis_surface_monotonic_residual"] = monotonic_residual
            row["thesis_surface_score"] = score
            ranked.append(row)

        ranked.sort(
            key=lambda row: (
                -(safe_float(row.get("thesis_surface_score"), default=0.0)),
                -(safe_float(row.get("repricing_score"), default=0.0)),
                -(safe_float(row.get("repricing_watch_score"), default=0.0)),
                -(safe_float(row.get("repricing_lane_prior"), default=0.0)),
                safe_float(row.get("entry"), default=1.0),
            )
        )

        best_market_key = ranked[0].get("market_key")
        for idx, row in enumerate(ranked, start=1):
            row["thesis_surface_rank"] = idx
            row["thesis_surface_selected"] = row.get("market_key") == best_market_key

        route_summaries.append(
            {
                "thesis_id": thesis_id,
                "thesis_type": thesis_type,
                "direction": direction,
                "cluster_size": cluster_size,
                "selected_market_key": ranked[0].get("market_key"),
                "selected_question": ranked[0].get("question"),
                "selected_dimension_label": ranked[0].get("thesis_dimension_label"),
                "selected_surface_score": ranked[0].get("thesis_surface_score"),
                "members": [
                    {
                        "market_key": row.get("market_key"),
                        "question": row.get("question"),
                        "dimension_label": row.get("thesis_dimension_label"),
                        "entry": row.get("entry"),
                        "surface_score": row.get("thesis_surface_score"),
                        "surface_residual": row.get("thesis_surface_residual"),
                        "surface_rank": row.get("thesis_surface_rank"),
                        "selected": row.get("thesis_surface_selected"),
                    }
                    for row in ranked
                ],
            }
        )

    route_summaries.sort(
        key=lambda row: (
            -int(row.get("cluster_size") or 0),
            -(safe_float(row.get("selected_surface_score"), default=0.0)),
            str(row.get("thesis_id") or ""),
        )
    )
    return route_summaries
