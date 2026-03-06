from collections import defaultdict

from relations import build_market_relations


_DESCENDING_THRESHOLD_DIRECTIONS = {"above", "at_or_above"}
_ASCENDING_THRESHOLD_DIRECTIONS = {"below"}


def _clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def _safe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_price(market):
    price = _safe_float(market.get("ref_price"))
    if price is not None:
        return _clamp(price, low=0.0, high=1.0)

    bid = _safe_float(market.get("best_bid"))
    ask = _safe_float(market.get("best_ask"))
    if bid is not None and ask is not None and 0 <= bid <= ask <= 1:
        return (bid + ask) / 2.0

    return None


def _default_bucket():
    return {
        "coverage_weight": 0.0,
        "signal_weight": 0.0,
        "support_gap_sum": 0.0,
        "violation_sum": 0.0,
        "violation_weight": 0.0,
        "positive_pressure": 0.0,
        "negative_pressure": 0.0,
        "confidence_sum": 0.0,
        "edge_count": 0,
        "constraint_count": 0,
        "violation_count": 0,
        "peer_ids": set(),
    }


def _record_coverage(bucket, confidence, peer_id=None, constraint=False):
    bucket["coverage_weight"] += confidence
    bucket["confidence_sum"] += confidence
    bucket["edge_count"] += 1
    if peer_id:
        bucket["peer_ids"].add(peer_id)
    if constraint:
        bucket["constraint_count"] += 1


def _apply_gap(bucket, gap, confidence):
    if abs(gap) < 1e-9:
        return
    bucket["signal_weight"] += confidence
    bucket["support_gap_sum"] += gap * confidence
    bucket["violation_sum"] += abs(gap) * confidence
    bucket["violation_weight"] += confidence
    bucket["violation_count"] += 1
    if gap > 0:
        bucket["positive_pressure"] += gap * confidence
    else:
        bucket["negative_pressure"] += abs(gap) * confidence


def _apply_lower_bound(bucket, current_price, bound_value, confidence, peer_id=None):
    _record_coverage(bucket, confidence, peer_id=peer_id, constraint=True)
    if current_price is None or bound_value is None:
        return
    if current_price < bound_value:
        _apply_gap(bucket, bound_value - current_price, confidence)


def _apply_upper_bound(bucket, current_price, bound_value, confidence, peer_id=None):
    _record_coverage(bucket, confidence, peer_id=peer_id, constraint=True)
    if current_price is None or bound_value is None:
        return
    if current_price > bound_value:
        _apply_gap(bucket, bound_value - current_price, confidence)


def _same_entity_compatible(left, right):
    left_meta = left.get("resolution_metadata") or {}
    right_meta = right.get("resolution_metadata") or {}

    if not left_meta or not right_meta:
        return False
    if left_meta.get("family") != right_meta.get("family"):
        return False
    if bool(left_meta.get("threshold")) != bool(right_meta.get("threshold")):
        return False
    if left_meta.get("comparator") != right_meta.get("comparator"):
        return False

    left_year = left_meta.get("target_year")
    right_year = right_meta.get("target_year")
    if left_year and right_year and left_year != right_year:
        return False

    left_date = left_meta.get("target_date")
    right_date = right_meta.get("target_date")
    if left_date and right_date and left_date != right_date:
        return False

    return True


def _exclusive_event_group(markets):
    if len(markets) < 3:
        return False

    winner_like = 0
    named_outcomes = 0
    for market in markets:
        market_type = market.get("market_type")
        family = (market.get("resolution_metadata") or {}).get("family")
        outcome = str(market.get("selected_outcome") or "").strip().lower()
        if market_type in {"winner_multi", "range_multi"} or family == "winner":
            winner_like += 1
        if outcome and outcome not in {"yes", "no", "true", "false"}:
            named_outcomes += 1

    return winner_like >= max(1, len(markets) // 2) or named_outcomes == len(markets)


def compute_relation_residuals(markets, relation_graph=None):
    graph = relation_graph or build_market_relations(markets)
    market_by_id = {market.get("id"): market for market in markets if market.get("id")}
    buckets = defaultdict(_default_bucket)
    grouped_by_event = defaultdict(list)

    for market in markets:
        market_id = market.get("id")
        if not market_id:
            continue
        event_key = market.get("event_id") or market.get("event_slug") or market_id
        grouped_by_event[event_key].append(market)
        buckets[market_id]

    for edge in graph.get("edges", []):
        left = market_by_id.get(edge.get("source_id"))
        right = market_by_id.get(edge.get("target_id"))
        if not left or not right:
            continue

        left_price = _safe_price(left)
        right_price = _safe_price(right)
        confidence = _clamp(_safe_float(edge.get("confidence")) or 0.0, low=0.0, high=1.0)
        detail = edge.get("detail") or {}
        relation_type = edge.get("type")

        if relation_type == "threshold_monotonic":
            direction = detail.get("direction")
            if direction in _DESCENDING_THRESHOLD_DIRECTIONS:
                _apply_lower_bound(buckets[left["id"]], left_price, right_price, confidence, peer_id=right.get("id"))
                _apply_upper_bound(buckets[right["id"]], right_price, left_price, confidence, peer_id=left.get("id"))
            elif direction in _ASCENDING_THRESHOLD_DIRECTIONS:
                _apply_upper_bound(buckets[left["id"]], left_price, right_price, confidence, peer_id=right.get("id"))
                _apply_lower_bound(buckets[right["id"]], right_price, left_price, confidence, peer_id=left.get("id"))

        elif relation_type == "time_monotonic":
            comparator = detail.get("comparator")
            if comparator in {"by", "before"}:
                _apply_upper_bound(buckets[left["id"]], left_price, right_price, confidence, peer_id=right.get("id"))
                _apply_lower_bound(buckets[right["id"]], right_price, left_price, confidence, peer_id=left.get("id"))
            elif comparator == "after":
                _apply_lower_bound(buckets[left["id"]], left_price, right_price, confidence, peer_id=right.get("id"))
                _apply_upper_bound(buckets[right["id"]], right_price, left_price, confidence, peer_id=left.get("id"))

        elif relation_type == "same_entity_cross_market" and _same_entity_compatible(left, right):
            soft_confidence = confidence * 0.40
            left_bucket = buckets[left["id"]]
            right_bucket = buckets[right["id"]]
            _record_coverage(left_bucket, soft_confidence, peer_id=right.get("id"), constraint=False)
            _record_coverage(right_bucket, soft_confidence, peer_id=left.get("id"), constraint=False)
            if left_price is not None and right_price is not None:
                gap = right_price - left_price
                if abs(gap) >= 0.08:
                    _apply_gap(left_bucket, gap * 0.35, soft_confidence)
                    _apply_gap(right_bucket, (-gap) * 0.35, soft_confidence)

    for event_markets in grouped_by_event.values():
        if not _exclusive_event_group(event_markets):
            continue
        priced = [(market, _safe_price(market)) for market in event_markets]
        priced = [(market, price) for market, price in priced if price is not None]
        if len(priced) < 3:
            continue

        total_price = sum(price for _, price in priced)
        overround = total_price - 1.0
        if overround <= 0.03:
            continue

        event_confidence = min(0.90, 0.52 + (0.04 * len(priced)))
        for market, price in priced:
            market_id = market.get("id")
            if not market_id:
                continue
            bucket = buckets[market_id]
            _record_coverage(bucket, event_confidence, constraint=True)
            allocated = overround * (price / max(total_price, 1e-9))
            _apply_gap(bucket, -allocated, event_confidence)

    residuals = {}
    for market in markets:
        market_id = market.get("id")
        current_price = _safe_price(market)
        bucket = buckets.get(market_id, _default_bucket())
        signal_weight = bucket["signal_weight"]
        coverage_weight = bucket["coverage_weight"]
        edge_count = bucket["edge_count"]
        mean_confidence = (
            bucket["confidence_sum"] / edge_count
            if edge_count
            else 0.0
        )
        residual = (
            bucket["support_gap_sum"] / signal_weight
            if signal_weight > 0
            else 0.0
        )
        constraint_violation = (
            bucket["violation_sum"] / bucket["violation_weight"]
            if bucket["violation_weight"] > 0
            else 0.0
        )
        positive_pressure = (
            bucket["positive_pressure"] / signal_weight
            if signal_weight > 0
            else 0.0
        )
        negative_pressure = (
            bucket["negative_pressure"] / signal_weight
            if signal_weight > 0
            else 0.0
        )
        directional_conflict = 0.0
        max_pressure = max(positive_pressure, negative_pressure)
        if max_pressure > 0:
            directional_conflict = min(positive_pressure, negative_pressure) / max_pressure

        coverage_ratio = _clamp(coverage_weight / 1.6)
        signal_ratio = _clamp(signal_weight / max(0.40, coverage_weight))
        inconsistency_score = _clamp(
            (constraint_violation / 0.05) * 0.72
            + (directional_conflict * 0.18)
            + (max(0.0, abs(residual) - 0.03) / 0.06) * 0.10
        )
        support_confidence = _clamp(
            0.16
            + (coverage_ratio * 0.28)
            + (signal_ratio * 0.18)
            + (mean_confidence * 0.18)
            - (directional_conflict * 0.16)
            - (inconsistency_score * 0.10)
        )

        support_price = None
        if current_price is not None:
            support_price = _clamp(current_price + residual, low=0.01, high=0.99)

        residuals[market_id] = {
            "support_price": support_price,
            "residual": residual,
            "constraint_violation": constraint_violation,
            "inconsistency_score": inconsistency_score,
            "support_confidence": support_confidence,
            "coverage_ratio": coverage_ratio,
            "signal_ratio": signal_ratio,
            "mean_relation_confidence": mean_confidence,
            "peer_count": len(bucket["peer_ids"]),
            "constraint_count": bucket["constraint_count"],
            "violation_count": bucket["violation_count"],
            "positive_pressure": positive_pressure,
            "negative_pressure": negative_pressure,
            "directional_conflict": directional_conflict,
        }

    return residuals


def annotate_relation_residuals(markets, relation_graph=None):
    residuals = compute_relation_residuals(markets, relation_graph=relation_graph)
    default_metrics = {
        "support_price": None,
        "residual": 0.0,
        "constraint_violation": 0.0,
        "inconsistency_score": 0.0,
        "support_confidence": 0.0,
        "coverage_ratio": 0.0,
        "signal_ratio": 0.0,
        "mean_relation_confidence": 0.0,
        "peer_count": 0,
        "constraint_count": 0,
        "violation_count": 0,
        "positive_pressure": 0.0,
        "negative_pressure": 0.0,
        "directional_conflict": 0.0,
    }
    for market in markets:
        market["relation_residual"] = residuals.get(market.get("id"), dict(default_metrics))
    return residuals
