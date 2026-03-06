from collections import defaultdict


def _event_key(market):
    return market.get("event_id") or market.get("event_slug") or market.get("id")


def _primary_entity_key(market):
    parsed = market.get("resolution_metadata") or {}
    if parsed.get("subject_entity_key"):
        return parsed["subject_entity_key"]

    entity_keys = (market.get("entity_metadata") or {}).get("entity_keys") or []
    return entity_keys[0] if entity_keys else None


def _add_edge(edges, metrics, left, right, relation_type, confidence, detail=None, seen_edges=None):
    left_id = left.get("id")
    right_id = right.get("id")
    if not left_id or not right_id or left_id == right_id:
        return
    edge_key = (relation_type, *sorted((left_id, right_id)))
    if seen_edges is not None and edge_key in seen_edges:
        return
    if seen_edges is not None:
        seen_edges.add(edge_key)

    edge = {
        "source_id": left_id,
        "target_id": right_id,
        "type": relation_type,
        "confidence": confidence,
    }
    if detail:
        edge["detail"] = detail
    edges.append(edge)

    for market_id in (left_id, right_id):
        bucket = metrics[market_id]
        bucket["relation_degree"] += 1
        bucket["relation_confidence_sum"] += confidence
        bucket["relation_types"][relation_type] += 1

    if relation_type == "mutually_exclusive":
        metrics[left_id]["exclusive_degree"] += 1
        metrics[right_id]["exclusive_degree"] += 1
    elif relation_type == "threshold_monotonic":
        metrics[left_id]["monotonic_degree"] += 1
        metrics[right_id]["monotonic_degree"] += 1
    elif relation_type == "same_entity_cross_market":
        metrics[left_id]["same_entity_degree"] += 1
        metrics[right_id]["same_entity_degree"] += 1


def _empty_metrics():
    return {
        "relation_degree": 0,
        "exclusive_degree": 0,
        "monotonic_degree": 0,
        "same_entity_degree": 0,
        "relation_confidence_sum": 0.0,
        "relation_confidence": 0.0,
        "related_event_count": 0,
        "relation_types": defaultdict(int),
    }


def _finalize_metrics(metrics, event_ids):
    finalized = {}
    for market_id, values in metrics.items():
        relation_degree = values["relation_degree"]
        relation_types = dict(values["relation_types"])
        finalized[market_id] = {
            "relation_degree": relation_degree,
            "exclusive_degree": values["exclusive_degree"],
            "monotonic_degree": values["monotonic_degree"],
            "same_entity_degree": values["same_entity_degree"],
            "relation_confidence": (
                values["relation_confidence_sum"] / relation_degree if relation_degree else 0.0
            ),
            "related_event_count": len(event_ids.get(market_id, set())),
            "relation_types": relation_types,
        }
    return finalized


def build_market_relations(markets):
    edges = []
    seen_edges = set()
    metrics = defaultdict(_empty_metrics)
    market_event_links = defaultdict(set)

    grouped_by_event = defaultdict(list)
    grouped_by_entity = defaultdict(list)
    grouped_by_threshold = defaultdict(list)
    grouped_by_date = defaultdict(list)

    for market in markets:
        market_id = market.get("id")
        if not market_id:
            continue

        metrics[market_id]

        event_key = _event_key(market)
        if event_key:
            grouped_by_event[event_key].append(market)

        primary_entity = _primary_entity_key(market)
        if primary_entity:
            grouped_by_entity[primary_entity].append(market)

        parsed = market.get("resolution_metadata") or {}
        threshold = parsed.get("threshold") or {}
        if primary_entity and threshold.get("kind") == "threshold":
            threshold_key = (primary_entity, threshold.get("direction"), threshold.get("unit"))
            grouped_by_threshold[threshold_key].append(market)

        if (
            primary_entity
            and parsed.get("target_date")
            and parsed.get("comparator") in {"by", "before", "after", "on"}
            and not parsed.get("threshold")
        ):
            date_key = (primary_entity, parsed.get("comparator"))
            grouped_by_date[date_key].append(market)

    for event_markets in grouped_by_event.values():
        if len(event_markets) <= 1:
            continue
        for idx, left in enumerate(event_markets):
            for right in event_markets[idx + 1 :]:
                confidence = min(
                    0.95,
                    0.55
                    + ((left.get("resolution_metadata") or {}).get("confidence", 0.4) * 0.15)
                    + ((right.get("resolution_metadata") or {}).get("confidence", 0.4) * 0.15),
                )
                _add_edge(edges, metrics, left, right, "mutually_exclusive", confidence, seen_edges=seen_edges)
                left_id = left.get("id")
                right_id = right.get("id")
                market_event_links[left_id].add(_event_key(right))
                market_event_links[right_id].add(_event_key(left))

    for threshold_markets in grouped_by_threshold.values():
        if len(threshold_markets) <= 1:
            continue
        ordered = sorted(
            threshold_markets,
            key=lambda market: float(((market.get("resolution_metadata") or {}).get("threshold") or {}).get("value") or 0.0),
        )
        for idx, left in enumerate(ordered[:-1]):
            right = ordered[idx + 1]
            confidence = min(
                0.92,
                0.58
                + ((left.get("resolution_metadata") or {}).get("confidence", 0.4) * 0.12)
                + ((right.get("resolution_metadata") or {}).get("confidence", 0.4) * 0.12),
            )
            _add_edge(edges, metrics, left, right, "threshold_monotonic", confidence, seen_edges=seen_edges)
            left_id = left.get("id")
            right_id = right.get("id")
            market_event_links[left_id].add(_event_key(right))
            market_event_links[right_id].add(_event_key(left))

    for date_markets in grouped_by_date.values():
        if len(date_markets) <= 1:
            continue
        ordered = sorted(date_markets, key=lambda market: (market.get("resolution_metadata") or {}).get("target_date") or "")
        for idx, left in enumerate(ordered[:-1]):
            right = ordered[idx + 1]
            confidence = min(
                0.90,
                0.56
                + ((left.get("resolution_metadata") or {}).get("confidence", 0.4) * 0.12)
                + ((right.get("resolution_metadata") or {}).get("confidence", 0.4) * 0.12),
            )
            _add_edge(edges, metrics, left, right, "time_monotonic", confidence, seen_edges=seen_edges)
            left_id = left.get("id")
            right_id = right.get("id")
            market_event_links[left_id].add(_event_key(right))
            market_event_links[right_id].add(_event_key(left))

    for entity_markets in grouped_by_entity.values():
        if len(entity_markets) <= 1:
            continue
        anchors = entity_markets[: min(4, len(entity_markets))]
        for anchor in anchors:
            for peer in entity_markets:
                if anchor is peer:
                    continue
                if _event_key(anchor) == _event_key(peer):
                    continue
                confidence = min(
                    0.80,
                    0.45
                    + ((anchor.get("resolution_metadata") or {}).get("confidence", 0.4) * 0.08)
                    + ((peer.get("resolution_metadata") or {}).get("confidence", 0.4) * 0.08),
                )
                _add_edge(edges, metrics, anchor, peer, "same_entity_cross_market", confidence, seen_edges=seen_edges)
                anchor_id = anchor.get("id")
                peer_id = peer.get("id")
                market_event_links[anchor_id].add(_event_key(peer))
                market_event_links[peer_id].add(_event_key(anchor))

    return {
        "edges": edges,
        "metrics": _finalize_metrics(metrics, market_event_links),
    }


def annotate_market_relations(markets):
    graph = build_market_relations(markets)
    metrics = graph["metrics"]
    for market in markets:
        market["relation_metrics"] = metrics.get(
            market.get("id"),
            {
                "relation_degree": 0,
                "exclusive_degree": 0,
                "monotonic_degree": 0,
                "same_entity_degree": 0,
                "relation_confidence": 0.0,
                "related_event_count": 0,
                "relation_types": {},
            },
        )
    return graph
