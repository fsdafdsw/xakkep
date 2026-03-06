from collections import defaultdict

from market_profile import as_int


def _clamp(value, low=0.0, high=1.0):
    return max(low, min(high, value))


def _safe_share(values):
    total = sum(values)
    if total <= 0:
        return [0.0 for _ in values]
    return [value / total for value in values]


def compute_event_graph_metrics(nodes):
    if not nodes:
        return []

    grouped = defaultdict(list)
    for idx, node in enumerate(nodes):
        grouped[node["event_key"]].append((idx, node))

    results = [None] * len(nodes)
    for entries in grouped.values():
        local_nodes = [node for _, node in entries]
        size = len(local_nodes)
        implieds = [float(node.get("implied") or 0.0) for node in local_nodes]
        fairs = [float(node.get("fair") or 0.0) for node in local_nodes]
        deltas = [fair - implied for fair, implied in zip(fairs, implieds)]
        fair_shares = _safe_share(fairs)

        mean_delta = sum(deltas) / size if size else 0.0
        dispersion = (sum(abs(delta - mean_delta) for delta in deltas) / size) if size else 0.0
        max_abs_delta = max(0.01, max(abs(delta) for delta in deltas)) if deltas else 0.01
        leader_delta = max(deltas) if deltas else 0.0
        ordered = sorted(range(size), key=lambda idx: deltas[idx], reverse=True)
        leader_gap = (
            deltas[ordered[0]] - deltas[ordered[1]]
            if size > 1
            else max(0.0, leader_delta)
        )
        rank_lookup = {local_idx: rank for rank, local_idx in enumerate(ordered)}

        multi_outcome = any(
            node.get("market_type") in {"winner_multi", "range_multi"}
            or as_int(node.get("event_market_count"), default=1) >= 3
            for node in local_nodes
        )
        sum_implied = sum(implieds) if multi_outcome else None
        overround = max(0.0, (sum_implied or 1.0) - 1.02) if multi_outcome else 0.0
        underround = max(0.0, 0.90 - (sum_implied or 1.0)) if multi_outcome else 0.0
        positive_count = sum(1 for delta in deltas if delta > 0)
        crowdedness = 0.0
        if size > 1 and positive_count > 1:
            crowdedness = (positive_count - 1) / (size - 1)

        dispersion_penalty = _clamp(dispersion / 0.03)
        event_bias_penalty = _clamp(abs(mean_delta) / 0.03)
        overround_penalty = _clamp(overround / 0.20)
        underround_penalty = _clamp(underround / 0.20)

        for local_idx, (global_idx, node) in enumerate(entries):
            rank = rank_lookup[local_idx]
            rank_score = 1.0 if size == 1 else 1.0 - (rank / (size - 1))
            delta = deltas[local_idx]

            if rank == 0:
                local_margin = leader_gap
            else:
                local_margin = delta - leader_delta

            margin_score = _clamp(0.5 + (local_margin / 0.05))
            support_score = _clamp(0.5 + (delta / (max_abs_delta * 2.0)))
            normalized_share = fair_shares[local_idx]
            normalized_gap = normalized_share - implieds[local_idx]

            consistency = 0.58
            consistency += rank_score * 0.14
            consistency += margin_score * 0.12
            consistency += support_score * 0.10
            consistency += _clamp(0.5 + (normalized_gap / 0.08)) * 0.08
            consistency -= dispersion_penalty * 0.10
            consistency -= event_bias_penalty * 0.10
            if multi_outcome:
                consistency -= overround_penalty * 0.14
                consistency -= crowdedness * 0.10
                consistency -= underround_penalty * 0.06

            consistency = _clamp(consistency)
            correlation_penalty = max(0.0, 0.60 - consistency) * 0.04
            if multi_outcome:
                correlation_penalty += overround_penalty * 0.03
                correlation_penalty += crowdedness * 0.02

            results[global_idx] = {
                "event_size": size,
                "rank_in_event": rank + 1,
                "sum_implied": sum_implied,
                "mean_delta": mean_delta,
                "dispersion": dispersion,
                "overround": overround,
                "underround": underround,
                "crowdedness": crowdedness,
                "normalized_share": normalized_share,
                "normalized_gap": normalized_gap,
                "consistency": consistency,
                "correlation_penalty": correlation_penalty,
                "multi_outcome": multi_outcome,
            }

    return results
