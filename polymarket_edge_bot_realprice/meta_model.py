import json
import math
from bisect import bisect_right
from pathlib import Path


DEFAULT_NUMERIC_FIELDS = (
    "market_implied",
    "fair",
    "fair_lcb",
    "gross_edge",
    "net_edge",
    "net_edge_lcb",
    "confidence",
    "meta_confidence",
    "graph_consistency",
    "robustness_score",
    "spread",
    "cost_per_share",
    "quality",
    "momentum",
    "anomaly",
    "orderbook",
    "news",
    "external",
    "external_confidence",
    "domain_confidence",
    "relation_degree",
    "relation_confidence",
    "relation_support_confidence",
    "relation_residual",
    "relation_inconsistency",
    "event_size",
    "rank_in_event",
    "overround",
    "crowdedness",
    "correlation_penalty",
    "graph_penalty",
    "regime_penalty",
    "uncertainty",
    "total_penalty",
    "price_extremeness",
    "supported_adjustment",
    "overreach",
    "raw_adjustment",
    "fair_minus_market",
)

DEFAULT_CATEGORICAL_FIELDS = (
    "market_type",
    "category_group",
    "domain_name",
    "semantic_family",
)


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp_probability(value):
    if value is None:
        return None
    return max(0.001, min(0.999, float(value)))


def _logit(probability):
    probability = _clamp_probability(probability)
    return math.log(probability / (1.0 - probability))


def _sigmoid(value):
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _quantile_edges(values, bin_count):
    if not values:
        return []
    sorted_values = sorted(values)
    edges = []
    for idx in range(1, bin_count):
        position = int((len(sorted_values) - 1) * idx / bin_count)
        edge = sorted_values[position]
        if not edges or edge > edges[-1]:
            edges.append(edge)
    return edges


def _assign_bin(value, edges):
    return bisect_right(edges, value)


def _bucket_probability(positives, count, base_rate, smoothing):
    return (positives + (smoothing * base_rate)) / (count + smoothing)


def _fit_numeric_field(rows, field, base_rate, smoothing, bin_count, min_bucket_rows):
    labeled = []
    for row in rows:
        value = _safe_float(row.get(field))
        label = _safe_float(row.get("_label"))
        if value is None or label is None:
            continue
        labeled.append((value, label))
    if len(labeled) < max(min_bucket_rows * 2, 6):
        return {"enabled": False, "reason": "insufficient_rows", "edges": [], "buckets": []}

    edges = _quantile_edges([value for value, _ in labeled], bin_count)
    buckets = {}
    for value, label in labeled:
        bucket_idx = _assign_bin(value, edges)
        payload = buckets.setdefault(bucket_idx, {"count": 0, "positives": 0.0})
        payload["count"] += 1
        payload["positives"] += label

    serialized = []
    for idx in range(len(edges) + 1):
        payload = buckets.get(idx, {"count": 0, "positives": 0.0})
        count = payload["count"]
        positives = payload["positives"]
        if count < min_bucket_rows:
            weight = 0.0
            enabled = False
            probability = base_rate
        else:
            probability = _bucket_probability(positives, count, base_rate, smoothing)
            weight = _logit(probability) - _logit(base_rate)
            enabled = True
        serialized.append(
            {
                "index": idx,
                "count": count,
                "positive_rate": (positives / count) if count else None,
                "smoothed_probability": probability,
                "weight": weight,
                "enabled": enabled,
            }
        )

    return {
        "enabled": True,
        "reason": None,
        "edges": edges,
        "buckets": serialized,
    }


def _fit_categorical_field(rows, field, base_rate, smoothing, min_bucket_rows):
    grouped = {}
    for row in rows:
        value = row.get(field)
        label = _safe_float(row.get("_label"))
        if value is None or label is None:
            continue
        key = str(value)
        payload = grouped.setdefault(key, {"count": 0, "positives": 0.0})
        payload["count"] += 1
        payload["positives"] += label

    if len(grouped) < 1:
        return {"enabled": False, "reason": "no_values", "values": {}}

    values = {}
    for key, payload in sorted(grouped.items()):
        count = payload["count"]
        positives = payload["positives"]
        if count < min_bucket_rows:
            probability = base_rate
            weight = 0.0
            enabled = False
        else:
            probability = _bucket_probability(positives, count, base_rate, smoothing)
            weight = _logit(probability) - _logit(base_rate)
            enabled = True
        values[key] = {
            "count": count,
            "positive_rate": (positives / count) if count else None,
            "smoothed_probability": probability,
            "weight": weight,
            "enabled": enabled,
        }

    return {
        "enabled": True,
        "reason": None,
        "values": values,
    }


def fit_meta_model(
    rows,
    label_field="label_trade_positive",
    numeric_fields=DEFAULT_NUMERIC_FIELDS,
    categorical_fields=DEFAULT_CATEGORICAL_FIELDS,
    bin_count=5,
    smoothing=8.0,
    min_bucket_rows=4,
):
    labeled_rows = []
    for row in rows:
        label = _safe_float(row.get(label_field))
        if label is None:
            continue
        enriched = dict(row)
        enriched["_label"] = label
        labeled_rows.append(enriched)

    if not labeled_rows:
        raise ValueError("no labeled rows available for meta model")

    base_rate = sum(row["_label"] for row in labeled_rows) / len(labeled_rows)
    artifact = {
        "version": 1,
        "model_type": "additive_scorecard",
        "label_field": label_field,
        "row_count": len(labeled_rows),
        "base_rate": base_rate,
        "base_logit": _logit(base_rate),
        "bin_count": bin_count,
        "smoothing": smoothing,
        "min_bucket_rows": min_bucket_rows,
        "numeric_fields": {},
        "categorical_fields": {},
    }

    for field in numeric_fields:
        artifact["numeric_fields"][field] = _fit_numeric_field(
            labeled_rows,
            field=field,
            base_rate=base_rate,
            smoothing=smoothing,
            bin_count=bin_count,
            min_bucket_rows=min_bucket_rows,
        )

    for field in categorical_fields:
        artifact["categorical_fields"][field] = _fit_categorical_field(
            labeled_rows,
            field=field,
            base_rate=base_rate,
            smoothing=smoothing,
            min_bucket_rows=min_bucket_rows,
        )

    return artifact


def _score_numeric_field(value, payload):
    if not payload.get("enabled"):
        return 0.0, None
    numeric_value = _safe_float(value)
    if numeric_value is None:
        return 0.0, None
    edges = payload.get("edges") or []
    bucket_idx = _assign_bin(numeric_value, edges)
    buckets = payload.get("buckets") or []
    if bucket_idx >= len(buckets):
        return 0.0, bucket_idx
    bucket = buckets[bucket_idx]
    if not bucket.get("enabled"):
        return 0.0, bucket_idx
    return _safe_float(bucket.get("weight"), 0.0) or 0.0, bucket_idx


def _score_categorical_field(value, payload):
    if not payload.get("enabled") or value is None:
        return 0.0, None
    bucket = (payload.get("values") or {}).get(str(value))
    if not bucket or not bucket.get("enabled"):
        return 0.0, str(value)
    return _safe_float(bucket.get("weight"), 0.0) or 0.0, str(value)


def score_meta_row(row, artifact):
    if not artifact:
        return {"probability": None, "score": None, "trade_score": None, "contributions": {}}

    score = _safe_float(artifact.get("base_logit"), 0.0) or 0.0
    contributions = {}

    for field, payload in (artifact.get("numeric_fields") or {}).items():
        weight, bucket_idx = _score_numeric_field(row.get(field), payload)
        score += weight
        contributions[field] = {"bucket": bucket_idx, "weight": weight}

    for field, payload in (artifact.get("categorical_fields") or {}).items():
        weight, bucket_key = _score_categorical_field(row.get(field), payload)
        score += weight
        contributions[field] = {"bucket": bucket_key, "weight": weight}

    probability = _sigmoid(score)
    edge_reference = _safe_float(row.get("net_edge_lcb"))
    if edge_reference is None:
        edge_reference = _safe_float(row.get("net_edge"), 0.0) or 0.0

    return {
        "probability": probability,
        "score": score,
        "trade_score": probability * edge_reference,
        "contributions": contributions,
    }


def score_meta_rows(rows, artifact):
    scored = []
    for row in rows:
        updated = dict(row)
        prediction = score_meta_row(updated, artifact)
        updated["meta_trade_prob"] = prediction["probability"]
        updated["meta_trade_score"] = prediction["trade_score"]
        updated["meta_trade_model_score"] = prediction["score"]
        scored.append(updated)
    return scored


def brier_score(rows, probability_field, label_field):
    values = []
    for row in rows:
        prediction = _safe_float(row.get(probability_field))
        label = _safe_float(row.get(label_field))
        if prediction is None or label is None:
            continue
        values.append((prediction - label) ** 2)
    return (sum(values) / len(values)) if values else None


def log_loss(rows, probability_field, label_field):
    values = []
    for row in rows:
        prediction = _safe_float(row.get(probability_field))
        label = _safe_float(row.get(label_field))
        if prediction is None or label is None:
            continue
        probability = _clamp_probability(prediction)
        values.append(-(label * math.log(probability) + ((1.0 - label) * math.log(1.0 - probability))))
    return (sum(values) / len(values)) if values else None


def save_meta_model(artifact, output_path):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(artifact, fh, indent=2, ensure_ascii=True)


def load_meta_model(path):
    artifact_path = Path(path)
    with artifact_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _nested(source, key):
    if isinstance(source, dict):
        return source.get(key) or {}
    return getattr(source, key, {}) or {}


def _value(source, key, default=None):
    if isinstance(source, dict):
        return source.get(key, default)
    return getattr(source, key, default)


def build_meta_feature_row(candidate):
    model = _nested(candidate, "model")
    graph = model.get("graph") or {}
    robust = model.get("robust") or {}
    robust_components = robust.get("components") or {}
    external_components = model.get("external_components") or {}
    relation_metrics = external_components.get("relation_metrics") or {}
    relation_residual = external_components.get("relation_residual") or {}
    resolution_metadata = external_components.get("resolution_metadata") or {}
    policy = _nested(candidate, "policy")

    fair = _safe_float(_value(candidate, "fair"))
    entry = _safe_float(_value(candidate, "entry"))

    return {
        "market_type": _value(candidate, "market_type"),
        "category_group": _value(candidate, "category_group"),
        "domain_name": _value(candidate, "domain_name") or model.get("domain_name"),
        "semantic_family": _value(candidate, "semantic_family") or resolution_metadata.get("family"),
        "market_implied": entry,
        "fair": fair,
        "fair_lcb": _safe_float(_value(candidate, "fair_lcb")),
        "gross_edge": _safe_float(_value(candidate, "gross_edge")),
        "net_edge": _safe_float(_value(candidate, "net_edge")),
        "net_edge_lcb": _safe_float(_value(candidate, "net_edge_lcb")),
        "confidence": _safe_float(_value(candidate, "confidence")),
        "meta_confidence": _safe_float(_value(candidate, "meta_confidence")),
        "graph_consistency": _safe_float(_value(candidate, "graph_consistency")),
        "robustness_score": _safe_float(_value(candidate, "robustness_score")),
        "spread": _safe_float(_value(candidate, "spread")),
        "cost_per_share": _safe_float(_value(candidate, "cost_per_share")),
        "quality": _safe_float(model.get("quality")),
        "momentum": _safe_float(model.get("momentum")),
        "anomaly": _safe_float(model.get("anomaly")),
        "orderbook": _safe_float(model.get("orderbook")),
        "news": _safe_float(model.get("news")),
        "external": _safe_float(model.get("external")),
        "external_confidence": _safe_float(model.get("external_confidence")),
        "domain_confidence": _safe_float(_value(candidate, "domain_confidence") or model.get("domain_confidence")),
        "relation_degree": _safe_float(_value(candidate, "relation_degree") or relation_metrics.get("relation_degree")),
        "relation_confidence": _safe_float(
            _value(candidate, "relation_confidence") or relation_metrics.get("relation_confidence")
        ),
        "relation_support_confidence": _safe_float(
            _value(candidate, "relation_support_confidence") or relation_residual.get("support_confidence")
        ),
        "relation_residual": _safe_float(_value(candidate, "relation_residual") or relation_residual.get("residual")),
        "relation_inconsistency": _safe_float(
            _value(candidate, "relation_inconsistency") or relation_residual.get("inconsistency_score")
        ),
        "event_size": _safe_float(graph.get("event_size")),
        "rank_in_event": _safe_float(graph.get("rank_in_event")),
        "overround": _safe_float(graph.get("overround")),
        "crowdedness": _safe_float(graph.get("crowdedness")),
        "correlation_penalty": _safe_float(robust.get("correlation_penalty")),
        "graph_penalty": _safe_float(robust.get("graph_penalty")),
        "regime_penalty": _safe_float(robust.get("regime_penalty")),
        "uncertainty": _safe_float(robust.get("uncertainty") or robust_components.get("uncertainty")),
        "total_penalty": _safe_float(robust.get("total_penalty")),
        "price_extremeness": _safe_float(robust.get("price_extremeness")),
        "supported_adjustment": _safe_float(robust.get("supported_adjustment")),
        "overreach": _safe_float(robust.get("overreach")),
        "raw_adjustment": _safe_float(robust.get("raw_adjustment")),
        "fair_minus_market": (fair - entry) if fair is not None and entry is not None else None,
        "policy_min_confidence": _safe_float(policy.get("min_confidence")),
        "policy_min_gross_edge": _safe_float(policy.get("min_gross_edge")),
        "policy_edge_threshold": _safe_float(policy.get("edge_threshold")),
        "policy_min_meta_confidence": _safe_float(policy.get("min_meta_confidence")),
        "policy_min_graph_consistency": _safe_float(policy.get("min_graph_consistency")),
        "policy_min_robustness_score": _safe_float(policy.get("min_robustness_score")),
        "policy_min_lcb_edge": _safe_float(policy.get("min_lcb_edge")),
    }

