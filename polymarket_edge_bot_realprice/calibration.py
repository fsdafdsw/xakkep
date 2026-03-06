import json
from pathlib import Path


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


def _fit_isotonic_blocks(points):
    if not points:
        return []

    blocks = []
    for probability, outcome in points:
        block = {
            "count": 1,
            "sum_outcome": outcome,
            "mean_outcome": outcome,
            "min_probability": probability,
            "max_probability": probability,
        }
        blocks.append(block)

        while len(blocks) >= 2 and blocks[-2]["mean_outcome"] > blocks[-1]["mean_outcome"]:
            right = blocks.pop()
            left = blocks.pop()
            merged_count = left["count"] + right["count"]
            merged_sum = left["sum_outcome"] + right["sum_outcome"]
            blocks.append(
                {
                    "count": merged_count,
                    "sum_outcome": merged_sum,
                    "mean_outcome": merged_sum / merged_count,
                    "min_probability": left["min_probability"],
                    "max_probability": right["max_probability"],
                }
            )

    return [
        {
            "low": block["min_probability"],
            "high": block["max_probability"],
            "value": _clamp_probability(block["mean_outcome"]),
            "count": block["count"],
        }
        for block in blocks
    ]


def fit_family_calibrators(rows, probability_field="fair", family_field="market_type", min_rows=30):
    grouped = {}
    for row in rows:
        probability = _safe_float(row.get(probability_field))
        outcome = _safe_float(row.get("resolved_outcome"))
        family = row.get(family_field) or "unknown"
        if probability is None or outcome is None:
            continue
        grouped.setdefault(family, []).append((_clamp_probability(probability), outcome))

    calibrators = {
        "method": "isotonic_pav",
        "probability_field": probability_field,
        "family_field": family_field,
        "min_rows": min_rows,
        "families": {},
    }

    for family, points in sorted(grouped.items()):
        points.sort(key=lambda item: item[0])
        if len(points) < min_rows:
            calibrators["families"][family] = {
                "enabled": False,
                "row_count": len(points),
                "reason": "insufficient_rows",
                "blocks": [],
            }
            continue

        blocks = _fit_isotonic_blocks(points)
        calibrators["families"][family] = {
            "enabled": True,
            "row_count": len(points),
            "reason": None,
            "blocks": blocks,
        }

    return calibrators


def save_calibrators(calibrators, output_path):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(calibrators, fh, indent=2, ensure_ascii=True)


def load_calibrators(path):
    artifact_path = Path(path)
    with artifact_path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def apply_family_calibration(probability, family, calibrators):
    probability = _clamp_probability(probability)
    if probability is None or not calibrators:
        return probability

    family_payload = (calibrators.get("families") or {}).get(family)
    if not family_payload or not family_payload.get("enabled"):
        return probability

    blocks = family_payload.get("blocks") or []
    if not blocks:
        return probability

    for block in blocks:
        low = _safe_float(block.get("low"), 0.0)
        high = _safe_float(block.get("high"), 1.0)
        if low <= probability <= high:
            return _clamp_probability(block.get("value"))

    if probability < _safe_float(blocks[0].get("low"), 0.0):
        return _clamp_probability(blocks[0].get("value"))
    return _clamp_probability(blocks[-1].get("value"))
