import math


def _percentile(sorted_values, q):
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]

    position = (len(sorted_values) - 1) * q
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return sorted_values[lower]

    weight = position - lower
    return sorted_values[lower] + ((sorted_values[upper] - sorted_values[lower]) * weight)


def distribution_stats(values):
    clean = sorted(float(value) for value in values if value is not None)
    if not clean:
        return {
            "count": 0,
            "min": None,
            "max": None,
            "mean": None,
            "p10": None,
            "p50": None,
            "p90": None,
        }

    return {
        "count": len(clean),
        "min": clean[0],
        "max": clean[-1],
        "mean": sum(clean) / len(clean),
        "p10": _percentile(clean, 0.10),
        "p50": _percentile(clean, 0.50),
        "p90": _percentile(clean, 0.90),
    }
