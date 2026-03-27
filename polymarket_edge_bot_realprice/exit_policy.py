from utils import clamp, safe_float as _safe_float


def _clamp(value, low=0.01, high=0.99):
    return clamp(value, low, high)


def _clamp_price(value, low=0.0, high=0.99):
    return clamp(value, low, high)


def _policy_for(action_family, repricing_verdict=None, catalyst_type=None):
    action_family = str(action_family or "generic")
    repricing_verdict = str(repricing_verdict or "")
    catalyst_type = str(catalyst_type or "")
    policy = {
        "name": "generic_repricing",
        "take_profit_pct": 0.35,
        "take_profit_floor": 0.18,
        "take_profit_abs_min": 0.0,
        "stop_loss_pct": 0.18,
        "time_stop_days": 7.0,
        "trailing_arm_pct": 0.18,
        "trailing_drawdown_pct": 0.18,
        "stop_activation_hours": 6.0,
    }

    if action_family == "conflict":
        policy.update(
            {
                "name": "conflict_fast",
                "take_profit_pct": 0.55,
                "take_profit_floor": 0.10,
                "take_profit_abs_min": 0.0,
                "stop_loss_pct": 0.20,
                "time_stop_days": 3.0,
                "trailing_arm_pct": 0.25,
                "trailing_drawdown_pct": 0.18,
                "stop_activation_hours": 18.0,
            }
        )
    elif action_family == "diplomacy":
        policy.update(
            {
                "name": "diplomacy_patience",
                "take_profit_pct": 0.45,
                "take_profit_floor": 0.18,
                "take_profit_abs_min": 0.0,
                "stop_loss_pct": 0.28,
                "time_stop_days": 7.0 if repricing_verdict == "buy_now" else 10.0,
                "trailing_arm_pct": 0.22,
                "trailing_drawdown_pct": 0.18,
                "stop_activation_hours": 24.0,
            }
        )
    elif action_family == "release":
        policy.update(
            {
                "name": "release_event",
                "take_profit_pct": 0.40,
                "take_profit_floor": 0.25,
                "take_profit_abs_min": 0.0,
                "stop_loss_pct": 0.20,
                "time_stop_days": 8.0,
                "trailing_arm_pct": 0.20,
                "trailing_drawdown_pct": 0.16,
                "stop_activation_hours": 12.0,
            }
        )
        if catalyst_type in {"hearing", "court_ruling", "appeal"}:
            policy.update(
                {
                    "name": "release_hearing_fast",
                    "take_profit_pct": 0.80,
                    "take_profit_floor": 0.0,
                    "take_profit_abs_min": 0.010,
                    "stop_loss_pct": 0.45,
                    "time_stop_days": 4.0,
                    "trailing_arm_pct": 0.35,
                    "trailing_drawdown_pct": 0.20,
                    "stop_activation_hours": 30.0,
                }
            )
        elif catalyst_type == "hostage_release":
            policy.update(
                {
                    "name": "release_hostage_patience",
                    "take_profit_pct": 0.45,
                    "take_profit_floor": 0.18,
                    "take_profit_abs_min": 0.0,
                    "stop_loss_pct": 0.24,
                    "time_stop_days": 6.0,
                    "trailing_arm_pct": 0.22,
                    "trailing_drawdown_pct": 0.16,
                    "stop_activation_hours": 24.0,
                }
            )
    elif action_family == "regime_shift":
        policy.update(
            {
                "name": "regime_shift_binary",
                "take_profit_pct": 0.50,
                "take_profit_floor": 0.30,
                "take_profit_abs_min": 0.0,
                "stop_loss_pct": 0.16,
                "time_stop_days": 5.0,
                "trailing_arm_pct": 0.24,
                "trailing_drawdown_pct": 0.14,
                "stop_activation_hours": 8.0,
            }
        )
    elif action_family == "crypto_micro":
        policy.update(
            {
                "name": "crypto_micro_fast",
                "take_profit_pct": 0.14,
                "take_profit_floor": 0.0,
                "take_profit_abs_min": 0.035,
                "stop_loss_pct": 0.10,
                "time_stop_days": 0.04,
                "trailing_arm_pct": 0.08,
                "trailing_drawdown_pct": 0.10,
                "stop_activation_hours": 0.0,
            }
        )

    return policy


def should_execute_repricing_trade(repricing_verdict):
    return str(repricing_verdict or "") == "buy_now"


def _price_at_or_before(history, ts):
    candidate = None
    for point_ts, point_price in history:
        if point_ts <= ts:
            candidate = (point_ts, point_price)
        else:
            break
    return candidate


def simulate_exit(forward_history, *, entry_ts, settle_ts, entry_price, action_family, repricing_verdict=None, catalyst_type=None):
    policy = _policy_for(action_family, repricing_verdict=repricing_verdict, catalyst_type=catalyst_type)
    if not forward_history or entry_price is None or entry_price <= 0:
        return {
            "policy": policy,
            "take_profit_price": None,
            "stop_loss_price": None,
            "time_stop_ts": entry_ts,
            "exit_reason": "no_history",
            "exit_ts": entry_ts,
            "exit_price": entry_price,
            "exit_return_pct": 0.0,
            "holding_hours": 0.0,
        }

    take_profit_price = _clamp_price(
        max(
            entry_price * (1.0 + policy["take_profit_pct"]),
            policy["take_profit_floor"],
            entry_price + policy.get("take_profit_abs_min", 0.0),
        )
    )
    stop_loss_price = _clamp_price(entry_price * (1.0 - policy["stop_loss_pct"]))
    trailing_arm_price = _clamp_price(entry_price * (1.0 + policy["trailing_arm_pct"]))
    time_stop_ts = min(settle_ts, int(entry_ts + (policy["time_stop_days"] * 24 * 3600)))
    stop_activation_ts = int(entry_ts + (policy["stop_activation_hours"] * 3600))

    peak_price = entry_price
    trailing_active = False

    for ts, price in forward_history:
        if ts < entry_ts:
            continue
        peak_price = max(peak_price, price)
        if not trailing_active and price >= trailing_arm_price:
            trailing_active = True

        if price >= take_profit_price:
            exit_price = take_profit_price
            return {
                "policy": policy,
                "take_profit_price": take_profit_price,
                "stop_loss_price": stop_loss_price,
                "time_stop_ts": time_stop_ts,
                "exit_reason": "take_profit",
                "exit_ts": ts,
                "exit_price": exit_price,
                "exit_return_pct": (exit_price / entry_price) - 1.0,
                "holding_hours": max(0.0, (ts - entry_ts) / 3600.0),
            }

        if trailing_active:
            trailing_stop_price = peak_price * (1.0 - policy["trailing_drawdown_pct"])
            if price <= trailing_stop_price:
                exit_price = _clamp_price(trailing_stop_price)
                return {
                    "policy": policy,
                    "take_profit_price": take_profit_price,
                    "stop_loss_price": stop_loss_price,
                    "time_stop_ts": time_stop_ts,
                    "exit_reason": "trailing_stop",
                    "exit_ts": ts,
                    "exit_price": exit_price,
                    "exit_return_pct": (exit_price / entry_price) - 1.0,
                    "holding_hours": max(0.0, (ts - entry_ts) / 3600.0),
                }

        if ts >= stop_activation_ts and price <= stop_loss_price:
            exit_price = stop_loss_price
            return {
                "policy": policy,
                "take_profit_price": take_profit_price,
                "stop_loss_price": stop_loss_price,
                "time_stop_ts": time_stop_ts,
                "exit_reason": "stop_loss",
                "exit_ts": ts,
                "exit_price": exit_price,
                "exit_return_pct": (exit_price / entry_price) - 1.0,
                "holding_hours": max(0.0, (ts - entry_ts) / 3600.0),
            }

        if ts >= time_stop_ts:
            exit_price = price
            return {
                "policy": policy,
                "take_profit_price": take_profit_price,
                "stop_loss_price": stop_loss_price,
                "time_stop_ts": time_stop_ts,
                "exit_reason": "time_stop",
                "exit_ts": ts,
                "exit_price": exit_price,
                "exit_return_pct": (exit_price / entry_price) - 1.0,
                "holding_hours": max(0.0, (ts - entry_ts) / 3600.0),
            }

    fallback = _price_at_or_before(forward_history, min(settle_ts, time_stop_ts)) or forward_history[-1]
    exit_ts, exit_price = fallback
    fallback_reason = "time_stop" if exit_ts <= time_stop_ts else "settlement"
    return {
        "policy": policy,
        "take_profit_price": take_profit_price,
        "stop_loss_price": stop_loss_price,
        "time_stop_ts": time_stop_ts,
        "exit_reason": fallback_reason,
        "exit_ts": exit_ts,
        "exit_price": exit_price,
        "exit_return_pct": (exit_price / entry_price) - 1.0,
        "holding_hours": max(0.0, (exit_ts - entry_ts) / 3600.0),
    }


def live_exit_plan(action_family, repricing_verdict=None, entry_price=None, catalyst_type=None):
    policy = _policy_for(action_family, repricing_verdict=repricing_verdict, catalyst_type=catalyst_type)
    entry = _safe_float(entry_price, default=0.0)
    return {
        "policy_name": policy["name"],
        "take_profit_price": _clamp_price(
            max(
                entry * (1.0 + policy["take_profit_pct"]),
                policy["take_profit_floor"],
                entry + policy.get("take_profit_abs_min", 0.0),
            )
        ) if entry > 0 else None,
        "stop_loss_price": _clamp_price(entry * (1.0 - policy["stop_loss_pct"])) if entry > 0 else None,
        "time_stop_days": policy["time_stop_days"],
        "stop_activation_hours": policy["stop_activation_hours"],
        "trailing_arm_pct": policy["trailing_arm_pct"],
        "trailing_drawdown_pct": policy["trailing_drawdown_pct"],
    }
