from config import (
    CONFLICT_FAST_LANE_BONUS,
    CONFLICT_LANE_PRIOR,
    CONFLICT_MIN_SETUP_SCORE,
    CONFLICT_MIN_URGENCY_SCORE,
    CONFLICT_REPRICING_BUY_SCORE,
    DIPLOMACY_CALL_HIGH_UPSIDE_MIN_ATTENTION_GAP,
    DIPLOMACY_CALL_HIGH_UPSIDE_OPTIONALITY,
    DIPLOMACY_CALL_MAX_ALREADY_PRICED,
    DIPLOMACY_CALL_MAX_ENTRY_PRICE,
    DIPLOMACY_CALL_MIN_ATTENTION_GAP,
    DIPLOMACY_MEETING_MAX_ALREADY_PRICED,
    DIPLOMACY_MEETING_MAX_ENTRY_PRICE,
    DIPLOMACY_MEETING_HIGH_UPSIDE_MIN_ATTENTION_GAP,
    DIPLOMACY_MEETING_HIGH_UPSIDE_MIN_WATCH_SCORE,
    DIPLOMACY_MEETING_LANE_PRIOR,
    DIPLOMACY_MEETING_MIN_ATTENTION_GAP,
    DIPLOMACY_MEETING_WATCH_SCORE,
    DIPLOMACY_RESUME_TALKS_LANE_PRIOR,
    DIPLOMACY_RESUME_TALKS_MIN_ATTENTION_GAP,
    DIPLOMACY_RESUME_TALKS_WATCH_SCORE,
    DIPLOMACY_CEASEFIRE_LANE_PRIOR,
    DIPLOMACY_CEASEFIRE_HIGH_UPSIDE_OPTIONALITY,
    DIPLOMACY_CEASEFIRE_WATCH_ONLY,
    DIPLOMACY_CEASEFIRE_WATCH_SCORE,
    DIPLOMACY_CALL_WATCH_ONLY,
    DIPLOMACY_CALL_WATCH_SCORE,
    DIPLOMACY_HIGH_UPSIDE_OPTIONALITY,
    DIPLOMACY_REPRICING_BUY_SCORE,
    DIPLOMACY_TALK_CALL_LANE_PRIOR,
    MAX_SPREAD,
    MAX_REPRICING_BUY_PRICE,
    MIN_REPRICING_BUY_SCORE,
    MIN_REPRICING_WATCH_SCORE,
    REGIME_SHIFT_LANE_PRIOR,
    REPRICING_LANE_PRIOR_WEIGHT,
    REPRICING_WATCH_PRIOR_WEIGHT,
    RELEASE_FAST_LANE_BONUS,
    RELEASE_GENERIC_LANE_PRIOR,
    RELEASE_HEARING_BUY_SCORE,
    RELEASE_HEARING_FAST_LANE_BONUS,
    RELEASE_HEARING_LANE_PRIOR,
    RELEASE_HOSTAGE_LANE_PRIOR,
    RELEASE_HEARING_MIN_LEGITIMACY_SCORE,
    RELEASE_HEARING_MIN_SUBJECT_SCORE,
    RELEASE_HOSTAGE_BUY_SCORE,
    RELEASE_HOSTAGE_WATCH_ONLY,
    RELEASE_HOSTAGE_WATCH_SCORE,
    RELEASE_HOSTAGE_MIN_LEGITIMACY_SCORE,
    RELEASE_HOSTAGE_MIN_SUBJECT_SCORE,
    RELEASE_MIN_LEGITIMACY_SCORE,
    RELEASE_MIN_SUBJECT_SCORE,
    RELEASE_REPRICING_BUY_SCORE,
)
from meeting_subtype import infer_meeting_subtype
from repricing_context import build_repricing_context
from utils import clamp as _base_clamp, safe_float as _safe_float


def _clamp(value, low=0.0, high=1.0):
    return _base_clamp(value, low, high)


def _domain_components(model):
    external = (model or {}).get("external_components") or {}
    domain = external.get("domain") or {}
    return domain.get("components") or {}


def _repricing_lane(action_family, catalyst_type=None, meeting_subtype=None):
    lane_key = "generic_repricing"
    lane_label = "Generic repricing"
    lane_prior = 0.50

    if action_family == "conflict":
        return "conflict_fast", "Conflict fast lane", CONFLICT_LANE_PRIOR

    if action_family == "release":
        if catalyst_type in {"hearing", "court_ruling", "appeal"}:
            return "release_hearing", "Release hearing lane", RELEASE_HEARING_LANE_PRIOR
        if catalyst_type == "hostage_release":
            return "release_hostage", "Hostage release lane", RELEASE_HOSTAGE_LANE_PRIOR
        return "release_generic", "Release lane", RELEASE_GENERIC_LANE_PRIOR

    if action_family == "diplomacy":
        if catalyst_type == "call_or_meeting":
            if meeting_subtype == "talk_call":
                return "diplomacy_talk_call", "Talk / call lane", DIPLOMACY_TALK_CALL_LANE_PRIOR
            if meeting_subtype == "meeting":
                return "diplomacy_meeting", "Meeting lane", DIPLOMACY_MEETING_LANE_PRIOR
            if meeting_subtype == "resume_talks":
                return "diplomacy_resume_talks", "Resume talks lane", DIPLOMACY_RESUME_TALKS_LANE_PRIOR
            return "diplomacy_call_generic", "Call / meeting lane", DIPLOMACY_TALK_CALL_LANE_PRIOR
        if catalyst_type == "ceasefire":
            return "diplomacy_ceasefire", "Ceasefire lane", DIPLOMACY_CEASEFIRE_LANE_PRIOR
        return "diplomacy_generic", "Diplomacy lane", 0.48

    if action_family == "regime_shift":
        return "regime_shift", "Regime shift lane", REGIME_SHIFT_LANE_PRIOR

    return lane_key, lane_label, lane_prior


def repricing_lane_info(action_family, catalyst_type=None, meeting_subtype=None):
    lane_key, lane_label, lane_prior = _repricing_lane(action_family, catalyst_type, meeting_subtype)
    return {
        "lane_key": lane_key,
        "lane_label": lane_label,
        "lane_prior": lane_prior,
    }


def _family_policy(action_family, catalyst_hardness, catalyst_type=None, meeting_subtype=None):
    policy = {
        "buy_threshold": MIN_REPRICING_BUY_SCORE,
        "watch_threshold": MIN_REPRICING_WATCH_SCORE,
        "min_underreaction": 0.48,
        "min_fresh": 0.58,
        "max_chase": 0.16,
        "max_already_priced": 0.34,
        "high_upside_optionality": 0.70,
        "high_upside_max_chase": 0.10,
        "high_upside_max_already_priced": 0.22,
        "high_upside_min_watch_score": 0.0,
        "allow_high_upside": True,
        "allow_buy_now": True,
        "require_hard_for_buy": False,
        "require_official_source_for_buy": False,
        "min_setup_score": 0.0,
        "min_urgency_score": 0.0,
        "family_bonus": 0.0,
        "max_entry_price": MAX_REPRICING_BUY_PRICE,
        "min_attention_gap": 0.0,
        "high_upside_min_attention_gap": 0.0,
    }

    if action_family == "conflict":
        policy.update(
            {
                "buy_threshold": CONFLICT_REPRICING_BUY_SCORE,
                "min_underreaction": 0.52,
                "min_fresh": 0.66,
                "max_chase": 0.10,
                "max_already_priced": 0.24,
                "allow_high_upside": False,
                "min_setup_score": CONFLICT_MIN_SETUP_SCORE,
                "min_urgency_score": CONFLICT_MIN_URGENCY_SCORE,
                "family_bonus": CONFLICT_FAST_LANE_BONUS,
            }
        )
    elif action_family == "diplomacy":
        policy.update(
            {
                "buy_threshold": DIPLOMACY_REPRICING_BUY_SCORE,
                "min_underreaction": 0.58,
                "min_fresh": 0.70 if catalyst_hardness == "hard" else 0.78,
                "max_chase": 0.08,
                "max_already_priced": 0.20,
                "high_upside_optionality": DIPLOMACY_HIGH_UPSIDE_OPTIONALITY,
                "high_upside_max_chase": 0.12,
                "high_upside_max_already_priced": 0.26,
                "allow_high_upside": True,
                "require_hard_for_buy": True,
                "require_official_source_for_buy": True,
            }
        )
        if catalyst_type == "call_or_meeting":
            policy.update(
                {
                    "watch_threshold": DIPLOMACY_CALL_WATCH_SCORE,
                    "min_underreaction": 0.56,
                    "min_fresh": 0.72,
                    "max_chase": 0.10,
                    "max_already_priced": DIPLOMACY_CALL_MAX_ALREADY_PRICED,
                    "high_upside_optionality": DIPLOMACY_CALL_HIGH_UPSIDE_OPTIONALITY,
                    "high_upside_max_chase": 0.14,
                    "high_upside_max_already_priced": DIPLOMACY_CALL_MAX_ALREADY_PRICED,
                    "allow_buy_now": not DIPLOMACY_CALL_WATCH_ONLY,
                    "require_hard_for_buy": False,
                    "require_official_source_for_buy": False,
                    "max_entry_price": DIPLOMACY_CALL_MAX_ENTRY_PRICE,
                    "min_attention_gap": DIPLOMACY_CALL_MIN_ATTENTION_GAP,
                    "high_upside_min_attention_gap": DIPLOMACY_CALL_HIGH_UPSIDE_MIN_ATTENTION_GAP,
                }
            )
            if meeting_subtype == "meeting":
                policy.update(
                    {
                        "watch_threshold": DIPLOMACY_MEETING_WATCH_SCORE,
                        "min_underreaction": 0.58,
                        "min_fresh": 0.72,
                        "max_already_priced": DIPLOMACY_MEETING_MAX_ALREADY_PRICED,
                        "high_upside_max_already_priced": DIPLOMACY_MEETING_MAX_ALREADY_PRICED,
                        "max_entry_price": DIPLOMACY_MEETING_MAX_ENTRY_PRICE,
                        "min_attention_gap": DIPLOMACY_MEETING_MIN_ATTENTION_GAP,
                        "high_upside_min_attention_gap": DIPLOMACY_MEETING_HIGH_UPSIDE_MIN_ATTENTION_GAP,
                        "high_upside_min_watch_score": DIPLOMACY_MEETING_HIGH_UPSIDE_MIN_WATCH_SCORE,
                    }
                )
            elif meeting_subtype == "resume_talks":
                policy.update(
                    {
                        "watch_threshold": DIPLOMACY_RESUME_TALKS_WATCH_SCORE,
                        "min_underreaction": 0.54,
                        "min_fresh": 0.68,
                        "min_attention_gap": DIPLOMACY_RESUME_TALKS_MIN_ATTENTION_GAP,
                        "high_upside_min_attention_gap": DIPLOMACY_RESUME_TALKS_MIN_ATTENTION_GAP,
                    }
                )
        elif catalyst_type == "ceasefire":
            policy.update(
                {
                    "watch_threshold": DIPLOMACY_CEASEFIRE_WATCH_SCORE,
                    "min_underreaction": 0.56,
                    "min_fresh": 0.74,
                    "max_chase": 0.09,
                    "max_already_priced": 0.20,
                    "high_upside_optionality": DIPLOMACY_CEASEFIRE_HIGH_UPSIDE_OPTIONALITY,
                    "high_upside_max_chase": 0.12,
                    "high_upside_max_already_priced": 0.24,
                    "allow_buy_now": not DIPLOMACY_CEASEFIRE_WATCH_ONLY,
                    "require_hard_for_buy": False,
                    "require_official_source_for_buy": False,
                }
            )
    elif action_family == "release":
        policy.update(
            {
                "buy_threshold": RELEASE_REPRICING_BUY_SCORE,
                "min_underreaction": 0.50,
                "min_fresh": 0.62,
                "max_chase": 0.12,
                "max_already_priced": 0.28,
                "allow_high_upside": True,
                "min_setup_score": RELEASE_MIN_SUBJECT_SCORE,
                "min_urgency_score": RELEASE_MIN_LEGITIMACY_SCORE,
                "family_bonus": RELEASE_FAST_LANE_BONUS,
            }
        )
        if catalyst_type in {"hearing", "court_ruling", "appeal"}:
            policy.update(
                {
                    "buy_threshold": RELEASE_HEARING_BUY_SCORE,
                    "min_underreaction": 0.56,
                    "min_fresh": 0.64,
                    "max_chase": 0.10,
                    "max_already_priced": 0.20,
                    "min_setup_score": RELEASE_HEARING_MIN_SUBJECT_SCORE,
                    "min_urgency_score": RELEASE_HEARING_MIN_LEGITIMACY_SCORE,
                    "family_bonus": RELEASE_HEARING_FAST_LANE_BONUS,
                }
            )
        elif catalyst_type == "hostage_release":
            policy.update(
                {
                    "buy_threshold": RELEASE_HOSTAGE_BUY_SCORE,
                    "watch_threshold": RELEASE_HOSTAGE_WATCH_SCORE,
                    "min_underreaction": 0.54,
                    "min_fresh": 0.68,
                    "max_chase": 0.10,
                    "max_already_priced": 0.16,
                    "allow_buy_now": not RELEASE_HOSTAGE_WATCH_ONLY,
                    "min_setup_score": RELEASE_HOSTAGE_MIN_SUBJECT_SCORE,
                    "min_urgency_score": RELEASE_HOSTAGE_MIN_LEGITIMACY_SCORE,
                    "family_bonus": RELEASE_FAST_LANE_BONUS,
                }
            )
    elif action_family == "regime_shift":
        policy.update(
            {
                "buy_threshold": max(MIN_REPRICING_BUY_SCORE, 0.80),
                "min_underreaction": 0.54,
                "min_fresh": 0.68,
                "max_chase": 0.10,
                "max_already_priced": 0.22,
                "allow_high_upside": True,
            }
        )

    return policy


def score_repricing_signal(
    *,
    entry_price,
    confidence,
    net_edge,
    net_edge_lcb,
    spread,
    liquidity=None,
    volume24h=None,
    one_hour_change=None,
    one_day_change=None,
    one_week_change=None,
    hours_to_close=None,
    model,
    market_type=None,
    category_group=None,
    question=None,
):
    model = model or {}
    domain_name = model.get("domain_name")
    components = _domain_components(model)
    relation_residual = ((model.get("external_components") or {}).get("relation_residual") or {})

    if domain_name != "geopolitical_repricing":
        return {
            "score": 0.0,
            "watch_score": 0.0,
            "verdict": "ignore",
            "reason": "not repricing domain",
            "buy_now": False,
            "watch": False,
            "attention_gap": 0.0,
            "stale_score": 0.0,
            "already_priced_penalty": 0.0,
            "catalyst_type": components.get("catalyst_type") or "generic",
            "catalyst_strength": _safe_float(components.get("catalyst_strength")),
            "action_family": components.get("action_family"),
        }

    entry_price = _safe_float(entry_price, 0.5)
    spread = _safe_float(spread, 0.0)
    confidence = _safe_float(confidence, 0.5)
    net_edge = _safe_float(net_edge, 0.0)
    net_edge_lcb = _safe_float(net_edge_lcb, 0.0)
    domain_confidence = _safe_float(model.get("domain_confidence"), 0.5)
    repricing_potential = _safe_float(components.get("repricing_potential"), 0.0)
    catalyst_strength = _safe_float(components.get("catalyst_strength"), 0.0)
    catalyst_type = str(components.get("catalyst_type") or "generic")
    meeting_subtype = infer_meeting_subtype(question, catalyst_type=catalyst_type)
    catalyst_hardness = str(components.get("catalyst_hardness") or "soft")
    catalyst_reversibility = str(components.get("catalyst_reversibility") or "high")
    catalyst_has_official_source = bool(components.get("catalyst_has_official_source"))
    hard_state = bool(components.get("hard_state"))
    binary_event_grid = bool(components.get("binary_event_grid"))
    question_geo_keywords = list(components.get("question_geo_keywords") or [])
    institution_keywords = list(components.get("institution_keywords") or [])
    release_context_keywords = list(components.get("release_context_keywords") or [])
    release_figure_keywords = list(components.get("release_figure_keywords") or [])
    liquidity = _safe_float(liquidity, _safe_float(components.get("liquidity"), 0.0))
    volume24h = _safe_float(volume24h, _safe_float(components.get("volume24h"), 0.0))
    one_hour_change = _safe_float(one_hour_change, _safe_float(components.get("one_hour_change"), 0.0))
    one_day_change = _safe_float(one_day_change, _safe_float(components.get("one_day_change"), 0.0))
    one_week_change = _safe_float(one_week_change, _safe_float(components.get("one_week_change"), 0.0))
    hours_to_close = _safe_float(hours_to_close, _safe_float(components.get("hours_to_close"), 0.0))
    relation_support_confidence = _safe_float(relation_residual.get("support_confidence"), 0.0)
    relation_residual_gap = _safe_float(relation_residual.get("residual"), 0.0)

    repricing_context = build_repricing_context(
        entry_price=entry_price,
        repricing_potential=repricing_potential,
        catalyst_strength=catalyst_strength,
        spread=spread,
        liquidity=liquidity,
        volume24h=volume24h,
        one_hour_change=one_hour_change,
        one_day_change=one_day_change,
        one_week_change=one_week_change,
        hours_to_close=hours_to_close,
        max_buy_price=MAX_REPRICING_BUY_PRICE,
    )
    attention_gap = repricing_context["attention_gap"]
    stale_score = repricing_context["stale_score"]
    already_priced_penalty = repricing_context["already_priced_penalty"]
    underreaction_score = repricing_context["underreaction_score"]
    fresh_catalyst_score = repricing_context["fresh_catalyst_score"]
    trend_chase_penalty = repricing_context["trend_chase_penalty"]
    compression_score = repricing_context["compression_score"]
    deadline_pressure = repricing_context["deadline_pressure"]
    spread_penalty = _clamp(max(0.0, spread - min(MAX_SPREAD, 0.05)) * 6.0)
    liquidity_penalty = _clamp(max(0.0, (250.0 - liquidity) / 250.0) * 0.25)
    volume_penalty = _clamp(max(0.0, (200.0 - volume24h) / 200.0) * 0.18)
    low_price_optionality = _clamp(max(0.0, 0.24 - entry_price) / 0.24)

    catalyst_bonus = catalyst_strength * 0.24
    if catalyst_hardness == "hard":
        catalyst_bonus += 0.05
    if catalyst_reversibility == "low":
        catalyst_bonus += 0.03
    if catalyst_has_official_source:
        catalyst_bonus += 0.04

    score = 0.18
    score += repricing_potential * 0.17
    score += underreaction_score * 0.20
    score += fresh_catalyst_score * 0.16
    score += attention_gap * 0.12
    score += stale_score * 0.10
    score += catalyst_bonus
    score += (confidence - 0.5) * 0.18
    score += (domain_confidence - 0.5) * 0.12
    score += max(-0.03, min(0.03, relation_residual_gap)) * 1.1
    score += relation_support_confidence * 0.05
    score += max(0.0, min(0.03, net_edge)) * 1.8
    score -= max(0.0, -net_edge_lcb) * 0.35
    score -= already_priced_penalty * 0.28
    score -= trend_chase_penalty * 0.18
    score -= spread_penalty * 0.18
    score -= liquidity_penalty
    score -= volume_penalty

    watch_score = (
        score
        + (attention_gap * 0.08)
        + (underreaction_score * 0.08)
        + (fresh_catalyst_score * 0.06)
        + (0.04 if catalyst_type in {"hostage_release", "appeal", "court_ruling", "military_action"} else 0.0)
    )
    optionality_score = _clamp(
        (repricing_potential * 0.18)
        + (underreaction_score * 0.24)
        + (attention_gap * 0.16)
        + (low_price_optionality * 0.18)
        + (_clamp(1.0 - trend_chase_penalty) * 0.12)
        + (_clamp(1.0 - already_priced_penalty) * 0.12)
    )
    score = _clamp(score)
    watch_score = _clamp(watch_score)
    action_family = components.get("action_family")
    family_policy = _family_policy(action_family, catalyst_hardness, catalyst_type, meeting_subtype)
    lane_key, lane_label, lane_prior = _repricing_lane(action_family, catalyst_type, meeting_subtype)
    conflict_setup_score = 0.0
    conflict_urgency_score = 0.0
    release_subject_score = 0.0
    release_legitimacy_score = 0.0
    if action_family == "conflict":
        conflict_setup_score = _clamp(
            (underreaction_score * 0.32)
            + (attention_gap * 0.20)
            + (fresh_catalyst_score * 0.18)
            + (stale_score * 0.12)
            + (confidence * 0.10)
            + (domain_confidence * 0.08)
        )
        conflict_urgency_score = _clamp(
            (deadline_pressure * 0.34)
            + (compression_score * 0.18)
            + (_clamp(1.0 - trend_chase_penalty) * 0.18)
            + (_clamp(1.0 - already_priced_penalty) * 0.18)
            + (_clamp(1.0 - entry_price / max(MAX_REPRICING_BUY_PRICE, 0.01)) * 0.12)
        )
        score = _clamp(
            score
            + (conflict_setup_score * 0.08)
            + (conflict_urgency_score * 0.07)
            + family_policy["family_bonus"]
        )
        watch_score = _clamp(
            watch_score
            + (conflict_setup_score * 0.05)
            + (conflict_urgency_score * 0.04)
            + (family_policy["family_bonus"] * 0.5)
        )
    elif action_family == "release":
        figure_bonus = 0.18 if release_figure_keywords else 0.0
        hostage_bonus = 0.16 if catalyst_type == "hostage_release" else 0.0
        legal_context_bonus = min(0.14, len(release_context_keywords) * 0.02)
        geo_bonus = min(0.10, len(question_geo_keywords) * 0.02)
        human_case_bonus = 0.12 if (release_figure_keywords or catalyst_type == "hostage_release") else 0.0
        release_subject_score = _clamp(
            (underreaction_score * 0.22)
            + (attention_gap * 0.14)
            + figure_bonus
            + hostage_bonus
            + legal_context_bonus
            + geo_bonus
            + human_case_bonus
        )
        official_bonus = 0.14 if catalyst_has_official_source else 0.0
        hard_bonus = 0.10 if catalyst_hardness == "hard" else 0.0
        low_reversibility_bonus = 0.06 if catalyst_reversibility == "low" else 0.0
        institution_bonus = min(0.10, len(institution_keywords) * 0.02)
        binary_penalty = 0.08 if binary_event_grid else 0.0
        generic_penalty = 0.12 if (not release_figure_keywords and catalyst_type not in {"hostage_release"}) else 0.0
        release_legitimacy_score = _clamp(
            (fresh_catalyst_score * 0.26)
            + (deadline_pressure * 0.12)
            + (confidence * 0.08)
            + (domain_confidence * 0.06)
            + official_bonus
            + hard_bonus
            + low_reversibility_bonus
            + institution_bonus
            - binary_penalty
            - generic_penalty
        )
        score = _clamp(
            score
            + (release_subject_score * 0.06)
            + (release_legitimacy_score * 0.05)
            + family_policy["family_bonus"]
        )
        watch_score = _clamp(
            watch_score
            + (release_subject_score * 0.04)
            + (release_legitimacy_score * 0.03)
            + (family_policy["family_bonus"] * 0.4)
        )
    prior_delta = lane_prior - 0.50
    score = _clamp(score + (prior_delta * REPRICING_LANE_PRIOR_WEIGHT))
    watch_score = _clamp(watch_score + (prior_delta * REPRICING_WATCH_PRIOR_WEIGHT))
    clean_entry = (
        entry_price <= family_policy["max_entry_price"]
        and underreaction_score >= family_policy["min_underreaction"]
        and fresh_catalyst_score >= family_policy["min_fresh"]
        and attention_gap >= family_policy["min_attention_gap"]
        and trend_chase_penalty <= family_policy["max_chase"]
        and already_priced_penalty <= family_policy["max_already_priced"]
        and (
            action_family != "conflict"
            or (
                conflict_setup_score >= family_policy["min_setup_score"]
                and conflict_urgency_score >= family_policy["min_urgency_score"]
            )
        )
        and (
            action_family != "release"
            or (
                release_subject_score >= family_policy["min_setup_score"]
                and release_legitimacy_score >= family_policy["min_urgency_score"]
            )
        )
        and (
            not family_policy["require_hard_for_buy"]
            or catalyst_hardness == "hard"
        )
        and (
            not family_policy["require_official_source_for_buy"]
            or catalyst_has_official_source
        )
    )

    verdict = "ignore"
    reason = "repricing score too low"
    buy_ready = score >= family_policy["buy_threshold"] and clean_entry
    if buy_ready and family_policy["allow_buy_now"]:
        verdict = "buy_now"
        if action_family == "release" and catalyst_type in {"hearing", "court_ruling", "appeal"}:
            reason = "hard legal catalyst and market still underreacted"
        elif action_family == "release" and catalyst_type == "hostage_release":
            reason = "credible hostage-release setup with room left in pricing"
        elif action_family == "diplomacy" and catalyst_type == "call_or_meeting":
            if meeting_subtype == "talk_call":
                reason = "talk-or-call setup looks credible and market still underreacted"
            elif meeting_subtype == "resume_talks":
                reason = "talks-resume setup looks credible and market still underreacted"
            else:
                reason = "meeting setup looks credible and market still underreacted"
        elif action_family == "diplomacy" and catalyst_type == "ceasefire":
            reason = "ceasefire setup looks credible and market still underreacted"
        else:
            reason = "fresh catalyst and market still underreacted"
    elif buy_ready and not family_policy["allow_buy_now"]:
        verdict = "watch"
        if action_family == "release" and catalyst_type == "hostage_release":
            reason = "credible hostage-release setup, but this family stays watch-only"
        elif action_family == "diplomacy" and catalyst_type == "call_or_meeting":
            if meeting_subtype == "talk_call":
                reason = "talk-or-call setup looks live, but this family stays watch-only"
            elif meeting_subtype == "resume_talks":
                reason = "talks-resume setup looks live, but this family stays watch-only"
            else:
                reason = "meeting setup looks live, but this family stays watch-only"
        elif action_family == "diplomacy" and catalyst_type == "ceasefire":
            reason = "ceasefire setup looks live, but this family stays watch-only"
        else:
            reason = "setup looks live, but this family stays watch-only"
    elif watch_score >= family_policy["watch_threshold"]:
        if (
            family_policy["allow_high_upside"]
            and catalyst_hardness != "hard"
            and watch_score >= family_policy["high_upside_min_watch_score"]
            and optionality_score >= family_policy["high_upside_optionality"]
            and attention_gap >= family_policy["high_upside_min_attention_gap"]
            and trend_chase_penalty <= family_policy["high_upside_max_chase"]
            and already_priced_penalty <= family_policy["high_upside_max_already_priced"]
        ):
            verdict = "watch_high_upside"
            reason = "large repricing optionality, but catalyst still soft"
        if (
            entry_price > family_policy["max_entry_price"]
            or trend_chase_penalty > family_policy["max_chase"]
            or already_priced_penalty > family_policy["max_already_priced"]
        ):
            verdict = "watch_late"
            if action_family == "diplomacy" and catalyst_type == "call_or_meeting":
                if meeting_subtype == "talk_call":
                    reason = "talk-or-call theme looks live, but part of the move may already be gone"
                elif meeting_subtype == "resume_talks":
                    reason = "talks-resume theme looks live, but part of the move may already be gone"
                else:
                    reason = "meeting theme looks live, but part of the move may already be gone"
            elif action_family == "diplomacy" and catalyst_type == "ceasefire":
                reason = "ceasefire theme looks live, but part of the move may already be gone"
            else:
                reason = "strong catalyst, but recent move suggests part of repricing is gone"
        elif verdict != "watch_high_upside":
            verdict = "watch"
            if action_family == "release" and catalyst_type == "hostage_release":
                reason = "credible hostage-release setup, but waiting for confirmation"
            elif action_family == "diplomacy" and catalyst_type == "call_or_meeting":
                if meeting_subtype == "talk_call":
                    reason = "talk-or-call theme is interesting, but still needs confirmation"
                elif meeting_subtype == "resume_talks":
                    reason = "talks-resume theme is interesting, but still needs confirmation"
                else:
                    reason = "meeting theme is interesting, but still needs confirmation"
            elif action_family == "diplomacy" and catalyst_type == "ceasefire":
                reason = "ceasefire theme is interesting, but still needs confirmation"
            else:
                reason = "strong catalyst, but waiting for cleaner entry or confirmation"
    elif trend_chase_penalty >= 0.32:
        reason = "market already moved too much for a clean repricing entry"
    elif underreaction_score < 0.35:
        reason = "catalyst is not strong enough versus current market pricing"

    return {
        "score": score,
        "watch_score": watch_score,
        "verdict": verdict,
        "reason": reason,
        "buy_now": verdict == "buy_now",
        "watch": verdict in {"buy_now", "watch", "watch_high_upside", "watch_late"},
        "attention_gap": attention_gap,
        "stale_score": stale_score,
        "already_priced_penalty": already_priced_penalty,
        "underreaction_score": underreaction_score,
        "fresh_catalyst_score": fresh_catalyst_score,
        "trend_chase_penalty": trend_chase_penalty,
        "optionality_score": optionality_score,
        "recent_runup": repricing_context["recent_runup"],
        "recent_selloff": repricing_context["recent_selloff"],
        "compression_score": repricing_context["compression_score"],
        "deadline_pressure": repricing_context["deadline_pressure"],
        "book_quality": repricing_context["book_quality"],
        "family_policy": family_policy,
        "conflict_setup_score": conflict_setup_score,
        "conflict_urgency_score": conflict_urgency_score,
        "release_subject_score": release_subject_score,
        "release_legitimacy_score": release_legitimacy_score,
        "catalyst_type": catalyst_type,
        "meeting_subtype": meeting_subtype,
        "catalyst_strength": catalyst_strength,
        "action_family": action_family,
        "lane_key": lane_key,
        "lane_label": lane_label,
        "lane_prior": lane_prior,
        "hardness": catalyst_hardness,
        "reversibility": catalyst_reversibility,
        "has_official_source": catalyst_has_official_source,
        "market_type": market_type,
        "category_group": category_group,
    }
