from config import (
    MAX_CALL_MEETING_WATCHLIST,
    MAX_CEASEFIRE_WATCHLIST,
    MAX_CONFLICT_LEADERBOARD,
    MAX_GEOPOLITICAL_RADAR,
    MAX_HOSTAGE_NEGOTIATION_WATCHLIST,
    MAX_LEGAL_CATALYST_LEADERS,
    MAX_RELEASE_BUY_NOW,
    MAX_RELEASE_WATCHLIST,
    MAX_RESUME_TALKS_WATCHLIST,
    MAX_TALK_CALL_WATCHLIST,
    MAX_WATCHLIST,
    MIN_GEOPOLITICAL_REPRICING,
)


def _is_geopolitical_radar_candidate(candidate):
    repricing_verdict = str(candidate.get("repricing_verdict") or "")
    return (
        candidate.get("domain_name") == "geopolitical_repricing"
        and (
            repricing_verdict in {"buy_now", "watch", "watch_high_upside", "watch_late"}
            or (candidate.get("repricing_potential") or 0.0) >= MIN_GEOPOLITICAL_REPRICING
        )
    )


def _consistency_sort_prefix(candidate):
    residual = candidate.get("consistency_residual")
    if residual is None:
        residual = 0.0
    next_buyer_edge = candidate.get("next_buyer_edge")
    if next_buyer_edge is None:
        next_buyer_edge = 0.0
    next_buyer_score = candidate.get("next_buyer_score")
    if next_buyer_score is None:
        next_buyer_score = 0.0
    return (
        -(1 if candidate.get("consistency_selected") else 0),
        -max(residual, 0.0),
        -(1 if candidate.get("next_buyer_selected") else 0),
        -max(next_buyer_edge, 0.0),
        -max(next_buyer_score, 0.0),
        -(1 if candidate.get("thesis_surface_selected", True) else 0),
        -(candidate.get("thesis_surface_score") or 0.0),
    )


def _build_geopolitical_radar(value_bets, watchlist, rejected_candidates, excluded_links):
    merged = []
    seen = set()
    for source_name, rows in (
        ("value", value_bets),
        ("watch", watchlist),
        ("rejected", rejected_candidates),
    ):
        for row in rows:
            link = row.get("link")
            if not link or link in seen or link in excluded_links:
                continue
            if not _is_geopolitical_radar_candidate(row):
                continue
            item = dict(row)
            item["radar_source"] = source_name
            merged.append(item)
            seen.add(link)

    merged.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(x.get("repricing_score") or 0.0),
            -(x.get("repricing_watch_score") or 0.0),
            -(x.get("repricing_potential") or 0.0),
            -(x.get("confidence") or 0.0),
            -(x.get("net_edge") or -999.0),
            x.get("diagnostic_shortfall") or 0.0,
        )
    )
    return merged[:MAX_GEOPOLITICAL_RADAR]


def _build_conflict_leaderboard(value_bets, watchlist, rejected_candidates):
    conflict_rows = []
    seen = set()
    for source_name, rows in (
        ("value", value_bets),
        ("watch", watchlist),
        ("rejected", rejected_candidates),
    ):
        for row in rows:
            link = row.get("link")
            if not link or link in seen:
                continue
            if str(row.get("domain_action_family") or "") != "conflict":
                continue
            item = dict(row)
            item["radar_source"] = source_name
            conflict_rows.append(item)
            seen.add(link)
    conflict_rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(x.get("repricing_score") or 0.0),
            -(x.get("repricing_conflict_setup_score") or 0.0),
            -(x.get("repricing_conflict_urgency_score") or 0.0),
            -(x.get("repricing_attention_gap") or 0.0),
            -(x.get("confidence") or 0.0),
        )
    )
    return conflict_rows[:MAX_CONFLICT_LEADERBOARD]


def _build_legal_catalyst_leaders(value_bets, watchlist, rejected_candidates):
    legal_rows = []
    seen = set()
    legal_catalysts = {"hearing", "court_ruling", "appeal"}
    for source_name, rows in (
        ("value", value_bets),
        ("watch", watchlist),
        ("rejected", rejected_candidates),
    ):
        for row in rows:
            link = row.get("link")
            if not link or link in seen:
                continue
            if str(row.get("domain_action_family") or "") != "release":
                continue
            if str(row.get("catalyst_type") or "") not in legal_catalysts:
                continue
            verdict = str(row.get("repricing_verdict") or "")
            if verdict not in {"buy_now", "watch", "watch_high_upside", "watch_late"}:
                continue
            item = dict(row)
            item["radar_source"] = source_name
            legal_rows.append(item)
            seen.add(link)
    legal_rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(x.get("repricing_score") or 0.0),
            -(x.get("repricing_release_legitimacy_score") or 0.0),
            -(x.get("repricing_release_subject_score") or 0.0),
            -(x.get("confidence") or 0.0),
        )
    )
    return legal_rows[:MAX_LEGAL_CATALYST_LEADERS]


def _build_release_watchlist(value_bets, watchlist, rejected_candidates):
    release_rows = []
    seen = set()
    for source_name, rows in (
        ("value", value_bets),
        ("watch", watchlist),
        ("rejected", rejected_candidates),
    ):
        for row in rows:
            link = row.get("link")
            if not link or link in seen:
                continue
            if str(row.get("domain_action_family") or "") != "release":
                continue
            verdict = str(row.get("repricing_verdict") or "")
            if verdict not in {"watch", "watch_high_upside", "watch_late"}:
                continue
            item = dict(row)
            item["radar_source"] = source_name
            release_rows.append(item)
            seen.add(link)
    release_rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(x.get("repricing_score") or 0.0),
            -(x.get("repricing_release_subject_score") or 0.0),
            -(x.get("repricing_release_legitimacy_score") or 0.0),
            -(x.get("repricing_attention_gap") or 0.0),
            -(x.get("confidence") or 0.0),
        )
    )
    return release_rows[:MAX_RELEASE_WATCHLIST]


def _build_ceasefire_watchlist(value_bets, watchlist, rejected_candidates):
    rows = []
    seen = set()
    for source_name, candidates in (
        ("value", value_bets),
        ("watch", watchlist),
        ("rejected", rejected_candidates),
    ):
        for row in candidates:
            link = row.get("link")
            if not link or link in seen:
                continue
            verdict = str(row.get("repricing_verdict") or "")
            if verdict not in {"watch", "watch_high_upside", "watch_late"}:
                continue
            if str(row.get("domain_action_family") or "") != "diplomacy":
                continue
            if str(row.get("catalyst_type") or "") != "ceasefire":
                continue
            item = dict(row)
            item["radar_source"] = source_name
            rows.append(item)
            seen.add(link)

    rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(x.get("repricing_watch_score") or 0.0),
            -(x.get("repricing_score") or 0.0),
            -(x.get("repricing_optionality_score") or 0.0),
            -(x.get("repricing_attention_gap") or 0.0),
            -(x.get("confidence") or 0.0),
        )
    )
    return rows[:MAX_CEASEFIRE_WATCHLIST]


def _build_talk_call_watchlist(value_bets, watchlist, rejected_candidates):
    rows = []
    seen = set()
    for source_name, candidates in (
        ("value", value_bets),
        ("watch", watchlist),
        ("rejected", rejected_candidates),
    ):
        for row in candidates:
            link = row.get("link")
            if not link or link in seen:
                continue
            verdict = str(row.get("repricing_verdict") or "")
            if verdict not in {"watch", "watch_high_upside", "watch_late"}:
                continue
            if str(row.get("domain_action_family") or "") != "diplomacy":
                continue
            if str(row.get("catalyst_type") or "") != "call_or_meeting":
                continue
            if str(row.get("meeting_subtype") or "") != "talk_call":
                continue
            item = dict(row)
            item["radar_source"] = source_name
            rows.append(item)
            seen.add(link)

    rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(x.get("repricing_watch_score") or 0.0),
            -(x.get("repricing_optionality_score") or 0.0),
            -(x.get("repricing_attention_gap") or 0.0),
            -(x.get("repricing_score") or 0.0),
            -(x.get("confidence") or 0.0),
        )
    )
    return rows[:MAX_TALK_CALL_WATCHLIST]


def _collect_call_meeting_candidates(value_bets, watchlist, rejected_candidates):
    rows = []
    seen = set()
    for source_name, candidates in (
        ("value", value_bets),
        ("watch", watchlist),
        ("rejected", rejected_candidates),
    ):
        for row in candidates:
            link = row.get("link")
            if not link or link in seen:
                continue
            verdict = str(row.get("repricing_verdict") or "")
            if verdict not in {"watch", "watch_high_upside", "watch_late"}:
                continue
            if str(row.get("domain_action_family") or "") != "diplomacy":
                continue
            if str(row.get("catalyst_type") or "") != "call_or_meeting":
                continue
            if str(row.get("meeting_subtype") or "") == "talk_call":
                continue
            item = dict(row)
            item["radar_source"] = source_name
            rows.append(item)
            seen.add(link)

    return rows


def _build_meeting_watchlist(value_bets, watchlist, rejected_candidates):
    rows = [
        row
        for row in _collect_call_meeting_candidates(value_bets, watchlist, rejected_candidates)
        if str(row.get("meeting_subtype") or "") in {"meeting", "meeting_generic"}
    ]

    rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(x.get("repricing_watch_score") or 0.0),
            -(x.get("repricing_attention_gap") or 0.0),
            -(x.get("repricing_optionality_score") or 0.0),
            -(x.get("repricing_score") or 0.0),
            (x.get("repricing_already_priced_penalty") or 0.0),
            -(x.get("confidence") or 0.0),
        )
    )
    return rows[:MAX_CALL_MEETING_WATCHLIST]


def _build_resume_talks_watchlist(value_bets, watchlist, rejected_candidates):
    rows = [
        row
        for row in _collect_call_meeting_candidates(value_bets, watchlist, rejected_candidates)
        if str(row.get("meeting_subtype") or "") == "resume_talks"
    ]

    rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(x.get("repricing_watch_score") or 0.0),
            -(x.get("repricing_optionality_score") or 0.0),
            -(x.get("repricing_attention_gap") or 0.0),
            -(x.get("repricing_score") or 0.0),
            -(x.get("confidence") or 0.0),
        )
    )
    return rows[:MAX_RESUME_TALKS_WATCHLIST]


def _build_hostage_negotiation_watchlist(value_bets, watchlist, rejected_candidates):
    rows = []
    seen = set()
    negotiation_catalysts = {"negotiation", "summit"}
    for source_name, candidates in (
        ("value", value_bets),
        ("watch", watchlist),
        ("rejected", rejected_candidates),
    ):
        for row in candidates:
            link = row.get("link")
            if not link or link in seen:
                continue
            verdict = str(row.get("repricing_verdict") or "")
            if verdict not in {"watch", "watch_high_upside", "watch_late"}:
                continue

            action_family = str(row.get("domain_action_family") or "")
            catalyst_type = str(row.get("catalyst_type") or "")
            is_hostage = action_family == "release" and catalyst_type == "hostage_release"
            is_negotiation = action_family == "diplomacy" and catalyst_type in negotiation_catalysts
            if not (is_hostage or is_negotiation):
                continue

            item = dict(row)
            item["radar_source"] = source_name
            rows.append(item)
            seen.add(link)

    rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(x.get("repricing_watch_score") or 0.0),
            -(x.get("repricing_score") or 0.0),
            -(x.get("repricing_optionality_score") or 0.0),
            -(x.get("repricing_attention_gap") or 0.0),
            -(x.get("confidence") or 0.0),
        )
    )
    return rows[:MAX_HOSTAGE_NEGOTIATION_WATCHLIST]


def _build_release_buy_now(value_bets, watchlist, rejected_candidates):
    release_rows = []
    seen = set()
    for source_name, rows in (
        ("value", value_bets),
        ("watch", watchlist),
        ("rejected", rejected_candidates),
    ):
        for row in rows:
            link = row.get("link")
            if not link or link in seen:
                continue
            if str(row.get("domain_action_family") or "") != "release":
                continue
            if str(row.get("repricing_verdict") or "") != "buy_now":
                continue
            item = dict(row)
            item["radar_source"] = source_name
            release_rows.append(item)
            seen.add(link)
    release_rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(x.get("repricing_score") or 0.0),
            -(x.get("repricing_release_legitimacy_score") or 0.0),
            -(x.get("repricing_release_subject_score") or 0.0),
            -(x.get("confidence") or 0.0),
        )
    )
    return release_rows[:MAX_RELEASE_BUY_NOW]


def _build_best_watchlist(*candidate_groups):
    rows = []
    seen = set()
    for group in candidate_groups:
        for row in group or []:
            link = row.get("link")
            if not link or link in seen:
                continue
            verdict = str(row.get("repricing_verdict") or "")
            if verdict not in {"watch", "watch_high_upside", "watch_late"}:
                continue
            rows.append(dict(row))
            seen.add(link)

    rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(1 if str(x.get("repricing_verdict") or "") == "watch_high_upside" else 0),
            -(x.get("repricing_watch_score") or 0.0),
            -(x.get("repricing_score") or 0.0),
            -(x.get("repricing_lane_prior") or 0.0),
            -(x.get("confidence") or 0.0),
            -(x.get("net_edge") or -999.0),
        )
    )
    return rows[:MAX_WATCHLIST]


def _build_paper_scout_candidates(*candidate_groups):
    rows = []
    seen = set()
    for group in candidate_groups:
        for row in group or []:
            link = row.get("link")
            if not link or link in seen:
                continue
            verdict = str(row.get("repricing_verdict") or "")
            if verdict not in {"buy_now", "watch", "watch_high_upside"}:
                continue
            rows.append(dict(row))
            seen.add(link)

    rows.sort(
        key=lambda x: (
            *_consistency_sort_prefix(x),
            -(1 if str(x.get("repricing_verdict") or "") == "buy_now" else 0),
            -(1 if str(x.get("repricing_verdict") or "") == "watch_high_upside" else 0),
            -(x.get("repricing_watch_score") or 0.0),
            -(x.get("repricing_score") or 0.0),
            -(x.get("repricing_lane_prior") or 0.0),
            -(x.get("repricing_attention_gap") or 0.0),
            -(x.get("confidence") or 0.0),
        )
    )
    return rows


def _build_compact_radar(geopolitical_radar, excluded_links):
    rows = []
    for candidate in geopolitical_radar:
        link = candidate.get("link")
        if not link or link in excluded_links:
            continue
        rows.append(candidate)
    return rows[:MAX_GEOPOLITICAL_RADAR]


def build_report_sections(value_bets, watchlist, rejected_candidates):
    displayed_links = {candidate.get("link") for candidate in value_bets if candidate.get("link")}
    geopolitical_radar = _build_geopolitical_radar(value_bets, watchlist, rejected_candidates, displayed_links)
    conflict_leaderboard = _build_conflict_leaderboard(value_bets, watchlist, rejected_candidates)
    legal_catalyst_leaders = _build_legal_catalyst_leaders(value_bets, watchlist, rejected_candidates)
    legal_links = {candidate.get("link") for candidate in legal_catalyst_leaders if candidate.get("link")}

    release_buy_now = [
        candidate
        for candidate in _build_release_buy_now(value_bets, watchlist, rejected_candidates)
        if candidate.get("link") not in legal_links
    ]
    release_buy_links = {candidate.get("link") for candidate in release_buy_now if candidate.get("link")}

    release_watchlist = [
        candidate
        for candidate in _build_release_watchlist(value_bets, watchlist, rejected_candidates)
        if candidate.get("link") not in legal_links
    ]
    release_links = {candidate.get("link") for candidate in release_watchlist if candidate.get("link")}

    ceasefire_watchlist = [
        candidate
        for candidate in _build_ceasefire_watchlist(value_bets, watchlist, rejected_candidates)
        if candidate.get("link") not in legal_links
        and candidate.get("link") not in release_buy_links
        and candidate.get("link") not in release_links
    ]
    ceasefire_links = {candidate.get("link") for candidate in ceasefire_watchlist if candidate.get("link")}

    talk_call_watchlist = [
        candidate
        for candidate in _build_talk_call_watchlist(value_bets, watchlist, rejected_candidates)
        if candidate.get("link") not in legal_links
        and candidate.get("link") not in release_buy_links
        and candidate.get("link") not in release_links
        and candidate.get("link") not in ceasefire_links
    ]
    talk_call_links = {candidate.get("link") for candidate in talk_call_watchlist if candidate.get("link")}

    meeting_watchlist = [
        candidate
        for candidate in _build_meeting_watchlist(value_bets, watchlist, rejected_candidates)
        if candidate.get("link") not in legal_links
        and candidate.get("link") not in release_buy_links
        and candidate.get("link") not in release_links
        and candidate.get("link") not in ceasefire_links
        and candidate.get("link") not in talk_call_links
    ]
    meeting_links = {candidate.get("link") for candidate in meeting_watchlist if candidate.get("link")}

    resume_talks_watchlist = [
        candidate
        for candidate in _build_resume_talks_watchlist(value_bets, watchlist, rejected_candidates)
        if candidate.get("link") not in legal_links
        and candidate.get("link") not in release_buy_links
        and candidate.get("link") not in release_links
        and candidate.get("link") not in ceasefire_links
        and candidate.get("link") not in talk_call_links
        and candidate.get("link") not in meeting_links
    ]
    call_meeting_watchlist = meeting_watchlist + resume_talks_watchlist
    call_meeting_links = {candidate.get("link") for candidate in call_meeting_watchlist if candidate.get("link")}

    hostage_negotiation_watchlist = [
        candidate
        for candidate in _build_hostage_negotiation_watchlist(value_bets, watchlist, rejected_candidates)
        if candidate.get("link") not in legal_links
        and candidate.get("link") not in release_buy_links
        and candidate.get("link") not in release_links
        and candidate.get("link") not in ceasefire_links
        and candidate.get("link") not in talk_call_links
        and candidate.get("link") not in call_meeting_links
    ]

    best_watchlist = _build_best_watchlist(
        watchlist,
        conflict_leaderboard,
        legal_catalyst_leaders,
        release_watchlist,
        ceasefire_watchlist,
        talk_call_watchlist,
        meeting_watchlist,
        resume_talks_watchlist,
        hostage_negotiation_watchlist,
        geopolitical_radar,
    )
    paper_scout_candidates = _build_paper_scout_candidates(
        watchlist,
        conflict_leaderboard,
        legal_catalyst_leaders,
        release_buy_now,
        release_watchlist,
        ceasefire_watchlist,
        talk_call_watchlist,
        meeting_watchlist,
        resume_talks_watchlist,
        hostage_negotiation_watchlist,
        geopolitical_radar,
    )

    displayed_buy_links = {candidate.get("link") for candidate in value_bets if candidate.get("link")}
    displayed_buy_links.update(candidate.get("link") for candidate in release_buy_now if candidate.get("link"))
    displayed_watch_links = {candidate.get("link") for candidate in best_watchlist if candidate.get("link")}
    geopolitical_radar_core = _build_compact_radar(
        geopolitical_radar,
        displayed_buy_links | displayed_watch_links,
    )

    return {
        "geopolitical_radar": geopolitical_radar,
        "geopolitical_radar_core": geopolitical_radar_core,
        "conflict_leaderboard": conflict_leaderboard,
        "legal_catalyst_leaders": legal_catalyst_leaders,
        "release_buy_now": release_buy_now,
        "release_watchlist": release_watchlist,
        "ceasefire_watchlist": ceasefire_watchlist,
        "talk_call_watchlist": talk_call_watchlist,
        "meeting_watchlist": meeting_watchlist,
        "resume_talks_watchlist": resume_talks_watchlist,
        "call_meeting_watchlist": call_meeting_watchlist,
        "hostage_negotiation_watchlist": hostage_negotiation_watchlist,
        "best_watchlist": best_watchlist,
        "paper_scout_candidates": paper_scout_candidates,
    }
