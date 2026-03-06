import json
import re
import time
import unicodedata
import urllib.parse
import urllib.request
from datetime import datetime
from statistics import mean, median, pstdev

from config import (
    ODDS_API_BASE_URL,
    ODDS_API_KEY,
    ODDS_BOOKMAKERS,
    ODDS_CACHE_TTL_SECONDS,
    ODDS_MARKETS,
    ODDS_MAX_EVENT_DELTA_HOURS,
    ODDS_MIN_BOOKMAKERS,
    ODDS_MIN_MATCH_QUALITY,
    ODDS_REGIONS,
    REQUEST_TIMEOUT_SECONDS,
)


_WIN_ON_DATE_RE = re.compile(
    r"^will (?P<subject>.+?) win on (?P<date>20\d{2}-\d{2}-\d{2})\??$",
    re.IGNORECASE,
)
_TEAM_NOISE_TOKENS = {
    "fc",
    "cf",
    "sc",
    "ac",
    "afc",
    "club",
    "the",
    "women",
    "w",
    "ii",
    "iii",
}
_SPORT_KEY_BY_SLUG_PREFIX = {
    "epl": "soccer_epl",
    "fl1": "soccer_france_ligue_one",
    "bun": "soccer_germany_bundesliga",
    "bl2": "soccer_germany_bundesliga2",
    "laliga": "soccer_spain_la_liga",
    "serie": "soccer_italy_serie_a",
    "ucl": "soccer_uefa_champs_league",
}
_SPORT_KEYS_BY_TEXT = (
    (("nba",), "basketball_nba"),
    (("nfl",), "americanfootball_nfl"),
    (("nhl",), "icehockey_nhl"),
    (("mlb",), "baseball_mlb"),
    (("premier league", "epl"), "soccer_epl"),
    (("ligue 1", "ligue one"), "soccer_france_ligue_one"),
    (("bundesliga 2", "2. bundesliga", "bl2"), "soccer_germany_bundesliga2"),
    (("bundesliga", "bun"), "soccer_germany_bundesliga"),
    (("la liga",), "soccer_spain_la_liga"),
    (("serie a",), "soccer_italy_serie_a"),
)
_CACHE = {}


def _normalize_text(*parts):
    text = " ".join(str(part or "") for part in parts).strip().lower()
    return " ".join(text.split())


def _strip_accents(text):
    normalized = unicodedata.normalize("NFKD", text or "")
    return normalized.encode("ascii", "ignore").decode("ascii")


def _canonical_team(text):
    lowered = _strip_accents(_normalize_text(text))
    cleaned = re.sub(r"[^a-z0-9]+", " ", lowered)
    tokens = [token for token in cleaned.split() if token and token not in _TEAM_NOISE_TOKENS]
    if tokens and tokens[0] == "1":
        tokens = tokens[1:]
    return " ".join(tokens), set(tokens)


def _slug_prefix(market):
    slug = _normalize_text(market.get("event_slug") or market.get("slug"))
    return slug.split("-", 1)[0] if slug else ""


def _parse_subject(question_text):
    match = _WIN_ON_DATE_RE.match(question_text)
    if not match:
        return None, None
    return match.group("subject").strip(), match.group("date")


def _parse_iso_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _infer_sport_key(market):
    prefix = _slug_prefix(market)
    if prefix in _SPORT_KEY_BY_SLUG_PREFIX:
        return _SPORT_KEY_BY_SLUG_PREFIX[prefix]

    text = _normalize_text(
        market.get("event_category"),
        market.get("event_title"),
        market.get("question"),
    )
    for keywords, sport_key in _SPORT_KEYS_BY_TEXT:
        if any(keyword in text for keyword in keywords):
            return sport_key
    return None


def _cached_json(url, params):
    query = urllib.parse.urlencode(params)
    cache_key = f"{url}?{query}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached["ts"]) <= ODDS_CACHE_TTL_SECONDS:
        return cached["data"]

    req = urllib.request.Request(
        cache_key,
        headers={"User-Agent": "Mozilla/5.0 (compatible; edge-bot/2.0)"},
    )
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    _CACHE[cache_key] = {"ts": now, "data": payload}
    return payload


def _fetch_sport_events(sport_key):
    if not ODDS_API_KEY:
        return []

    params = {
        "apiKey": ODDS_API_KEY,
        "markets": ODDS_MARKETS,
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    if ODDS_BOOKMAKERS:
        params["bookmakers"] = ODDS_BOOKMAKERS
    else:
        params["regions"] = ODDS_REGIONS

    url = f"{ODDS_API_BASE_URL}/sports/{sport_key}/odds"
    data = _cached_json(url, params)
    return data if isinstance(data, list) else []


def _team_match_score(subject_team, candidate_team):
    subject_name, subject_tokens = _canonical_team(subject_team)
    candidate_name, candidate_tokens = _canonical_team(candidate_team)
    if not subject_name or not candidate_name:
        return 0.0
    if subject_name == candidate_name:
        return 1.0
    if subject_name in candidate_name or candidate_name in subject_name:
        return 0.93
    overlap = len(subject_tokens & candidate_tokens)
    if overlap <= 0:
        return 0.0
    coverage = overlap / max(1, len(subject_tokens))
    precision = overlap / max(1, len(candidate_tokens))
    return min(0.90, (coverage * 0.65) + (precision * 0.25))


def _event_match_score(event, subject_team, target_date):
    home_team = event.get("home_team")
    away_team = event.get("away_team")
    team_score = max(
        _team_match_score(subject_team, home_team),
        _team_match_score(subject_team, away_team),
    )
    if team_score <= 0:
        return 0.0

    commence = _parse_iso_datetime(event.get("commence_time"))
    if not commence:
        return team_score * 0.75

    target_dt = _parse_iso_datetime(f"{target_date}T12:00:00+00:00")
    if not target_dt:
        return team_score

    delta_hours = abs((commence - target_dt).total_seconds()) / 3600.0
    if delta_hours > ODDS_MAX_EVENT_DELTA_HOURS:
        return 0.0

    time_score = max(0.0, 1.0 - (delta_hours / max(1.0, ODDS_MAX_EVENT_DELTA_HOURS)))
    return min(1.0, (team_score * 0.78) + (time_score * 0.22))


def _pick_event(events, subject_team, target_date):
    best_event = None
    best_score = 0.0
    for event in events:
        if not isinstance(event, dict):
            continue
        score = _event_match_score(event, subject_team, target_date)
        if score > best_score:
            best_score = score
            best_event = event
    return best_event, best_score


def _normalize_bookmaker_probabilities(outcomes):
    implied = []
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            continue
        price = outcome.get("price")
        try:
            price = float(price)
        except (TypeError, ValueError):
            continue
        if price <= 1.0:
            continue
        implied.append((outcome.get("name"), 1.0 / price))
    total = sum(probability for _, probability in implied)
    if total <= 0:
        return {}
    normalized = {}
    for name, probability in implied:
        normalized[str(name or "")] = probability / total
    return normalized


def _extract_market_probabilities(event, subject_team):
    probabilities = []
    bookmaker_names = []
    for bookmaker in event.get("bookmakers", []):
        if not isinstance(bookmaker, dict):
            continue
        bookmaker_name = bookmaker.get("title") or bookmaker.get("key") or "unknown"
        for market in bookmaker.get("markets", []):
            if not isinstance(market, dict) or market.get("key") != "h2h":
                continue
            normalized = _normalize_bookmaker_probabilities(market.get("outcomes", []))
            if not normalized:
                continue

            best_name = None
            best_score = 0.0
            for outcome_name in normalized:
                score = _team_match_score(subject_team, outcome_name)
                if score > best_score:
                    best_score = score
                    best_name = outcome_name

            if best_name is None or best_score < 0.72:
                continue

            probabilities.append(normalized[best_name])
            bookmaker_names.append(bookmaker_name)
            break
    return probabilities, bookmaker_names


def _build_odds_prior(market, subject_team, target_date):
    if market.get("closed"):
        return None

    sport_key = _infer_sport_key(market)
    if not sport_key:
        return None

    events = _fetch_sport_events(sport_key)
    if not events:
        return None

    event, match_quality = _pick_event(events, subject_team, target_date)
    if event is None or match_quality < ODDS_MIN_MATCH_QUALITY:
        return None

    probabilities, bookmaker_names = _extract_market_probabilities(event, subject_team)
    if len(probabilities) < ODDS_MIN_BOOKMAKERS:
        return None

    implied_probability = median(probabilities)
    dispersion = pstdev(probabilities) if len(probabilities) > 1 else 0.0
    price = float(market.get("ref_price") or 0.5)
    price_gap = implied_probability - price
    line_confirmation = min(1.0, len(probabilities) / max(1.0, float(ODDS_MIN_BOOKMAKERS + 2)))
    line_confirmation *= max(0.0, 1.0 - min(1.0, dispersion * 18.0))

    signal = 0.5 + (price_gap * 1.90) + ((implied_probability - 0.5) * 0.18)
    confidence = 0.60
    confidence += min(0.18, len(probabilities) * 0.035)
    confidence += match_quality * 0.10
    confidence += line_confirmation * 0.10
    confidence -= min(0.16, dispersion * 2.5)

    return {
        "name": "sports_odds_feed",
        "signal": max(0.0, min(signal, 1.0)),
        "confidence": max(0.0, min(confidence, 1.0)),
        "components": {
            "provider": "the_odds_api",
            "sport_key": sport_key,
            "subject_team": subject_team,
            "target_date": target_date,
            "home_team": event.get("home_team"),
            "away_team": event.get("away_team"),
            "commence_time": event.get("commence_time"),
            "implied_probability": implied_probability,
            "mean_probability": mean(probabilities),
            "probability_dispersion": dispersion,
            "bookmaker_count": len(probabilities),
            "bookmakers": bookmaker_names[:8],
            "price_gap": price_gap,
            "match_quality": match_quality,
            "line_confirmation": line_confirmation,
            "structural_penalty": 0.0,
        },
    }


def get_market_odds_prior(market):
    if not ODDS_API_KEY:
        return None

    question_text = _normalize_text(market.get("question"))
    subject_team, target_date = _parse_subject(question_text)
    if not subject_team or not target_date:
        return None

    try:
        return _build_odds_prior(market, subject_team, target_date)
    except Exception:
        return None
