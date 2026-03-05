import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct

from bot.config import Settings


@dataclass(slots=True)
class LiveSubmitResult:
    accepted: bool
    client_order_id: str
    exchange_order_id: str | None
    status: str
    filled_size: float
    avg_fill_price: float
    fee_paid: float
    reason: str


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _as_path(endpoint: str) -> str:
    return endpoint if endpoint.startswith("/") else f"/{endpoint}"


class ClobLiveClient:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._base_url = settings.polymarket_clob_base_url.rstrip("/")
        self._order_endpoint = _as_path(settings.live_order_endpoint)
        self._cancel_endpoint = _as_path(settings.live_cancel_endpoint)
        self._open_orders_endpoint = _as_path(settings.live_open_orders_endpoint)
        self._fills_endpoint = _as_path(settings.live_fills_endpoint)
        self._timeout = max(5, settings.ingest_timeout_seconds)
        self._private_key = settings.live_private_key.strip()
        self._api_key = settings.live_auth_api_key.strip()
        self._secret = settings.live_auth_secret.strip()
        self._passphrase = settings.live_auth_passphrase.strip()

    def _auth_headers(self, method: str, path: str, body: str) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if not self._api_key or not self._secret:
            return headers

        timestamp = str(int(datetime.now(UTC).timestamp()))
        prehash = f"{timestamp}{method.upper()}{path}{body}"
        signature = hmac.new(self._secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).hexdigest()
        headers.update(
            {
                "POLY_API_KEY": self._api_key,
                "POLY_SIGNATURE": signature,
                "POLY_TIMESTAMP": timestamp,
            }
        )
        if self._passphrase:
            headers["POLY_PASSPHRASE"] = self._passphrase
        return headers

    def _signed_order_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self._private_key:
            return payload
        canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        message = encode_defunct(text=canonical)
        signed = Account.sign_message(message, private_key=self._private_key)
        extended = dict(payload)
        extended["signature"] = signed.signature.hex()
        extended["signatureType"] = self._settings.live_signing_scheme
        extended["chainId"] = self._settings.live_chain_id
        return extended

    def submit_order(
        self,
        client_order_id: str,
        market_id: str,
        outcome_id: str,
        side: str,
        order_type: str,
        price: float,
        size: float,
    ) -> LiveSubmitResult:
        payload = {
            "client_order_id": client_order_id,
            "market_id": market_id,
            "token_id": outcome_id,
            "side": side,
            "order_type": order_type,
            "price": price,
            "size": size,
            "time_in_force": "GTC",
        }
        signed_payload = self._signed_order_payload(payload)
        body = json.dumps(signed_payload, separators=(",", ":"))
        headers = self._auth_headers("POST", self._order_endpoint, body)
        url = f"{self._base_url}{self._order_endpoint}"

        try:
            with httpx.Client(timeout=self._timeout, headers={"User-Agent": "polymarket-bot-live/0.1"}) as client:
                response = client.post(url, content=body, headers=headers)
            if response.status_code >= 400:
                return LiveSubmitResult(
                    accepted=False,
                    client_order_id=client_order_id,
                    exchange_order_id=None,
                    status="rejected",
                    filled_size=0.0,
                    avg_fill_price=0.0,
                    fee_paid=0.0,
                    reason=f"http_{response.status_code}",
                )
            data = response.json() if response.content else {}
            exchange_order_id = str(
                data.get("id")
                or data.get("orderID")
                or data.get("order_id")
                or data.get("orderId")
                or ""
            ) or None
            status = str(data.get("status") or data.get("state") or "open")
            return LiveSubmitResult(
                accepted=True,
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                status=status,
                filled_size=_safe_float(data.get("filled_size") or data.get("matchedSize")),
                avg_fill_price=_safe_float(data.get("avg_price") or data.get("avgPrice")),
                fee_paid=_safe_float(data.get("fee") or data.get("fees")),
                reason="accepted",
            )
        except Exception as exc:
            return LiveSubmitResult(
                accepted=False,
                client_order_id=client_order_id,
                exchange_order_id=None,
                status="error",
                filled_size=0.0,
                avg_fill_price=0.0,
                fee_paid=0.0,
                reason=str(exc),
            )

    def cancel_orders(self, client_order_ids: list[str]) -> bool:
        if not client_order_ids:
            return True
        payload = {"client_order_ids": client_order_ids}
        body = json.dumps(payload, separators=(",", ":"))
        headers = self._auth_headers("POST", self._cancel_endpoint, body)
        url = f"{self._base_url}{self._cancel_endpoint}"

        try:
            with httpx.Client(timeout=self._timeout, headers={"User-Agent": "polymarket-bot-live/0.1"}) as client:
                response = client.post(url, content=body, headers=headers)
            return response.status_code < 400
        except Exception:
            return False

    def fetch_open_order_ids(self) -> tuple[bool, set[str]]:
        path = self._open_orders_endpoint
        headers = self._auth_headers("GET", path, "")
        url = f"{self._base_url}{path}"
        try:
            with httpx.Client(timeout=self._timeout, headers={"User-Agent": "polymarket-bot-live/0.1"}) as client:
                response = client.get(url, headers=headers, params={"status": "open"})
            if response.status_code >= 400:
                return False, set()
            raw = response.json() if response.content else []
            orders = raw if isinstance(raw, list) else raw.get("data", [])
            result: set[str] = set()
            if isinstance(orders, list):
                for order in orders:
                    if not isinstance(order, dict):
                        continue
                    for key in ("client_order_id", "clientOrderId", "id", "order_id", "orderId"):
                        value = order.get(key)
                        if value:
                            result.add(str(value))
            return True, result
        except Exception:
            return False, set()

    def fetch_recent_fills(self, lookback_limit: int = 300) -> tuple[bool, dict[str, dict[str, float]]]:
        path = self._fills_endpoint
        headers = self._auth_headers("GET", path, "")
        url = f"{self._base_url}{path}"
        try:
            with httpx.Client(timeout=self._timeout, headers={"User-Agent": "polymarket-bot-live/0.1"}) as client:
                response = client.get(url, headers=headers, params={"limit": lookback_limit})
            if response.status_code >= 400:
                return False, {}
            raw = response.json() if response.content else []
            fills = raw if isinstance(raw, list) else raw.get("data", [])
            parsed: dict[str, dict[str, float]] = {}
            if isinstance(fills, list):
                for fill in fills:
                    if not isinstance(fill, dict):
                        continue
                    client_id = str(
                        fill.get("client_order_id")
                        or fill.get("clientOrderId")
                        or fill.get("order_id")
                        or fill.get("orderId")
                        or ""
                    )
                    if not client_id:
                        continue
                    parsed[client_id] = {
                        "size": _safe_float(fill.get("size") or fill.get("filled_size")),
                        "price": _safe_float(fill.get("price") or fill.get("avg_price")),
                        "fee": _safe_float(fill.get("fee") or fill.get("fees")),
                    }
            return True, parsed
        except Exception:
            return False, {}
