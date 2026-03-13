import json
import time
import urllib.error
import urllib.request


class HTTPClientError(RuntimeError):
    def __init__(self, message, *, status_code=None, url=""):
        super().__init__(message)
        self.status_code = status_code
        self.url = url


def fetch_json(
    url,
    *,
    timeout_seconds=12.0,
    retries=2,
    backoff_seconds=0.8,
    user_agent="Mozilla/5.0 (compatible; edge-bot/2.0)",
):
    headers = {"User-Agent": user_agent}
    last_error = None

    for attempt in range(retries + 1):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                payload = resp.read().decode("utf-8")
                return json.loads(payload)
        except urllib.error.HTTPError as exc:
            last_error = HTTPClientError(
                f"HTTP {exc.code}: {exc.reason}",
                status_code=exc.code,
                url=url,
            )
            if 400 <= exc.code < 500:
                raise last_error from exc
        except urllib.error.URLError as exc:
            last_error = HTTPClientError(f"URL error: {exc.reason}", url=url)
        except json.JSONDecodeError as exc:
            raise HTTPClientError("Invalid JSON response", url=url) from exc
        except TimeoutError as exc:
            last_error = HTTPClientError("Request timeout", url=url)
        except Exception as exc:  # noqa: BLE001
            last_error = HTTPClientError(f"Request failed: {exc}", url=url)

        if attempt < retries:
            time.sleep(backoff_seconds * (2**attempt))

    raise HTTPClientError(f"All retries failed: {last_error}", url=url) from last_error
