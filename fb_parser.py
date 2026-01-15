import os
import time
import json
import logging
from datetime import date, datetime
from typing import List, Dict, Any, Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - fb_parser - %(levelname)s - %(message)s",
)
logger = logging.getLogger("fb_parser")


# -----------------------------
# Config / ENV
# -----------------------------
API_BASE_URL = (os.getenv("API_BASE_URL") or "").rstrip("/")
if not API_BASE_URL:
    raise RuntimeError("API_BASE_URL is not set")


def _get_api_secret() -> str:
    return (
        os.getenv("API_SECRET")
        or os.getenv("MINIAPP_API_SECRET")
        or os.getenv("X_API_KEY")
        or os.getenv("PARSER_API_SECRET")
        or ""
    ).strip()


API_SECRET = _get_api_secret()

# ✅ по умолчанию берём единый список источников /api/groups
FB_GROUPS_API_URL = os.getenv("FB_GROUPS_API_URL") or f"{API_BASE_URL}/api/groups"


def _normalize_apify_token(token: Optional[str]) -> str:
    token = (token or "").strip()
    if not token:
        return ""
    if ("http://" in token or "https://" in token) and "token=" in token:
        token = token.split("token=", 1)[1].split("&", 1)[0].strip()
    return token


APIFY_TOKEN = _normalize_apify_token(os.getenv("APIFY_TOKEN"))
APIFY_ACTOR_ID = os.getenv("APIFY_ACTOR_ID", "AtBpiepuIUNs2k2ku")
if not APIFY_TOKEN:
    raise RuntimeError("APIFY_TOKEN is not set")

# Actor settings
APIFY_MIN_DELAY = int(os.getenv("APIFY_MIN_DELAY", "1"))
APIFY_MAX_DELAY = int(os.getenv("APIFY_MAX_DELAY", "10"))
APIFY_COUNT = int(os.getenv("APIFY_COUNT", "30"))
APIFY_SORT_TYPE = (os.getenv("APIFY_SORT_TYPE") or "new_posts").strip()
APIFY_TIMEOUT_SECONDS = int(os.getenv("APIFY_TIMEOUT_SECONDS", "600"))

# Optional: ограничение по дате (если пусто — не отправляем поле вообще)
APIFY_SCRAPE_UNTIL = (os.getenv("APIFY_SCRAPE_UNTIL") or "").strip()

# Optional proxy country
APIFY_PROXY_COUNTRY = (os.getenv("APIFY_PROXY_COUNTRY") or "").strip()

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "600"))

# cookies can be set either via miniapp secret or env
FB_COOKIES_JSON = os.getenv("FB_COOKIES_JSON", "[]")

# enable/disable parser from env
FB_PARSER_DISABLED = (os.getenv("FB_PARSER_DISABLED") or "").strip().lower() in ("1", "true", "yes", "y")

_seen_hashes: set[str] = set()


# -----------------------------
# Helpers
# -----------------------------
def _auth_headers() -> dict:
    headers: dict[str, str] = {}
    if API_SECRET:
        headers["X-API-KEY"] = API_SECRET
        headers["Authorization"] = f"Bearer {API_SECRET}"
    return headers


def _load_cookies_from_env() -> list:
    try:
        parsed = json.loads(FB_COOKIES_JSON)
        return parsed if isinstance(parsed, list) else []
    except Exception as e:
        logger.error("❌ Не удалось распарсить FB_COOKIES_JSON: %s", e)
        return []


def fetch_fb_cookies_from_miniapp() -> list:
    """
    miniapp endpoint: GET /api/parser_secrets/fb_cookies_json -> {"value": "<json string>"}
    """
    url = f"{API_BASE_URL}/api/parser_secrets/fb_cookies_json"
    if not API_SECRET:
        return []
    try:
        r = requests.get(url, headers=_auth_headers(), timeout=10)
        if r.status_code >= 400:
            return []
        data = r.json() or {}
        value = data.get("value")
        if not value:
            return []
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


FB_COOKIES = fetch_fb_cookies_from_miniapp() or _load_cookies_from_env()


def today_str() -> str:
    return date.today().isoformat()


def is_today(created_at: Any) -> bool:
    if not created_at:
        return False
    s = str(created_at)

    # ISO
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date() == date.today()
    except Exception:
        pass

    # epoch seconds/ms
    try:
        ts = float(s)
        if ts > 1e12:
            ts /= 1000.0
        dt = datetime.utcfromtimestamp(ts)
        return dt.date() == date.today()
    except Exception:
        return False


def get_fb_groups() -> List[str]:
    """
    Поддерживаем:
      - /api/groups -> {"groups":[{"group_id": "...", "group_url": "...", "type":"facebook"}, ...]}
      - /api/fb_groups -> {"items":[{"link":"..."}, ...]}
    """
    try:
        resp = requests.get(FB_GROUPS_API_URL, headers=_auth_headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("❌ Ошибка запроса FB-групп: %s", e)
        return []

    urls: list[str] = []

    if isinstance(data, dict) and "groups" in data:
        for g in (data.get("groups") or []):
            if not isinstance(g, dict):
                continue
            if not g.get("enabled", True):
                continue
            t = (g.get("type") or "").lower()
            if t and t != "facebook":
                continue
            raw = (g.get("group_url") or g.get("group_id") or "").strip()
            if raw and "t.me/" not in raw:
                urls.append(raw)

    elif isinstance(data, dict) and "items" in data:
        for it in (data.get("items") or []):
            if not isinstance(it, dict):
                continue
            raw = (it.get("link") or "").strip()
            if raw and "t.me/" not in raw:
                urls.append(raw)

    norm_urls: list[str] = []
    for raw in urls:
        if raw.startswith("http://") or raw.startswith("https://"):
            norm_urls.append(raw)
        else:
            norm_urls.append(f"https://www.facebook.com/groups/{raw.lstrip('@')}")

    return list(dict.fromkeys(norm_urls))


def send_alert(text: str) -> None:
    try:
        requests.post(
            f"{API_BASE_URL}/api/alert",
            headers=_auth_headers(),
            json={"text": text, "message": text, "source": "fb_parser"},
            timeout=10,
        )
    except Exception:
        pass


def hash_post(text: str, url: Optional[str]) -> str:
    base = (text or "").strip()
    if url:
        base += f"::{url}"
    return str(abs(hash(base)))


def send_job_to_miniapp(
    text: str,
    post_url: Optional[str],
    created_at: Optional[str],
    group_url: Optional[str],
    author_url: Optional[str],
) -> None:
    payload = {
        "source": "facebook",
        "source_name": group_url or "facebook_group",
        "external_id": post_url or created_at or hash_post(text, None),
        "url": post_url,
        "text": text,
        "sender_username": author_url,
        "created_at": created_at,
    }
    requests.post(
        f"{API_BASE_URL}/post",
        json=payload,
        headers=_auth_headers(),
        timeout=30,
    ).raise_for_status()


def post_status(key: str, value: str) -> None:
    try:
        requests.post(
            f"{API_BASE_URL}/api/parser_status/{key}",
            json={"value": value},
            headers=_auth_headers(),
            timeout=10,
        )
    except Exception:
        pass


# -----------------------------
# Apify
# -----------------------------
def call_apify_for_group(group_url: str) -> List[Dict[str, Any]]:
    global FB_COOKIES

    if FB_PARSER_DISABLED:
        logger.warning("⛔ FB парсер отключён (FB_PARSER_DISABLED=true)")
        return []

    # refresh cookies from miniapp if possible
    latest = fetch_fb_cookies_from_miniapp()
    if latest:
        FB_COOKIES = latest

    if not FB_COOKIES:
        send_alert(
            "FB парсер: cookies не заданы.\n"
            "Открой миниапп → ⚙️ Настройки → Аккаунты → Facebook cookies и вставь JSON."
        )
        return []

    endpoint = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN}

actor_input: Dict[str, Any] = {
    "cookie": FB_COOKIES,
    "minDelay": APIFY_MIN_DELAY,
    "maxDelay": APIFY_MAX_DELAY,
    "proxy": {"useApifyProxy": True},
    "scrapeGroupPosts.groupUrl": group_url,  # ✅ ВАЖНО: именно так, с точкой
    "sortType": APIFY_SORT_TYPE,
    "count": APIFY_COUNT,
}

if APIFY_PROXY_COUNTRY:
    actor_input["proxy"]["apifyProxyCountry"] = APIFY_PROXY_COUNTRY

if APIFY_SCRAPE_UNTIL:
    actor_input["scrapeUntil"] = APIFY_SCRAPE_UNTIL


    logger.info(
        "▶️ Apify call group=%s actor=%s cookies=%d count=%s sortType=%s",
        group_url,
        APIFY_ACTOR_ID,
        len(FB_COOKIES or []),
        actor_input.get("count"),
        actor_input.get("sortType"),
    )

    try:
        resp = requests.post(
            endpoint,
            params=params,
            json=actor_input,  # ВАЖНО: без {"input": ...}
            timeout=APIFY_TIMEOUT_SECONDS,
        )

        if resp.status_code >= 400:
            body = resp.text[:2000]
            try:
                j = resp.json()
                body = json.dumps(j, ensure_ascii=False)[:2000]
            except Exception:
                pass

            msg = f"Ошибка Apify:\n{group_url}\nHTTP {resp.status_code}\n{body}"
            logger.error("❌ %s", msg)
            send_alert(msg)
            return []

        data = resp.json()

    except Exception as e:
        logger.error("❌ Ошибка вызова Apify для %s: %s", group_url, e)
        send_alert(f"Ошибка Apify при запросе группы:\n{group_url}\n\n{e}")
        return []

    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "items" in data:
        return data["items"]

    logger.warning("Неожиданный формат ответа Apify: %r", data)
    return []


# -----------------------------
# Main loop
# -----------------------------
def process_cycle() -> None:
    group_urls = get_fb_groups()
    now_iso = datetime.utcnow().isoformat() + "Z"

    if not group_urls:
        post_status("fb_last_ok", now_iso)
        return

    for group_url in group_urls:
        items = call_apify_for_group(group_url)

        for item in items:
            if not isinstance(item, dict):
                continue

            text = item.get("text") or ""
            post_url = item.get("url")
            created_at = item.get("createdAt")

            # фильтруем только сегодняшние
            if not is_today(created_at):
                continue

            author_url: Optional[str] = None
            user_obj = item.get("user")
            if isinstance(user_obj, dict):
                author_url = user_obj.get("url")

            h = hash_post(text, post_url)
            if h in _seen_hashes:
                continue
            _seen_hashes.add(h)

            try:
                send_job_to_miniapp(
                    text=text,
                    post_url=post_url,
                    created_at=str(created_at) if created_at else None,
                    group_url=group_url,
                    author_url=author_url,
                )
            except Exception as e:
                logger.error("❌ Ошибка отправки поста: %s", e)

    post_status("fb_last_ok", now_iso)


def main() -> None:
    while True:
        try:
            process_cycle()
        except Exception as e:
            logger.error("❌ Критическая ошибка цикла: %s", e)
            send_alert(f"FB parser: critical error\n\n{e}")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
