import os
import time
import json
import logging
from datetime import date, datetime
from typing import List, Dict, Any

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - fb_parser - %(levelname)s - %(message)s",
)
logger = logging.getLogger("fb_parser")

# ---------- КОНФИГ ----------

API_BASE_URL = (os.getenv("API_BASE_URL") or "").rstrip("/")
if not API_BASE_URL:
    raise RuntimeError("API_BASE_URL is not set")


def _get_api_secret() -> str:
    """
    Shared secret between parsers and miniapp.
    Supports multiple env names to avoid misconfig in deploy platforms.
    """
    return (
        os.getenv("API_SECRET")
        or os.getenv("MINIAPP_API_SECRET")
        or os.getenv("X_API_KEY")
        or os.getenv("PARSER_API_SECRET")
        or ""
    ).strip()


API_SECRET = _get_api_secret()

FB_GROUPS_API_URL = os.getenv("FB_GROUPS_API_URL", f"{API_BASE_URL}/api/fb_groups")


def _normalize_apify_token(token: str | None) -> str:
    token = (token or "").strip()
    if not token:
        return ""
    # If someone pasted a full URL, try to extract token=...
    if ("http://" in token or "https://" in token) and "token=" in token:
        token = token.split("token=", 1)[1].split("&", 1)[0].strip()
    return token


APIFY_TOKEN = _normalize_apify_token(os.getenv("APIFY_TOKEN"))
APIFY_ACTOR_ID = os.getenv("APIFY_ACTOR_ID", "AtBpiepuIUNs2k2ku")  # можно username~actorname
if not APIFY_TOKEN:
    raise RuntimeError("APIFY_TOKEN is not set")

FB_COOKIES_JSON = os.getenv("FB_COOKIES_JSON", "[]")


def _load_cookies_from_env() -> list:
    try:
        parsed = json.loads(FB_COOKIES_JSON)
        return parsed if isinstance(parsed, list) else []
    except Exception as e:
        logger.error("❌ Не удалось распарсить FB_COOKIES_JSON: %s", e)
        return []


def fetch_fb_cookies_from_miniapp() -> list:
    """
    Берём cookies из miniapp (parser_secrets), чтобы не делать redeploy.
    Требует API_SECRET.
    """
    url = f"{API_BASE_URL}/api/parser_secrets/fb_cookies_json"

    if not API_SECRET:
        logger.warning(
            "⚠️ API_SECRET не задан в окружении парсера — не могу забрать cookies из miniapp (%s)",
            url,
        )
        return []

    headers = {
        "X-API-KEY": API_SECRET,
        # запасной вариант, если где-то режут кастомные заголовки
        "Authorization": f"Bearer {API_SECRET}",
    }

    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code >= 400:
            logger.error("❌ miniapp cookies HTTP %s: %s", r.status_code, r.text[:1000])
            return []

        data = r.json() or {}
        value = data.get("value")
        if not value:
            logger.warning("⚠️ miniapp вернул пустые cookies (value is empty)")
            return []

        parsed = json.loads(value)
        if not isinstance(parsed, list):
            logger.error("❌ miniapp cookies value не является JSON-массивом (type=%s)", type(parsed).__name__)
            return []

        logger.info("✅ cookies получены из miniapp: %d шт.", len(parsed))
        return parsed

    except Exception as e:
        logger.error("❌ Ошибка получения cookies из miniapp: %s", e)
        return []


# initial cookies (fallback)
FB_COOKIES = fetch_fb_cookies_from_miniapp() or _load_cookies_from_env()

APIFY_MIN_DELAY = int(os.getenv("APIFY_MIN_DELAY", "1"))
APIFY_MAX_DELAY = int(os.getenv("APIFY_MAX_DELAY", "10"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "600"))

_seen_hashes: set[str] = set()

# ⛔ флаг автостопа
FB_PARSER_DISABLED = False


# ---------- УТИЛИТЫ ----------

def today_str() -> str:
    return date.today().isoformat()


def is_today(created_at) -> bool:
    if not created_at:
        return False

    s = str(created_at)

    # ISO-строка
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date() == date.today()
    except Exception:
        pass

    # timestamp (сек/мс)
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
    Забираем список групп из miniapp и приводим к корректным FB-URL.
    """
    try:
        logger.info("Запрашиваю FB-группы из %s", FB_GROUPS_API_URL)
        resp = requests.get(FB_GROUPS_API_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("❌ Ошибка запроса FB-групп: %s", e)
        return []

    urls: list[str] = []

    groups = data.get("groups", []) if isinstance(data, dict) else data

    for g in groups:
        if not g.get("enabled", True):
            continue

        raw = (g.get("group_url") or g.get("group_id") or "").strip()
        if not raw:
            continue

        # если это телеграм – пропускаем
        if "t.me/" in raw or "telegram." in raw:
            continue

        if raw.startswith("http://") or raw.startswith("https://"):
            url = raw
        else:
            slug_or_id = raw.lstrip("@")
            url = f"https://www.facebook.com/groups/{slug_or_id}"

        urls.append(url)

    logger.info("Найдено %d включённых FB-групп", len(urls))
    for u in urls:
        logger.info("  • %s", u)
    return urls


def send_alert(text: str):
    """
    Алерт в miniapp (/api/alert).
    miniapp теперь принимает и X-API-KEY, и Bearer.
    """
    try:
        headers = {}
        if API_SECRET:
            headers = {"X-API-KEY": API_SECRET, "Authorization": f"Bearer {API_SECRET}"}

        requests.post(
            f"{API_BASE_URL}/api/alert",
            headers=headers,
            json={
                "text": text,
                "message": text,  # backward compat
                "source": "fb_parser",
            },
            timeout=10,
        )
    except Exception:
        pass


def hash_post(text: str, url: str | None) -> str:
    base = (text or "").strip()
    if url:
        base += f"::{url}"
    return str(abs(hash(base)))


def send_job_to_miniapp(
    text: str,
    post_url: str | None,
    created_at: str | None,
    group_url: str | None,
    author_url: str | None,
):
    endpoint = f"{API_BASE_URL}/post"
    headers = {"Content-Type": "application/json"}
    if API_SECRET:
        headers["X-API-KEY"] = API_SECRET

    payload = {
        "source": "facebook",
        "source_name": group_url or "facebook_group",
        "external_id": post_url or created_at,
        "url": post_url,
        "text": text,
        "sender_username": author_url,
        "created_at": created_at,
    }

    try:
        requests.post(endpoint, json=payload, headers=headers, timeout=30).raise_for_status()
        logger.info("✅ Отправили пост в миниапп: %s", post_url)
    except Exception as e:
        logger.error("❌ Ошибка отправки поста в миниапп: %s", e)


def post_status(key: str, value: str):
    """Пинг статуса парсера в miniapp (/api/parser_status/<key>)."""
    try:
        url = f"{API_BASE_URL}/api/parser_status/{key}"
        headers = {}
        if API_SECRET:
            headers = {"X-API-KEY": API_SECRET, "Authorization": f"Bearer {API_SECRET}"}
        requests.post(url, json={"value": value}, headers=headers, timeout=10)
    except Exception:
        pass


# ---------- APIFY ----------

def call_apify_for_group(group_url: str) -> List[Dict[str, Any]]:
    global FB_PARSER_DISABLED
    global FB_COOKIES

    # обновляем cookies динамически из miniapp (если есть)
    latest = fetch_fb_cookies_from_miniapp()
    if latest:
        FB_COOKIES = latest

    if FB_PARSER_DISABLED:
        logger.warning("⛔ FB парсер отключён из-за невалидных cookies")
        return []

    endpoint = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN}

    if not FB_COOKIES:
        send_alert(
            "FB парсер: cookies не заданы.\n"
            "Открой миниапп → ⚙️ Настройки → Аккаунты → Facebook cookies и вставь JSON."
        )
        return []

    actor_input = {
        "cookie": FB_COOKIES,
        "maxDelay": APIFY_MAX_DELAY,
        "minDelay": APIFY_MIN_DELAY,
        "proxy": {"useApifyProxy": True},
        # правильный формат для большинства версий actor'а
        "scrapeGroupPosts": {"groupUrl": group_url},
        # на случай форков, которые читают dot-notation
        "scrapeGroupPosts.groupUrl": group_url,
        "scrapeUntil": today_str(),
        "sortType": "new_posts",
    }

    logger.info("▶️ Вызов Apify для группы %s (cookies=%d)", group_url, len(FB_COOKIES or []))

    try:
        resp = requests.post(endpoint, params=params, json=actor_input, timeout=600)
        if resp.status_code >= 400:
            logger.error("❌ Apify HTTP %s: %s", resp.status_code, resp.text[:1500])
            resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("❌ Ошибка вызова Apify для %s: %s", group_url, e)
        send_alert(f"Ошибка Apify при запросе группы:\n{group_url}\n\n{e}")
        return []

    # Если actor вернул dict с ошибкой авторизации cookies
    if isinstance(data, dict):
        error_text = (data.get("error") or data.get("message") or data.get("statusMessage") or "")
        if "failed to authorize with given cookies" in error_text.lower():
            FB_PARSER_DISABLED = True
            logger.error("❌ Facebook cookies протухли — парсер остановлен")
            send_alert(
                "❌ Facebook cookies протухли.\n"
                "FB парсер автоматически остановлен.\n\n"
                "Обнови cookies в миниаппе: ⚙️ Настройки → Аккаунты → Facebook cookies."
            )
            return []

    if isinstance(data, list):
        return data

    if isinstance(data, dict) and "items" in data:
        return data["items"]

    logger.warning("Неожиданный формат ответа Apify: %r", data)
    return []


# ---------- ОСНОВНОЙ ЦИКЛ ----------

def process_cycle():
    """Один цикл: пройтись по всем группам и отправить свежие посты в миниапп."""
    group_urls = get_fb_groups()
    now_iso = datetime.utcnow().isoformat() + "Z"

    if not group_urls:
        post_status("fb_last_ok", now_iso)
        return

    for group_url in group_urls:
        items = call_apify_for_group(group_url)

        for item in items:
            text = item.get("text") or ""
            post_url = item.get("url")
            created_at = item.get("createdAt")

            if not is_today(created_at):
                continue

            user = item.get("user") or {}
            author_url = user.get("url")

            h = hash_post(text, post_url)
            if h in _seen_hashes:
                continue
            _seen_hashes.add(h)

            send_job_to_miniapp(
                text=text,
                post_url=post_url,
                created_at=str(created_at),
                group_url=group_url,
                author_url=author_url,
            )

    post_status("fb_last_ok", now_iso)


def main():
    logger.info("🚀 Запуск Facebook Job Parser через Apify")
    while True:
        try:
            process_cycle()
        except Exception as e:
            logger.error("❌ Критическая ошибка цикла: %s", e)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
