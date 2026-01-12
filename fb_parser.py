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

API_SECRET = os.getenv("API_SECRET", "")

FB_GROUPS_API_URL = os.getenv(
    "FB_GROUPS_API_URL",
    f"{API_BASE_URL}/api/fb_groups",
)

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
APIFY_ACTOR_ID = os.getenv("APIFY_ACTOR_ID", "AtBpiepuIUNs2k2ku")

if not APIFY_TOKEN:
    raise RuntimeError("APIFY_TOKEN is not set")

FB_COOKIES_JSON = os.getenv("FB_COOKIES_JSON", "[]")


def _load_cookies_from_env() -> list:
    try:
        return json.loads(FB_COOKIES_JSON)
    except Exception as e:
        logger.error("❌ Не удалось распарсить FB_COOKIES_JSON: %s", e)
        return []


def fetch_fb_cookies_from_miniapp() -> list:
    """Берём cookies из miniapp (parser_secrets), чтобы не делать redeploy."""
    try:
        url = f"{API_BASE_URL}/api/parser_secrets/fb_cookies_json"
        r = requests.get(
            url,
            headers={"X-API-KEY": API_SECRET} if API_SECRET else {},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json() or {}
        value = data.get("value")
        if not value:
            return []
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except Exception:
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

    # сначала пробуем ISO-строку
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date() == date.today()
    except Exception:
        pass

    # потом — timestamp (сек/мс)
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
    Забираем список FB-групп из miniapp.

    API сейчас отдаёт:
      {"groups": [{"id": ..., "group_id": "...", "enabled": true, ...}, ...]}

    group_id может быть как полным URL, так и просто ID группы.
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

    # поддерживаем оба варианта ответа: dict {"groups": [...]} и просто список
    groups = data.get("groups", []) if isinstance(data, dict) else data

    for g in groups:
        if not g.get("enabled", True):
            continue

        # сначала пробуем group_url (на будущее), потом group_id
        raw = (g.get("group_url") or g.get("group_id") or "").strip()
        if not raw:
            continue

        # если это не URL, считаем что это ID группы и собираем URL
        if raw.startswith("http://") or raw.startswith("https://"):
            url = raw
        else:
            url = f"https://facebook.com/groups/{raw}"

        urls.append(url)

    logger.info("Найдено %d включённых FB-групп", len(urls))
    return urls



def send_alert(text: str):
    try:
        requests.post(
            f"{API_BASE_URL}/api/alert",
            headers={"X-API-KEY": API_SECRET} if API_SECRET else {},
            json={
                "source": "fb_parser",
                "message": text,
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
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": API_SECRET,
    }

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
        requests.post(
            endpoint,
            json=payload,
            headers=headers,
            timeout=30,
        ).raise_for_status()
        logger.info("✅ Отправили пост в миниапп: %s", post_url)
    except Exception as e:
        logger.error("❌ Ошибка отправки поста в миниапп: %s", e)


def post_status(key: str, value: str):
    """Пинг статуса парсера в miniapp (/api/parser_status/<key>)."""
    try:
        url = f"{API_BASE_URL}/api/parser_status/{key}"
        headers = {"X-API-KEY": API_SECRET} if API_SECRET else {}
        requests.post(
            url,
            json={"value": value},
            headers=headers,
            timeout=10,
        )
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

    endpoint = (
        f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"
        f"?token={APIFY_TOKEN}"
    )

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
        "scrapeGroupPosts.groupUrl": group_url,
        "scrapeUntil": today_str(),
        "sortType": "new_posts",
    }

    logger.info("▶️ Вызов Apify для группы %s", group_url)

    try:
        resp = requests.post(endpoint, json=actor_input, timeout=600)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("❌ Ошибка вызова Apify для %s: %s", group_url, e)
        send_alert(f"Ошибка Apify при запросе группы:\n{group_url}\n\n{e}")
        return []

    if isinstance(data, dict):
        error_text = (
            data.get("error")
            or data.get("message")
            or data.get("statusMessage")
            or ""
        )

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
        # Групп нет, но парсер жив — пингуем fb_last_ok, чтобы вотчдог не ругался.
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

    # цикл успешно отработал — пингуем fb_last_ok
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
