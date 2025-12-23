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
try:
    FB_COOKIES = json.loads(FB_COOKIES_JSON)
except Exception as e:
    logger.error("❌ Не удалось распарсить FB_COOKIES_JSON: %s", e)
    FB_COOKIES = []

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

    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date() == date.today()
    except Exception:
        pass

    try:
        ts = float(s)
        if ts > 1e12:
            ts /= 1000.0
        dt = datetime.utcfromtimestamp(ts)
        return dt.date() == date.today()
    except Exception:
        return False


def get_fb_groups() -> List[str]:
    try:
        logger.info("Запрашиваю FB-группы из %s", FB_GROUPS_API_URL)
        resp = requests.get(FB_GROUPS_API_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("❌ Ошибка запроса FB-групп: %s", e)
        return []

    urls = []
    for g in data.get("groups", []):
        if not g.get("enabled", True):
            continue
        url = (g.get("group_url") or "").strip()
        if url:
            urls.append(url)

    logger.info("Найдено %d включённых FB-групп", len(urls))
    return urls


def send_alert(text: str):
    try:
        requests.post(
            f"{API_BASE_URL}/api/alert",
            headers={"X-API-SECRET": API_SECRET},
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
        requests.post(endpoint, json=payload, headers=headers, timeout=30).raise_for_status()
        logger.info("✅ Отправили пост в миниапп: %s", post_url)
    except Exception as e:
        logger.error("❌ Ошибка отправки поста в миниапп: %s", e)


# ---------- APIFY ----------

def call_apify_for_group(group_url: str) -> List[Dict[str, Any]]:
    global FB_PARSER_DISABLED

    if FB_PARSER_DISABLED:
        logger.warning("⛔ FB парсер отключён из-за невалидных cookies")
        return []

    endpoint = (
        f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"
        f"?token={APIFY_TOKEN}"
    )

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
                "Обнови cookies и сделай redeploy."
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
    group_urls = get_fb_groups()
    if not group_urls:
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
