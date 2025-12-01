import os
import time
import json
import logging
from datetime import date
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

# откуда берём список FB-групп
FB_GROUPS_API_URL = os.getenv(
    "FB_GROUPS_API_URL",
    f"{API_BASE_URL}/api/fb_groups",
)

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
APIFY_ACTOR_ID = os.getenv("APIFY_ACTOR_ID", "AtBpiepuIUNs2k2ku")

if not APIFY_TOKEN:
    raise RuntimeError("APIFY_TOKEN is not set")

# JSON-массив cookies как в твоём примере выше
FB_COOKIES_JSON = os.getenv("FB_COOKIES_JSON", "[]")
try:
    FB_COOKIES = json.loads(FB_COOKIES_JSON)
except Exception as e:
    logger.error("❌ Не удалось распарсить FB_COOKIES_JSON: %s", e)
    FB_COOKIES = []

APIFY_MIN_DELAY = int(os.getenv("APIFY_MIN_DELAY", "1"))
APIFY_MAX_DELAY = int(os.getenv("APIFY_MAX_DELAY", "10"))

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "900"))  # 15 мин по умолчанию

KEYWORDS = [
    "вакансия",
    "работа",
    "job",
    "hiring",
    "remote",
    "developer",
    "программист",
]

_seen_hashes: set[str] = set()


# ---------- УТИЛИТЫ ----------

def matches_keywords(text: str | None) -> bool:
    if not text:
        return False
    low = text.lower()
    return any(k in low for k in KEYWORDS)


def today_str() -> str:
    return date.today().isoformat()  # 'YYYY-MM-DD'


def get_fb_groups() -> List[str]:
    """
    Ожидаемый ответ миниаппа:
    { "groups": [ { "id": 1, "group_url": "...", "enabled": true }, ... ] }
    """
    try:
        logger.info("Запрашиваю FB-группы из %s", FB_GROUPS_API_URL)
        resp = requests.get(FB_GROUPS_API_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error("❌ Не удалось получить FB-группы: %s", e)
        return []

    groups = data.get("groups") or []
    urls: List[str] = []
    for g in groups:
        if not g.get("enabled", True):
            continue
        url = (g.get("group_url") or "").strip()
        if url:
            urls.append(url)

    logger.info("📥 Активные FB-группы: %s", urls)
    return urls


def call_apify_for_group(group_url: str) -> List[Dict[str, Any]]:
    """
    Вызывает actor AtBpiepuIUNs2k2ku для одной группы
    с тем же input, что у тебя в консоли:
      - cookie: [ ... ]
      - maxDelay, minDelay
      - proxy.useApifyProxy = true
      - scrapeGroupPosts.groupUrl
      - scrapeUntil = сегодня
      - sortType = new_posts
    """
    endpoint = (
        f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"
        f"?token={APIFY_TOKEN}"
    )

    actor_input = {
        "cookie": FB_COOKIES,
        "maxDelay": APIFY_MAX_DELAY,
        "minDelay": APIFY_MIN_DELAY,
        "proxy": {
            "useApifyProxy": True,
        },
        "scrapeGroupPosts.groupUrl": group_url,
        "scrapeUntil": today_str(),  # посты до сегодняшнего дня
        "sortType": "new_posts",
    }

    logger.info("▶️ Вызываю Apify actor для группы: %s", group_url)

    try:
        resp = requests.post(endpoint, json=actor_input, timeout=600)
        resp.raise_for_status()
    except Exception as e:
        logger.error("❌ Ошибка вызова Apify для %s: %s", group_url, e)
        return []

    try:
        data = resp.json()
    except Exception as e:
        logger.error("❌ JSON-ошибка от Apify (%s): %s", group_url, e)
        return []

    # run-sync-get-dataset-items обычно возвращает либо список, либо объект с items
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict) and "items" in data:
        items = data["items"]
    else:
        logger.warning(
            "Неожиданный формат ответа Apify для %s (%s): %r",
            group_url,
            type(data).__name__,
            str(data)[:300],
        )
        items = []

    logger.info("📄 Apify для %s вернул %d постов", group_url, len(items))
    return items


def hash_post(text: str, url: str | None) -> str:
    import hashlib

    h = hashlib.sha256()
    h.update((text or "").encode("utf-8"))
    if url:
        h.update(url.encode("utf-8"))
    return h.hexdigest()


def send_job_to_miniapp(
    text: str,
    post_url: str | None,
    created_at: str | None,
    group_url: str | None,
):
    if not text:
        return

    url = f"{API_BASE_URL}/post"
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": API_SECRET,
    }

    payload = {
        "source": "facebook",
        "source_name": group_url or "facebook_group",
        "external_id": post_url or (created_at or ""),
        "url": post_url,
        "text": text,
        "created_at": created_at,
    }

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        if resp.status_code >= 300:
            logger.error(
                "❌ Ошибка отправки в миниапп: %s %s",
                resp.status_code,
                resp.text[:500],
            )
        else:
            logger.info("✅ Вакансия отправлена в миниапп: %s", (post_url or "")[:120])
    except Exception as e:
        logger.error("❌ Сетевая ошибка при отправке в миниапп: %s", e)


# ---------- ОСНОВНОЙ ЦИКЛ ----------

def process_cycle():
    group_urls = get_fb_groups()
    if not group_urls:
        logger.warning("Нет FB-групп для парсинга, пропускаю цикл")
        return

    total_sent = 0

    for group_url in group_urls:
        items = call_apify_for_group(group_url)

        for item in items:
            # имена полей у актора могут быть своими — подстраховываемся
            text = (
                item.get("text")
                or item.get("message")
                or item.get("content")
                or item.get("postText")
                or ""
            )
            if not matches_keywords(text):
                continue

            post_url = (
                item.get("postUrl")
                or item.get("url")
                or item.get("post_url")
            )

            created_at = (
                item.get("createdAt")
                or item.get("timestamp")
                or item.get("created_time")
            )

            group_field = (
                item.get("groupUrl")
                or item.get("group_url")
                or group_url
            )

            h = hash_post(text, post_url)
            if h in _seen_hashes:
                logger.info("🔁 Дубликат поста (hash=%s), пропускаю", h)
                continue
            _seen_hashes.add(h)

            send_job_to_miniapp(text, post_url, created_at, group_field)
            total_sent += 1

    logger.info("✅ Цикл завершён, всего отправлено вакансий: %d", total_sent)


def main():
    logger.info("🚀 Запуск Facebook Job Parser через Apify actor %s", APIFY_ACTOR_ID)
    while True:
        try:
            process_cycle()
        except Exception as e:
            logger.error("❌ Необработанная ошибка в цикле: %s", e)
        logger.info("⏳ Ожидание %d секунд до следующего цикла", POLL_INTERVAL_SECONDS)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
