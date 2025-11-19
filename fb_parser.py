# fb_parser.py
import os
import time
import logging
import hashlib
from urllib.parse import urlparse

from facebook_scraper import get_posts
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - fb_parser - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

API_BASE_URL = os.getenv("API_BASE_URL")  # типа "https://telegram-job-parser-production.up.railway.app"
API_SECRET = os.getenv("API_SECRET", "mvp-secret-key-2024-xyz")

JOB_KEYWORDS = [
    kw.strip().lower()
    for kw in os.getenv("JOB_KEYWORDS", "вакансия,работа,job,hiring,remote,developer").split(",")
    if kw.strip()
]

CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "5"))
MAX_POSTS_PER_GROUP = int(os.getenv("MAX_POSTS_PER_GROUP", "20"))

FACEBOOK_COOKIES = os.getenv("FACEBOOK_COOKIES")  # сырые cookies строкой, если надо


def get_cookies_dict():
    """
    Простой разбор cookies формата "key1=value1; key2=value2"
    """
    if not FACEBOOK_COOKIES:
        return None
    cookies = {}
    for part in FACEBOOK_COOKIES.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        cookies[k.strip()] = v.strip()
    return cookies


def get_fb_groups():
    """
    Тянем список FB-групп из miniapp-сервиса.
    GET /api/groups
    """
    url = f"{API_BASE_URL}/api/groups"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    groups = []
    for g in data.get("groups", []):
        if g.get("enabled"):
            groups.append((g["group_id"], g["group_name"]))
    return groups


def text_matches_keywords(text: str) -> bool:
    t = (text or "").lower()
    return any(kw in t for kw in JOB_KEYWORDS)


def build_external_id(group_id: str, post_id: str) -> str:
    # На всякий случай нормализуем
    raw = f"fb:{group_id}:{post_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def send_job(group_name: str, group_link: str, post) -> None:
    """
    Отправка вакансии в miniapp /post
    """
    post_id = post.get("post_id") or post.get("post_url")
    text = post.get("text") or ""
    post_url = post.get("post_url") or group_link

    external_id = build_external_id(group_link, str(post_id))

    payload = {
        "source": "facebook",
        "source_name": group_name,
        "external_id": external_id,
        "url": post_url,
        "text": text,
        "created_at": post.get("time").isoformat() if post.get("time") else None,
    }

    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": API_SECRET,
    }

    resp = requests.post(f"{API_BASE_URL}/post", json=payload, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("status") == "duplicate":
            log.info(f"🔁 Уже есть такой пост: {external_id}")
        else:
            log.info(f"✅ Новая вакансия отправлена: {external_id}")
    else:
        log.error(f"❌ Ошибка отправки вакансии: {resp.status_code} {resp.text}")


def parse_group(group_link: str, group_name: str, cookies: dict | None):
    """
    Парсинг одной группы
    """
    log.info(f"🔍 Парсим группу: {group_name} ({group_link})")

    # facebook-scraper может принимать либо group=ID, либо account=...
    parsed = urlparse(group_link)
    group = parsed.path.strip("/")

    count = 0
    for post in get_posts(
        group=group,
        pages=1,
        cookies=cookies,
        options={"allow_extra_requests": False},
    ):
        text = post.get("text") or ""
        if not text_matches_keywords(text):
            continue

        send_job(group_name, group_link, post)
        count += 1

        if count >= MAX_POSTS_PER_GROUP:
            break

    log.info(f"📌 Для {group_name} найдено {count} постов по ключевым словам")
    return count


def run_loop():
    cookies = get_cookies_dict()
    log.info("🚀 Запуск Facebook Job Parser")

    while True:
        try:
            groups = get_fb_groups()
            log.info(f"Найдено {len(groups)} активных групп")

            total_posts = 0
            for group_link, group_name in groups:
                total_posts += parse_group(group_link, group_name, cookies)
                time.sleep(2)

            log.info(f"✅ Цикл завершён. Обработано {total_posts} постов")
        except Exception as e:
            log.exception(f"❌ Ошибка в основном цикле: {e}")

        log.info(f"⏳ Ожидание {CHECK_INTERVAL_MINUTES} минут...")
        time.sleep(CHECK_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    run_loop()
