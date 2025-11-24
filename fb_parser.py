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

# ====================== ENV ======================

# URL миниаппа, например:
# https://job-miniapp-service-production.up.railway.app
API_BASE_URL = os.getenv("API_BASE_URL")

# Должен совпадать с API_SECRET в миниаппе
API_SECRET = os.getenv("API_SECRET", "mvp-secret-key-2024-xyz")

JOB_KEYWORDS = [
    kw.strip().lower()
    for kw in os.getenv(
        "JOB_KEYWORDS",
        "вакансия,работа,job,hiring,remote,developer,программист,amazon",
    ).split(",")
    if kw.strip()
]

CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "5"))
MAX_POSTS_PER_GROUP = int(os.getenv("MAX_POSTS_PER_GROUP", "20"))

# Сырые cookies строкой, если нужно парсить закрытые/приватные группы:
# "key1=value1; key2=value2; ..."
FACEBOOK_COOKIES = os.getenv("FACEBOOK_COOKIES")


# ====================== Вспомогалки ======================

def get_cookies_dict():
    """
    Простой разбор cookies формата "key1=value1; key2=value2"
    """
    if not FACEBOOK_COOKIES:
        return None
    cookies: dict[str, str] = {}
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
    GET {API_BASE_URL}/api/groups
    """
    if not API_BASE_URL:
        raise RuntimeError("API_BASE_URL is not set")

    url = f"{API_BASE_URL.rstrip('/')}/api/groups"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    groups: list[tuple[str, str]] = []
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


def send_job(group_name: str, group_link: str, post: dict) -> None:
    """
    Отправка вакансии в miniapp /post
    """
    if not API_BASE_URL:
        raise RuntimeError("API_BASE_URL is not set")

    post_id = post.get("post_id") or post.get("post_url") or ""
    text = post.get("text") or ""
    post_url = post.get("post_url") or group_link

    external_id = build_external_id(group_link, str(post_id))

    created_at = None
    if post.get("time"):
        try:
            created_at = post["time"].isoformat()
        except Exception:
            created_at = None

    payload = {
        "source": "facebook",
        "source_name": group_name,
        "external_id": external_id,
        "url": post_url,
        "text": text,
        "created_at": created_at,
    }

    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": API_SECRET,
    }

    resp = requests.post(
        f"{API_BASE_URL.rstrip('/')}/post",
        json=payload,
        headers=headers,
        timeout=30,
    )
    if resp.status_code == 200:
        data = resp.json()
        if data.get("status") == "duplicate":
            log.info("🔁 Уже есть такой пост: %s", external_id)
        else:
            log.info("✅ Новая вакансия отправлена: %s", external_id)
    else:
        log.error("❌ Ошибка отправки вакансии: %s %s", resp.status_code, resp.text)


def parse_group(group_link: str, group_name: str, cookies: dict | None):
    """
    Парсинг одной группы.
    group_link — то, что хранится в fb_groups.group_id (может быть и полноценная ссылка).
    """
    log.info("🔍 Парсим группу: %s (%s)", group_name, group_link)

    parsed = urlparse(group_link)
    group = parsed.path.strip("/") or group_link

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

    log.info("📌 Для %s найдено %s постов по ключевым словам", group_name, count)
    return count


def run_loop():
    cookies = get_cookies_dict()
    log.info("🚀 Запуск Facebook Job Parser")
    log.info("Ключевые слова: %s", JOB_KEYWORDS)

    while True:
        try:
            groups = get_fb_groups()
            log.info("Найдено %s активных групп", len(groups))

            total_posts = 0
            for group_link, group_name in groups:
                total_posts += parse_group(group_link, group_name, cookies)
                time.sleep(2)

            log.info("✅ Цикл завершён. Обработано %s постов", total_posts)
        except Exception as e:
            log.exception("❌ Ошибка в основном цикле: %s", e)

        log.info("⏳ Ожидание %s минут...", CHECK_INTERVAL_MINUTES)
        time.sleep(CHECK_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    run_loop()
