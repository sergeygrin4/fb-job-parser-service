import os
import time
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests
from facebook_scraper import get_posts
from requests.exceptions import HTTPError, RequestException

# ----------------- ЛОГИ -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - fb_parser - %(levelname)s - %(message)s",
)
log = logging.getLogger("fb_parser")

# ----------------- КОНФИГ ЧЕРЕЗ ENV -----------------

# например: https://miniapptg-production-caaa.up.railway.app
API_BASE_URL = os.getenv("API_BASE_URL")
API_SECRET = os.getenv("API_SECRET", "mvp-secret-key-2024")

# Ключевые слова для поиска (через запятую), если не задано — дефолтный список
KEYWORDS_ENV = os.getenv(
    "KEYWORDS",
    "вакансия,работа,job,hiring,remote,developer,программист",
)
KEYWORDS: List[str] = [k.strip().lower() for k in KEYWORDS_ENV.split(",") if k.strip()]

# Куки для Facebook в JSON-формате в переменной FB_COOKIES_JSON
# пример:
# {"c_user": "...", "xs": "...", ...}
FB_COOKIES_JSON = os.getenv("FB_COOKIES_JSON")
COOKIES: Optional[Dict[str, str]] = None
if FB_COOKIES_JSON:
    try:
        COOKIES = json.loads(FB_COOKIES_JSON)
        log.info("Cookies загружены из FB_COOKIES_JSON")
    except json.JSONDecodeError:
        log.error("❌ Не могу распарсить FB_COOKIES_JSON — проверь формат JSON")
        COOKIES = None
else:
    log.warning("⚠️ FB_COOKIES_JSON не задан — Facebook, скорее всего, покажет капчу/логин")

if not API_BASE_URL:
    log.error("❌ Не задан API_BASE_URL — без него парсер не знает, куда слать вакансии")
    # но не выходим, вдруг кто-то поставит потом

# ----------------- УТИЛИТЫ -----------------


def normalize_group_identifier(group_link: str) -> Optional[str]:
    """
    Превращает ссылку/имя группы в то, что нужно передать в facebook_scraper.get_posts(group=...).

    Примеры:
      "https://www.facebook.com/groups/ProjectAmazon" -> "ProjectAmazon"
      "https://facebook.com/groups/123456789"        -> "123456789"
      "ProjectAmazon"                                -> "ProjectAmazon"
    """
    if not group_link:
        return None

    group_link = group_link.strip()

    # Если просто ID или имя
    if not group_link.startswith("http://") and not group_link.startswith("https://"):
        return group_link.strip("/")

    parsed = urlparse(group_link)
    path = (parsed.path or "").strip("/")  # "groups/ProjectAmazon" или "groups/123456789"

    parts = path.split("/")
    # Ожидаемый вариант: ["groups", "ProjectAmazon"]
    if len(parts) >= 2 and parts[0] == "groups":
        return parts[1]

    # На всякий случай — берём последний сегмент
    if parts:
        return parts[-1]

    return None


def matches_keywords(text: str) -> bool:
    """
    Проверка, содержит ли текст хотя бы одно из ключевых слов.
    """
    if not text:
        return False
    lower = text.lower()
    return any(k in lower for k in KEYWORDS)


# ----------------- РАБОТА С API МИНИАППА -----------------


def get_fb_groups() -> List[Dict]:
    """
    Забираем список групп из миниаппа: GET {API_BASE_URL}/api/groups

    Миниапп возвращает и Facebook, и Telegram источники в одной таблице fb_groups.
    Здесь мы оставляем ТОЛЬКО Facebook-группы:

      - group_id содержит 'facebook.com' или 'fb.com'
      - при этом НЕ содержит 't.me' (телега) и 'telegram.me'.
    """
    if not API_BASE_URL:
        log.error("❌ API_BASE_URL не задан, не могу получить список групп.")
        return []

    url = API_BASE_URL.rstrip("/") + "/api/groups"
    log.info(f"API групп: {url}")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except RequestException as e:
        log.error(f"❌ Не удалось получить группы: {e}")
        return []

    try:
        data = resp.json()
    except ValueError:
        log.error("❌ Невалидный JSON при получении групп")
        return []

    all_groups = data.get("groups") or []

    # Сначала берём только enabled = true
    enabled_groups: List[Dict] = [g for g in all_groups if g.get("enabled")]

    fb_groups: List[Dict] = []
    skipped_non_fb: List[str] = []

    for g in enabled_groups:
        gid = (g.get("group_id") or "").strip()
        low = gid.lower()
        # Телеграм-источники отдаем на съедение tg_parser'у
        if "t.me/" in low or "telegram.me" in low:
            skipped_non_fb.append(gid)
            continue
        # Оставляем только URL/идентификаторы, в которых явно видно facebook/fb
        if "facebook.com" in low or "fb.com" in low:
            fb_groups.append(g)
        else:
            # всё остальное тоже игнорируем (на будущее)
            skipped_non_fb.append(gid)

    log.info(
        f"Всего групп из API: {len(all_groups)}; активных: {len(enabled_groups)}; facebook-групп: {len(fb_groups)}"
    )
    if skipped_non_fb:
        log.info(f"Пропущены не-facebook источники: {skipped_non_fb}")

    return fb_groups


def send_job_to_api(
    source: str,
    source_name: str,
    external_id: str,
    url: Optional[str],
    text: str,
    created_at: Optional[datetime],
) -> None:
    """
    Шлём вакансию в миниапп: POST {API_BASE_URL}/post с X-API-KEY.
    Формат согласован с backend’ом миниаппа.
    """
    if not API_BASE_URL:
        log.error("❌ API_BASE_URL не задан, не могу отправить вакансию")
        return

    endpoint = API_BASE_URL.rstrip("/") + "/post"
    headers = {
        "Content-Type": "application/json",
    }
    if API_SECRET:
        headers["X-API-KEY"] = API_SECRET

    payload = {
        "source": source,  # "facebook"
        "source_name": source_name,
        "external_id": str(external_id),
        "url": url,
        "text": text,
        "created_at": created_at.isoformat() if created_at else None,
    }

    try:
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status")
        if status == "duplicate":
            log.info(f"🔁 Дубликат вакансии {external_id} ({source_name})")
        else:
            log.info(f"✅ Вакансия отправлена в API ({source_name} / {external_id})")
    except RequestException as e:
        log.error(f"❌ Ошибка отправки вакансии в API: {e}")


# ----------------- ПАРСИНГ ОДНОЙ ГРУППЫ ---------------


def parse_group(group_link: str, group_name: str, cookies: Optional[Dict]) -> int:
    """
    Парсим одну группу, фильтруем по ключевым словам, отправляем вакансии в миниапп.
    Возвращает количество обработанных постов (после фильтра).
    """
    log.info(f"🔍 Парсим группу: {group_name} ({group_link})")

    group = normalize_group_identifier(group_link)
    if not group:
        log.error(f"❌ Не удалось нормализовать group из {group_link}")
        return 0

    log.info(f"Используем идентификатор группы для facebook_scraper: {group}")

    count = 0

    try:
        # pages можно увеличить, если нужно больше постов
        for post in get_posts(
            group=group,
            pages=1,
            cookies=cookies,
            options={"allow_extra_requests": False},
        ):
            text = post.get("text") or ""
            if not matches_keywords(text):
                continue

            post_id = post.get("post_id") or ""
            external_id = str(post_id) if post_id else (
                post.get("post_url") or post.get("link") or text[:30]
            )

            post_url = post.get("post_url") or post.get("link")
            created_at = post.get("time")  # обычно datetime или None

            send_job_to_api(
                source="facebook",
                source_name=group_name,
                external_id=external_id,
                url=post_url,
                text=text,
                created_at=created_at,
            )
            count += 1

    except HTTPError as e:
        log.error(f"❌ HTTPError при запросе группы {group_link}: {e}")
    except Exception as e:
        log.error(f"❌ Неожиданная ошибка при парсинге {group_link}: {e}")

    log.info(f"📦 Обработано {count} постов для группы {group_name}")
    return count


# ----------------- ОСНОВНОЙ ЦИКЛ -----------------


def run_loop():
    """
    Один цикл парсинга:
      1) забираем активные FB-группы из миниаппа
      2) обходим по очереди
      3) отправляем вакансии
    """
    if not API_BASE_URL:
        log.error("❌ API_BASE_URL не задан — останавливаю цикл")
        return

    log.info(f"API: {API_BASE_URL}")
    log.info(f"Ключевые слова: {KEYWORDS}")
    if COOKIES:
        log.info("Cookies загружены из FB_COOKIES_JSON")
    else:
        log.warning("⚠️ Cookies НЕ заданы — Facebook скорее всего вернёт капчу/логин")

    groups = get_fb_groups()
    total_posts = 0

    for g in groups:
        group_link = g.get("group_id") or ""
        group_name = g.get("group_name") or group_link
        total_posts += parse_group(group_link, group_name, COOKIES)

    log.info(f"✅ Цикл завершён. Всего обработано постов: {total_posts}")


def main():
    log.info("🚀 Запуск Facebook Job Parser")

    while True:
        try:
            run_loop()
        except Exception as e:
            log.error(f"❌ Ошибка в основном цикле: {e}")
        log.info("⏳ Ожидание 5 минут...")
        time.sleep(300)


if __name__ == "__main__":
    main()
