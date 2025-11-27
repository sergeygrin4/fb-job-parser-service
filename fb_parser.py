import os
import time
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, HTTPError

# ----------------- ЛОГИ -----------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - fb_parser - %(levelname)s - %(message)s",
)
log = logging.getLogger("fb_parser")


# ----------------- КОНФИГ -----------------

API_BASE_URL = (os.getenv("API_BASE_URL") or "").rstrip("/")
API_SECRET = os.getenv("API_SECRET", "mvp-secret-key-2024")

# Ключевые слова (через запятую)
KEYWORDS_ENV = os.getenv(
    "KEYWORDS",
    "вакансия,работа,job,hiring,remote,developer,программист",
)
KEYWORDS: List[str] = [k.strip().lower() for k in KEYWORDS_ENV.split(",") if k.strip()]

CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "5"))
POSTS_PER_GROUP = int(os.getenv("POSTS_PER_GROUP", "20"))

FB_COOKIES_JSON = os.getenv("FB_COOKIES_JSON", "")
FB_USER_AGENT = os.getenv(
    "FB_USER_AGENT",
    # Мобильный Chrome под Android, чтобы FB не редиректил на десктоп
    "Mozilla/5.0 (Linux; Android 10; SM-G973F) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Mobile Safari/537.36",
)



# ----------------- КУКИ -----------------


def load_cookies() -> Optional[Dict[str, str]]:
    """
    Читает FB_COOKIES_JSON. Поддерживает оба формата:
      1) {"c_user": "...", "xs": "...", ...}
      2) [{"name": "c_user", "value": "...", ...}, ...]
    Возвращает dict name -> value.
    """
    if not FB_COOKIES_JSON:
        log.warning("⚠️ FB_COOKIES_JSON не задан — Facebook, скорее всего, покажет логин/капчу")
        return None

    try:
        raw = json.loads(FB_COOKIES_JSON)
    except json.JSONDecodeError as e:
        log.error(f"❌ Не могу распарсить FB_COOKIES_JSON как JSON: {e}")
        return None

    cookies: Dict[str, str]
    if isinstance(raw, dict):
        cookies = {k: str(v) for k, v in raw.items()}
    elif isinstance(raw, list):
        cookies = {
            c["name"]: str(c["value"])
            for c in raw
            if isinstance(c, dict) and "name" in c and "value" in c
        }
    else:
        log.error("❌ Неподдерживаемый формат FB_COOKIES_JSON (ожидался dict или list)")
        return None

    if not cookies:
        log.warning("⚠️ В FB_COOKIES_JSON нет ни одной cookies-пары")
        return None

    log.info(f"Cookies загружены. Ключи: {list(cookies.keys())}")
    return cookies


def create_fb_session(cookies: Optional[Dict[str, str]]) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": FB_USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        }
    )
    if cookies:
        s.cookies.update(cookies)
    return s


# ----------------- ГРУППЫ ИЗ МИНИАППА -----------------


def get_fb_groups() -> List[Dict]:
    """
    Забираем список групп из миниаппа: GET {API_BASE_URL}/api/groups.

    Формат ответа:
      {
        "groups": [
          {
            "id": ...,
            "group_id": "https://www.facebook.com/groups/ProjectAmazon",
            "group_name": "...",
            "enabled": true
          },
          ...
        ]
      }

    Здесь мы оставляем ТОЛЬКО активные facebook-группы:
      - enabled = true
      - group_id содержит facebook.com или fb.com
      - и не содержит t.me / telegram.me
    """
    if not API_BASE_URL:
        log.error("❌ API_BASE_URL не задан — не могу получить список групп.")
        return []

    url = f"{API_BASE_URL}/api/groups"
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
    enabled_groups = [g for g in all_groups if g.get("enabled")]

    fb_groups: List[Dict] = []
    skipped_non_fb: List[str] = []

    for g in enabled_groups:
        gid = (g.get("group_id") or "").strip()
        low = gid.lower()

        # Телеграм — пропускаем (их заберёт tg_parser)
        if "t.me/" in low or "telegram.me" in low:
            skipped_non_fb.append(gid)
            continue

        # Оставляем только URL/идентификаторы, где явно видно facebook / fb
        if "facebook.com" in low or "fb.com" in low:
            fb_groups.append(g)
        else:
            skipped_non_fb.append(gid)

    log.info(
        f"Всего групп из API: {len(all_groups)}; активных: {len(enabled_groups)}; facebook-групп: {len(fb_groups)}"
    )
    if skipped_non_fb:
        log.info(f"Пропущены не-facebook источники: {skipped_non_fb}")

    return fb_groups


# ----------------- УТИЛИТЫ -----------------


def normalize_group_link_to_mobile(group_link: str) -> str:
    """
    Делает из https://www.facebook.com/groups/ProjectAmazon
    → https://m.facebook.com/groups/ProjectAmazon
    """
    group_link = group_link.strip()
    if not group_link.startswith("http"):
        # если вдруг кто-то засунул просто ID/имя, соберём сами
        return f"https://m.facebook.com/groups/{group_link}"

    parsed = urlparse(group_link)
    host = parsed.netloc
    path = parsed.path or "/"

    # нормализуем хост на m.facebook.com
    if "facebook.com" in host and not host.startswith("m.facebook.com"):
        host = "m.facebook.com"

    mobile_url = f"https://{host}{path}"
    return mobile_url


def matches_keywords(text: str) -> bool:
    if not text:
        return False
    low = text.lower()
    return any(k in low for k in KEYWORDS)


# ----------------- ПАРСИНГ m.facebook.com -----------------


def fetch_group_html(session: requests.Session, mobile_url: str) -> Optional[str]:
    try:
        log.info(f"🔎 Загружаю мобильную группу: {mobile_url}")
        resp = session.get(mobile_url, timeout=30)
        resp.raise_for_status()
    except HTTPError as e:
        log.error(f"❌ HTTP ошибка при запросе {mobile_url}: {e}")
        return None
    except RequestException as e:
        log.error(f"❌ Ошибка сети при запросе {mobile_url}: {e}")
        return None

    return resp.text


def extract_posts_from_mobile_html(html: str, base_url: str) -> List[Tuple[str, Optional[str], Optional[datetime]]]:
    """
    Очень грубый мобильный парсер:

    - ищем блоки-посты по нескольким эвристикам:
        * article
        * div[data-ft][role=article]
        * div с id, похожим на "m_story"
    - собираем текст поста
    - пытаемся найти permalink
    - вытаскиваем время из abbr[data-utime] / span[data-utime]
    """
    soup = BeautifulSoup(html, "lxml")
    posts: List[Tuple[str, Optional[str], Optional[datetime]]] = []

    # 1) article
    containers = soup.find_all("article")

    # 2) div[data-ft][role=article]
    if not containers:
        containers = soup.find_all("div", attrs={"data-ft": True, "role": "article"})

    # 3) любые div с data-ft
    if not containers:
        containers = soup.find_all("div", attrs={"data-ft": True})

    # 4) последний шанс — div, id которых выглядит как story
    if not containers:
        containers = [
            d
            for d in soup.find_all("div")
            if (d.get("id") or "").startswith("m_story")
        ]

    if not containers:
        log.warning("⚠️ Не удалось найти контейнеры постов в мобильном HTML")
        return posts

    for block in containers[:POSTS_PER_GROUP]:
        text = block.get_text(" ", strip=True)
        if not text:
            continue

        # permalink
        post_url: Optional[str] = None
        for a in block.find_all("a", href=True):
            href = a["href"]
            if "story.php" in href or "/permalink/" in href or "/posts/" in href:
                post_url = urljoin(base_url, href.split("?", 1)[0])
                break

        # время
        created_at: Optional[datetime] = None
        abbr = block.find("abbr")
        if abbr and abbr.has_attr("data-utime"):
            try:
                ts = int(abbr["data-utime"])
                created_at = datetime.utcfromtimestamp(ts)
            except Exception:
                created_at = None
        else:
            span = block.find("span", attrs={"data-utime": True})
            if span:
                try:
                    ts = int(span["data-utime"])
                    created_at = datetime.utcfromtimestamp(ts)
                except Exception:
                    created_at = None

        posts.append((text, post_url, created_at))

    log.info(f"📄 Найдено {len(posts)} потенциальных постов в мобильном HTML")
    return posts


# ----------------- ОТПРАВКА ВАКАНСИЙ В МИНИАПП -----------------


def send_job_to_api(
    source_name: str,
    external_id: str,
    url: Optional[str],
    text: str,
    created_at: Optional[datetime],
) -> None:
    if not API_BASE_URL:
        log.error("❌ API_BASE_URL не задан — не могу отправить вакансию")
        return

    endpoint = f"{API_BASE_URL}/post"
    headers = {
        "Content-Type": "application/json",
    }
    if API_SECRET:
        headers["X-API-KEY"] = API_SECRET

    payload = {
        "source": "facebook",
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


# ----------------- ПАРСИНГ ОДНОЙ ГРУППЫ -----------------


def parse_one_group(
    session: requests.Session,
    group_link: str,
    group_name: str,
) -> int:
    """
    Возвращает количество отправленных вакансий.
    """
    mobile_url = normalize_group_link_to_mobile(group_link)
    log.info(f"🔍 Парсим группу: {group_name} ({group_link}) → {mobile_url}")

    html = fetch_group_html(session, mobile_url)
    if not html:
        return 0

    posts = extract_posts_from_mobile_html(html, base_url="https://m.facebook.com")
    sent = 0

    for text, post_url, created_at in posts:
        if not matches_keywords(text):
            continue

        # формируем external_id из источника + ссылки или куска текста
        base = group_link.split("?", 1)[0]
        ext = f"{base}|{post_url or text[:50]}"
        external_id = str(abs(hash(ext)))

        send_job_to_api(
            source_name=group_name or group_link,
            external_id=external_id,
            url=post_url,
            text=text,
            created_at=created_at,
        )
        sent += 1

    log.info(f"📦 Отправлено {sent} вакансий для группы {group_name}")
    return sent


# ----------------- ГЛАВНЫЙ ЦИКЛ -----------------


def run_once():
    if not API_BASE_URL:
        log.error("❌ API_BASE_URL не задан — останавливаю цикл")
        return

    log.info(f"API: {API_BASE_URL}")
    log.info(f"Ключевые слова: {KEYWORDS}")

    cookies = load_cookies()
    session = create_fb_session(cookies)

    groups = get_fb_groups()
    total_sent = 0

    for g in groups:
        group_link = g.get("group_id") or ""
        group_name = g.get("group_name") or group_link
        try:
            total_sent += parse_one_group(session, group_link, group_name)
            time.sleep(2)
        except Exception as e:
            log.error(f"❌ Неожиданная ошибка при парсинге {group_link}: {e}")

    log.info(f"✅ Цикл завершён. Всего отправлено вакансий: {total_sent}")


def main():
    log.info("🚀 Запуск Facebook Job Parser (мобильный m.facebook.com)")

    while True:
        try:
            run_once()
        except Exception as e:
            log.error(f"❌ Ошибка в основном цикле: {e}")
        log.info(f"⏳ Ожидание {CHECK_INTERVAL_MINUTES} минут...")
        time.sleep(CHECK_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    main()
