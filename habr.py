# -*- coding: utf-8 -*-
"""
Асинхронный парсер статей из раздела «Статьи» профиля компании на Habr.

Что умеет:
- Принимает ссылку на профиль компании через -l / --link (как на профиль, так и сразу на /articles/)
- Автоматически нормализует ссылку: добавляет /articles/ если нужно, чинит лишние слэши
- Грузит все страницы пагинации и парсит статьи
- Генерирует современный HTML с Tailwind-дизайном:
  - тёмная/светлая тема (сохранение выбора в localStorage)
  - липкий заголовок таблицы
  - сортировка по клику на заголовок столбца (возрастание/убывание)
  - форматирование чисел с разделителями тысяч
  - экспорт видимой таблицы в CSV
- Безопасность: rel="noopener noreferrer" у ссылок target="_blank", кастомный User-Agent

Примеры запуска:
    python arenadata_dash.py -l https://habr.com/ru/companies/tantor/articles/
    python arenadata_dash.py -l https://habr.com/ru/companies/tantor/ -o tantor.html
    python arenadata_dash.py -l https://habr.com/ru/companies/beget/ -o beget.html --timeout 15
"""

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
import os
import argparse
from urllib.parse import urlparse, urlunparse

# Базовая часть домена Habr для сборки абсолютных ссылок на статьи
BASE_URL = "https://habr.com"

# Значения по умолчанию (могут быть переопределены через аргументы)
REQUEST_TIMEOUT_DEFAULT = 12
USER_AGENT = "Mozilla/5.0 (compatible; ArenadataScraper/1.2; +https://habr.com)"
OUTPUT_FILENAME_DEFAULT = "paser_habr.html"


def format_number(text: str) -> str:
    """
    Аккуратное форматирование числовых значений (просмотры/закладки/оценки),
    замена неразрывных пробелов и тримминг.
    """
    if not text:
        return "N/A"
    s = text.strip().replace("\xa0", " ")
    return s


async def fetch_html(session: aiohttp.ClientSession, url: str) -> str | None:
    """
    Асинхронно получает HTML по указанному URL.
    Возвращает строку HTML или None при ошибке/таймауте.
    """
    try:
        async with session.get(url, timeout=session.timeout) as resp:
            resp.raise_for_status()
            return await resp.text()
    except Exception as e:
        print(f"Fetch error {url}: {e}")
        return None


def parse_pagination_last_page(html: str) -> int:
    """
    Находит блок пагинации и извлекает номер последней страницы.
    Если пагинации нет или парсинг не удался — возвращает 1.
    """
    soup = BeautifulSoup(html, "html.parser")
    pagination_block = soup.find("div", class_="tm-pagination", attrs={"data-test-id": "pagination"})
    if not pagination_block:
        return 1
    page_links = pagination_block.find_all("a", class_="tm-pagination__page")
    if not page_links:
        return 1
    try:
        return int(page_links[-1].text.strip())
    except Exception:
        return 1


def parse_article_block(article_block) -> dict:
    """
    Парсит один блок статьи из списка:
    - заголовок и URL
    - автор
    - дата/время публикации
    - метрики: просмотры, оценки, комментарии, закладки
    Возвращает словарь с данными по статье.
    """
    # Заголовок + URL
    title_element = article_block.find("h2", class_="tm-title tm-title_h2", attrs={"data-test-id": "articleTitle"})
    link_element = article_block.find(
        "a",
        class_="tm-title__link",
        attrs={"data-article-link": "true", "data-test-id": "article-snippet-title-link"},
    )
    title = title_element.find("span").text.strip() if title_element and title_element.find("span") else "N/A"
    url = BASE_URL + link_element["href"] if link_element and link_element.get("href") else "N/A"

    # Автор
    author_element = article_block.find("span", class_="tm-user-info__user", attrs={"data-test-id": "user-info-description"})
    author_name = "N/A"
    if author_element:
        a_user = author_element.find("a", class_="tm-user-info__username")
        if a_user:
            author_name = a_user.text.strip()

    # Дата/время (атрибут datetime у <time> обычно ISO8601)
    time_element = article_block.find("time")
    date_str = "N/A"
    time_str = "N/A"
    if time_element and time_element.get("datetime"):
        iso = time_element.get("datetime").strip()
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y-%m-%d")
            time_str = dt.strftime("%H:%M")
        except Exception:
            date_str = iso[:10]
            try:
                time_str = iso.split("T")[1][:5]
            except Exception:
                time_str = "N/A"

    # Просмотры
    views_parent_span = article_block.find("span", class_="tm-icon-counter tm-data-icons__item")
    views_value = "N/A"
    if views_parent_span:
        vv = views_parent_span.find("span", class_="tm-icon-counter__value")
        if vv:
            views_value = format_number(vv.get("title") or vv.text)

    # Оценка (голоса)
    votes_element = article_block.find("div", class_="tm-votes-meter tm-data-icons__item")
    votes_value = "N/A"
    if votes_element:
        vm = votes_element.find("span", class_="tm-votes-meter__value", attrs={"data-test-id": "votes-meter-value"})
        if vm and vm.text:
            votes_value = vm.text.strip()

    # Комментарии
    comments_value = "N/A"
    comments_wrapper = article_block.find(
        "div",
        class_="article-comments-counter-link-wrapper tm-data-icons__item"
    )
    if comments_wrapper:
        span_val = comments_wrapper.find("span", class_="value")
        if span_val and span_val.text:
            comments_value = span_val.text.strip()

    # Закладки (избранное)
    bookmarks_value = "N/A"
    bookmarks_button = article_block.find("button", class_="bookmarks-button tm-data-icons__item")
    if bookmarks_button:
        counter_span = bookmarks_button.find("span", class_="bookmarks-button__counter")
        if counter_span:
            # Приоритет: текст числа; если нет — берем title
            text_val = counter_span.text.strip() if counter_span.text else ""
            title_val = (counter_span.get("title") or "").strip()
            bookmarks_value = format_number(text_val or title_val)

    return {
        "url": url,
        "title": title,
        "author": author_name,
        "date": date_str,
        "time": time_str,
        "votes": votes_value,
        "comments": comments_value,
        "bookmarks": bookmarks_value,
        "views": views_value,
    }


def parse_articles_list(html: str) -> list[dict]:
    """
    Парсит список статей на странице компании (раздел «Статьи»).
    Возвращает список словарей, каждый — результат parse_article_block.
    """
    soup = BeautifulSoup(html, "html.parser")
    article_blocks = soup.find_all("article", class_="tm-articles-list__item")
    results = []
    for block in article_blocks:
        try:
            results.append(parse_article_block(block))
        except Exception as e:
            print(f"Parse article error: {e}")
    return results


def render_html_table(rows: list[dict], source_url: str) -> str:
    """
    Генерирует итоговый HTML с современным дизайном (Tailwind) и интерактивными возможностями.
    - Тема (тёмная/светлая) с переключателем и сохранением в localStorage
    - Липкий заголовок таблицы
    - Сортировка по столбцам (возрастание/убывание)
    - Форматирование чисел с пробелами как разделителями тысяч
    - Экспорт текущего вида таблицы в CSV
    """
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Генерация строк таблицы. Числовые поля получают класс .numeric для форматирования и сортировки.
    table_rows = []
    for r in rows:
        table_rows.append(
            f"""
            <tr class="hover:bg-gray-50 dark:hover:bg-gray-800">
                <td class="py-3 px-4"><a class="text-primary-DEFAULT hover:text-primary-hover" href="{r['url']}" target="_blank" rel="noopener noreferrer">{r['url']}</a></td>
                <td class="py-3 px-4 text-text-light dark:text-text-dark">{r['title']}</td>
                <td class="py-3 px-4">{r['author']}</td>
                <td class="py-3 px-4">{r['date']}</td>
                <td class="py-3 px-4">{r['time']}</td>
                <td class="py-3 px-4 numeric">{r['votes']}</td>
                <td class="py-3 px-4 numeric">{r['comments']}</td>
                <td class="py-3 px-4 numeric">{r['bookmarks']}</td>
                <td class="py-3 px-4 numeric">{r['views']}</td>
            </tr>"""
        )
    rows_html = "\n".join(table_rows)

    # Полный HTML с Tailwind и JS для сортировки/темы/CSV
    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>Статьи компании (Habr)</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
<script src="https://cdn.tailwindcss.com?plugins=forms,typography"></script>
<script>
  tailwind.config = {{
    darkMode: "class",
    theme: {{
      extend: {{
        colors: {{
          primary: {{ DEFAULT: "#4F46E5", hover: "#4338CA" }},
          "background-light": "#F9FAFB",
          "background-dark": "#0B1220",
          "card-light": "#FFFFFF",
          "card-dark": "#111827",
          "text-light": "#111827",
          "text-dark": "#F9FAFB",
          "subtext-light": "#6B7280",
          "subtext-dark": "#9CA3AF",
          "border-light": "#E5E7EB",
          "border-dark": "#1F2A44"
        }},
        fontFamily: {{
          sans: ['Inter', 'system-ui', 'Segoe UI', 'Roboto', 'sans-serif'],
        }},
        boxShadow: {{
          subtle: "0 1px 2px rgba(0,0,0,0.06)"
        }}
      }},
    }},
  }};
</script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
  html {{ scroll-behavior: smooth; }}
  body {{ font-family: 'Inter', sans-serif; }}
  .numeric {{ font-variant-numeric: tabular-nums; }}
  thead th.sticky {{ position: sticky; top: 0; z-index: 10; }}
</style>
</head>
<body class="bg-background-light dark:bg-background-dark text-text-light dark:text-text-dark">
<div class="min-h-screen p-4 sm:p-6 lg:p-8">
  <div class="max-w-7xl mx-auto">
    <div class="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6 gap-3">
      <div>
        <h1 class="text-2xl font-bold">Статьи компании (Habr)</h1>
        <p class="text-sm text-subtext-light dark:text-subtext-dark mt-1">
          Сгенерировано: {generated_at}. Источник:
          <a class="text-primary-DEFAULT hover:text-primary-hover underline decoration-2" href="{source_url}" target="_blank" rel="noopener noreferrer">{source_url}</a>
        </p>
      </div>
      <div class="flex items-center gap-2">
        <button id="toggleTheme" class="flex items-center bg-card-light dark:bg-card-dark text-text-light dark:text-text-dark border border-border-light dark:border-border-dark shadow-subtle py-2 px-3 rounded-md">
          <span class="material-icons mr-2">dark_mode</span>
          Тема
        </button>
        <button id="downloadCsv" class="flex items-center bg-primary-DEFAULT hover:bg-primary-hover text-white font-medium py-2 px-4 rounded-md shadow-subtle">
          <span class="material-icons mr-2">download</span>
          Скачать CSV
        </button>
      </div>
    </div>

    <div class="bg-card-light dark:bg-card-dark rounded-lg shadow-subtle border border-border-light dark:border-border-dark overflow-hidden">
      <div class="overflow-x-auto">
        <table id="articlesTable" class="w-full text-sm text-left">
          <thead class="text-xs uppercase bg-background-light dark:bg-background-dark border-b border-border-light dark:border-border-dark">
            <tr>
              <th class="py-3 px-4 sticky cursor-pointer select-none" scope="col" data-sort-key="url">URL <span class="material-icons align-middle text-base opacity-50">unfold_more</span></th>
              <th class="py-3 px-4 sticky cursor-pointer select-none" scope="col" data-sort-key="title">Название <span class="material-icons align-middle text-base opacity-50">unfold_more</span></th>
              <th class="py-3 px-4 sticky cursor-pointer select-none" scope="col" data-sort-key="author">Автор <span class="material-icons align-middle text-base opacity-50">unfold_more</span></th>
              <th class="py-3 px-4 sticky cursor-pointer select-none" scope="col" data-sort-key="date">Дата публикации <span class="material-icons align-middle text-base opacity-50">unfold_more</span></th>
              <th class="py-3 px-4 sticky cursor-pointer select-none" scope="col" data-sort-key="time">Время публикации <span class="material-icons align-middle text-base opacity-50">unfold_more</span></th>
              <th class="py-3 px-4 sticky cursor-pointer select-none" scope="col" data-sort-key="votes">Оценка <span class="material-icons align-middle text-base opacity-50">unfold_more</span></th>
              <th class="py-3 px-4 sticky cursor-pointer select-none whitespace-nowrap" scope="col" data-sort-key="comments">Комментарии <span class="material-icons align-middle text-base opacity-50">unfold_more</span></th>
              <th class="py-3 px-4 sticky cursor-pointer select-none whitespace-nowrap" scope="col" data-sort-key="bookmarks">Избранное <span class="material-icons align-middle text-base opacity-50">unfold_more</span></th>
              <th class="py-3 px-4 sticky cursor-pointer select-none" scope="col" data-sort-key="views">Просмотры <span class="material-icons align-middle text-base opacity-50">unfold_more</span></th>
            </tr>
          </thead>
          <tbody class="divide-y divide-border-light dark:divide-border-dark">
            {rows_html}
          </tbody>
        </table>
      </div>
      <div class="flex justify-between items-center px-4 py-3 bg-background-light dark:bg-background-dark border-t border-border-light dark:border-border-dark">
        <span class="text-sm text-subtext-light dark:text-subtext-dark">Всего статей: {len(rows)}</span>
      </div>
    </div>
  </div>
</div>

<script>
  // Тема: сохраняем выбор пользователя
  const root = document.documentElement;
  const themeKey = 'habr-company-theme';
  const toggleBtn = document.getElementById('toggleTheme');
  const applyStoredTheme = () => {{
    const stored = localStorage.getItem(themeKey);
    if (stored === 'dark') {{
      root.classList.add('dark');
    }} else {{
      root.classList.remove('dark');
    }}
  }};
  applyStoredTheme();
  toggleBtn.addEventListener('click', () => {{
    if (root.classList.contains('dark')) {{
      root.classList.remove('dark');
      localStorage.setItem(themeKey, 'light');
    }} else {{
      root.classList.add('dark');
      localStorage.setItem(themeKey, 'dark');
    }}
  }});

  // Форматирование чисел (1 234 567) — пробелы как разделители тысяч
  const formatNumber = (n) => {{
    const num = Number(String(n).replace(/[^\\d-]/g, ''));
    if (isNaN(num)) return n;
    return num.toString().replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, ' ');
  }};
  document.querySelectorAll('#articlesTable td.numeric').forEach(td => {{
    td.textContent = formatNumber(td.textContent);
  }});

  // Сортировка: клики по заголовкам с data-sort-key
  const table = document.getElementById('articlesTable');
  const tbody = table.querySelector('tbody');
  const headers = table.querySelectorAll('thead th[data-sort-key]');

  let currentSort = {{ key: null, dir: 'asc' }}; // dir: asc | desc

  const getCellValue = (row, key) => {{
    const idx = Array.from(headers).findIndex(h => h.dataset.sortKey === key);
    if (idx < 0) return '';
    const cell = row.children[idx];
    // Для URL: сортируем по href
    if (key === 'url') {{
      const a = cell.querySelector('a');
      return a ? a.getAttribute('href') : cell.textContent.trim();
    }}
    const raw = cell.textContent.trim();
    // Числа: убираем плюсы и пробелы
    if (['votes','comments','bookmarks','views'].includes(key)) {{
      const cleaned = raw.replace(/[^\\d-]/g, '');
      const num = Number(cleaned);
      return isNaN(num) ? -Infinity : num;
    }}
    // Дата/время как строки (при желании можно превратить в timestamp)
    return raw.toLowerCase();
  }};

  const updateSortIcon = (clickedHeader) => {{
    headers.forEach(h => {{
      const icon = h.querySelector('.material-icons');
      if (icon) icon.textContent = 'unfold_more';
    }});
    const icon = clickedHeader.querySelector('.material-icons');
    if (!icon) return;
    icon.textContent = currentSort.dir === 'asc' ? 'expand_less' : 'expand_more';
  }};

  const sortRows = (key) => {{
    const rows = Array.from(tbody.querySelectorAll('tr'));
    // Переключаем направление
    if (currentSort.key === key) {{
      currentSort.dir = currentSort.dir === 'asc' ? 'desc' : 'asc';
    }} else {{
      currentSort.key = key;
      currentSort.dir = 'asc';
    }}
    const dir = currentSort.dir === 'asc' ? 1 : -1;
    rows.sort((a, b) => {{
      const va = getCellValue(a, key);
      const vb = getCellValue(b, key);
      if (va < vb) return -1 * dir;
      if (va > vb) return 1 * dir;
      return 0;
    }});
    // Перерисовка
    const frag = document.createDocumentFragment();
    rows.forEach(r => frag.appendChild(r));
    tbody.innerHTML = '';
    tbody.appendChild(frag);
  }};

  headers.forEach(h => {{
    h.addEventListener('click', () => {{
      sortRows(h.dataset.sortKey);
      updateSortIcon(h);
    }});
  }});

  // Экспорт CSV (из текущего видимого порядка)
  const toCsv = () => {{
    const rows = Array.from(table.querySelectorAll('tbody tr'));
    const headCells = Array.from(table.querySelectorAll('thead th')).map(th => th.childNodes[0].textContent.trim());
    const csvLines = [headCells.join(';')];
    rows.forEach(tr => {{
      const cells = Array.from(tr.children).map(td => {{
        const a = td.querySelector('a');
        const val = a ? a.getAttribute('href') : td.textContent.trim();
        const safe = String(val).replace(/"/g, '""');
        return `"{{safe}}"`;
      }});
      csvLines.push(cells.join(';'));
    }});
    return csvLines.join('\\n');
  }};

  document.getElementById('downloadCsv').addEventListener('click', () => {{
    const csv = toCsv();
    const blob = new Blob([csv], {{ type: 'text/csv;charset=utf-8;' }});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'habr_company_articles.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }});
</script>
</body>
</html>"""
    return html


def normalize_company_articles_url(raw_url: str) -> str:
    """
    Нормализует переданную ссылку:
    - гарантирует схему https
    - гарантирует домен habr.com
    - приводит к виду: https://habr.com/ru/companies/<slug>/articles/
      Если пользователь дал только ссылку на профиль компании (без /articles/), добавим /articles/.
    Также аккуратно убираем лишние слэши.

    Важное замечание: мы не блокируем иные пути на habr.com, но именно ожидаемая структура
    /ru/companies/<slug>/articles/ даёт корректный список статей компании.
    """
    if not raw_url:
        raise ValueError("Не передана ссылка на профиль компании (-l/--link).")

    parsed = urlparse(raw_url.strip())
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or "habr.com"

    # Сборка пути и очистка лишних слэшей
    path = parsed.path or "/"
    path = "/" + "/".join([p for p in path.split("/") if p])

    # Если передана общая ссылка на компанию, добавим /articles/
    parts = path.strip("/").split("/")
    # Ищем сегмент 'companies' и следующий slug
    if len(parts) >= 3 and parts[1] == "companies":
        # parts: ['ru', 'companies', '<slug>', ...]
        if "articles" not in parts:
            path = "/" + "/".join(parts[:3] + ["articles"]) + "/"
        else:
            if not path.endswith("/"):
                path = path + "/"
    else:
        # Иная структура — оставляем как есть, завершаем слэшем для однозначности
        if not path.endswith("/"):
            path = path + "/"

    normalized = urlunparse((scheme, netloc, path, "", "", ""))
    return normalized


async def scrape_company_articles(articles_url: str, request_timeout: int) -> list[dict]:
    """
    Загружает первую страницу раздела «Статьи» компании, определяет количество страниц,
    асинхронно загружает остальные и парсит все статьи, возвращая список словарей.
    """
    headers = {"User-Agent": USER_AGENT}
    timeout = aiohttp.ClientTimeout(total=request_timeout)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        first_page_html = await fetch_html(session, articles_url)
        if not first_page_html:
            return []

        last_page = parse_pagination_last_page(first_page_html)
        rows = parse_articles_list(first_page_html)

        if last_page > 1:
            tasks = []
            for page_num in range(2, last_page + 1):
                # Пагинация на Habr: .../articles/pageN/
                url = f"{articles_url}page{page_num}/"
                tasks.append(fetch_html(session, url))
            pages_html = await asyncio.gather(*tasks)
            for html in pages_html:
                if html:
                    rows.extend(parse_articles_list(html))
                # Небольшая пауза между парсингом страниц для снижения нагрузки
                await asyncio.sleep(0.01)

        return rows


def parse_args() -> argparse.Namespace:
    """
    Определяет и парсит аргументы командной строки.
    -l / --link: ссылка на профиль компании или непосредственно на раздел «Статьи»
    -o / --output: имя выходного HTML-файла (по умолчанию arenadata_habr.html)
    --timeout: таймаут запросов в секундах (по умолчанию 12)
    """
    parser = argparse.ArgumentParser(
        description="Парсер статей из раздела «Статьи» профилей компаний Habr."
    )
    parser.add_argument(
        "-l", "--link",
        required=True,
        help="Ссылка на профиль компании Habr (например, https://habr.com/ru/companies/tantor/ или .../articles/)"
    )
    parser.add_argument(
        "-o", "--output",
        default=OUTPUT_FILENAME_DEFAULT,
        help=f"Имя выходного HTML-файла (по умолчанию {OUTPUT_FILENAME_DEFAULT})"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=REQUEST_TIMEOUT_DEFAULT,
        help=f"Таймаут HTTP-запросов в секундах (по умолчанию {REQUEST_TIMEOUT_DEFAULT})"
    )
    return parser.parse_args()


async def main_async() -> None:
    """
    Асинхронная точка входа:
    - парсим аргументы
    - нормализуем ссылку (добавим /articles/ при необходимости)
    - парсим статьи
    - рендерим и сохраняем HTML с улучшенным дизайном
    """
    args = parse_args()
    try:
        articles_url = normalize_company_articles_url(args.link)
    except Exception as e:
        print(f"Ошибка нормализации ссылки: {e}")
        return

    rows = await scrape_company_articles(articles_url, args.timeout)
    html = render_html_table(rows, source_url=articles_url)

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.output)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML сохранен: {out_path} (строк: {len(rows)})")


def main() -> None:
    """
    Синхронная точка входа, запускает асинхронную часть.
    """
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
