import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import logging
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

SPARQL_ENDPOINT  = "https://query.wikidata.org/sparql"
WIKIPEDIA_API    = "https://en.wikipedia.org/w/api.php"
SPARQL_PAGE_SIZE = 2000
SPARQL_SLEEP     = 5
WIKI_SLEEP       = 0.5
MAX_RETRIES      = 3

HEADERS = {
    "User-Agent": "IndianCinemaSongsScraper/1.0 (academic; contact@example.com)",
    "Accept":     "application/sparql-results+json",
}

# ── Language / country combos to query ───────────────────────────────────────

LANGUAGE_QIDS = {
    "Hindi":     "Q11051",
    "Tamil":     "Q5885",
    "Telugu":    "Q8097",
    "Malayalam": "Q36236",
    "Kannada":   "Q33673",
    "Bengali":   "Q9610",
    "Marathi":   "Q1571",
    "Punjabi":   "Q58635",
    "Urdu":      "Q9051",
    "Gujarati":  "Q5137",
    "Odia":      "Q33810",
}

# ── Step 1: Paginated SPARQL ──────────────────────────────────────────────────

def build_query(country_qid: str, lang_qid: str | None, limit: int, offset: int) -> str:
    lang_line = f"?film wdt:P364 wd:{lang_qid} ." if lang_qid else ""
    return f"""
SELECT DISTINCT ?filmLabel ?article WHERE {{
  ?film wdt:P31/wdt:P279* wd:Q11424 .
  ?film wdt:P495 wd:{country_qid} .
  {lang_line}

  ?article schema:about ?film ;
           schema:inLanguage "en" ;
           schema:isPartOf <https://en.wikipedia.org/> .

  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
  }}
}}
ORDER BY ?filmLabel
LIMIT {limit}
OFFSET {offset}
"""

def fetch_all_films() -> list[tuple[str, str]]:
    """Return list of (film_label, wiki_article_url) for all Indian/Pakistani films."""
    combos = [
        ("Q668", qid, f"Indian {lang}")
        for lang, qid in LANGUAGE_QIDS.items()
    ] + [("Q843", None, "Pakistani films")]

    seen: set[str] = set()
    all_films: list[tuple[str, str]] = []

    for country_qid, lang_qid, label in combos:
        log.info(f"\nFetching: {label}")
        offset = 0
        while True:
            query = build_query(country_qid, lang_qid, SPARQL_PAGE_SIZE, offset)
            try:
                resp = requests.get(
                    SPARQL_ENDPOINT,
                    params={"query": query, "format": "json"},
                    headers=HEADERS,
                    timeout=90,
                )
                resp.raise_for_status()
                bindings = resp.json()["results"]["bindings"]
            except Exception as e:
                log.warning(f"  SPARQL error at offset {offset}: {e}")
                break

            if not bindings:
                break

            for b in bindings:
                url   = b.get("article",   {}).get("value", "")
                label_val = b.get("filmLabel", {}).get("value", "")
                if url and url not in seen:
                    seen.add(url)
                    all_films.append((label_val, url))

            log.info(f"  offset={offset}  batch={len(bindings)}  total={len(all_films)}")

            if len(bindings) < SPARQL_PAGE_SIZE:
                break
            offset += SPARQL_PAGE_SIZE
            time.sleep(SPARQL_SLEEP)

    log.info(f"\nTotal unique films: {len(all_films):,}")
    return all_films


# ── Step 2: Wikipedia Fetcher ─────────────────────────────────────────────────

def wiki_title_from_url(url: str) -> str:
    return url.split("/wiki/")[-1].replace("_", " ")

def fetch_wiki_html(title: str) -> str | None:
    """Fetch parsed HTML for a Wikipedia article. Returns None on failure."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(
                WIKIPEDIA_API,
                params={
                    "action": "parse", "page": title,
                    "prop": "text", "formatversion": "2", "format": "json",
                },
                headers={"User-Agent": HEADERS["User-Agent"]},
                timeout=20,
            )
            if resp.status_code == 429:
                log.warning(f"  Rate limited — sleeping 30s")
                time.sleep(30)
                continue
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                return None
            return data["parse"]["text"]
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
    return None


# ── Step 3: Find soundtrack article links on the film page ───────────────────

# Patterns for how Wikipedia names soundtrack sub-articles
SOUNDTRACK_LINK_RE = re.compile(
    r"soundtrack|score|songs|music", re.IGNORECASE
)

def find_soundtrack_article(film_title: str, html: str) -> str | None:
    """
    Look for a link to a dedicated soundtrack article on the film page.
    e.g. "Dilwale Dulhania Le Jayenge (soundtrack)"
         "Lagaan (film score)"
         "3 Idiots § Soundtrack" → "3 Idiots (soundtrack)"
    """
    soup = BeautifulSoup(html, "html.parser")

    # Strategy A: look for wikilinks whose text mentions "soundtrack"
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        if (
            href.startswith("/wiki/")
            and ":" not in href
            and SOUNDTRACK_LINK_RE.search(text)
        ):
            candidate = wiki_title_from_url(href)
            # Make sure it's related to this film (avoid generic "Soundtrack" links)
            film_words = set(film_title.lower().split())
            if any(w in candidate.lower() for w in film_words if len(w) > 3):
                return candidate

    # Strategy B: look for links containing "(soundtrack)" or "(score)" in the href
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if SOUNDTRACK_LINK_RE.search(href) and href.startswith("/wiki/"):
            return wiki_title_from_url(a["href"])

    # Strategy C: guess the standard naming convention
    guesses = [
        f"{film_title} (soundtrack)",
        f"{film_title} (film score)",
        f"{film_title} (album)",
    ]
    return guesses   # return list of guesses to try


# ── Step 4: Parse song table from HTML ───────────────────────────────────────

SOUNDTRACK_SECTION_RE = re.compile(
    r"soundtrack|songs?|music|track\s*list|tracklist", re.IGNORECASE
)

COLUMN_MAP = {
    "no": "no", "no.": "no", "#": "no", "s.no": "no", "s.no.": "no",
    "title": "song", "song": "song", "song title": "song",
    "songs": "song", "name": "song", "track": "song", "track title": "song",
    "singer": "singer", "singers": "singer", "singer(s)": "singer",
    "vocals": "singer", "vocalist": "singer", "performed by": "singer",
    "sung by": "singer", "voice": "singer",
    "lyricist": "lyricist", "lyricists": "lyricist", "lyricist(s)": "lyricist",
    "lyrics": "lyricist", "lyrics by": "lyricist", "written by": "lyricist",
    "music": "composer", "composer": "composer", "music by": "composer",
    "composed by": "composer", "music director": "composer",
    "duration": "duration", "length": "duration", "time": "duration",
    "notes": "notes", "remarks": "notes",
}

KEEP_COLS = {"no", "song", "singer", "lyricist", "composer", "duration"}


def clean_cell(text: str) -> str:
    text = re.sub(r"\[[\w\s]+\]", "", text)   # [1] [note] [a]
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_songs_from_html(film_label: str, html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    songs = []

    # ── Try soundtrack section headings first ─────────────────────────────────
    for heading in soup.find_all(["h2", "h3", "h4"]):
        if not SOUNDTRACK_SECTION_RE.search(heading.get_text()):
            continue

        sibling = heading.find_next_sibling()
        while sibling:
            tag = sibling.name

            # Tables directly in the section
            if tag == "table":
                rows = _parse_table(sibling, film_label)
                songs.extend(rows)

            # Tables nested inside a div (common in Wikipedia's parsed HTML)
            elif tag == "div":
                for tbl in sibling.find_all("table"):
                    rows = _parse_table(tbl, film_label)
                    songs.extend(rows)

            # Bullet lists
            elif tag in ("ul", "ol"):
                rows = _parse_list(sibling, film_label)
                songs.extend(rows)

            # Stop at next same-level or higher heading
            elif tag in ("h2", "h3") and tag <= heading.name:
                break

            sibling = sibling.find_next_sibling()

        if songs:
            return songs

    # ── Fallback: any wikitable with a "song" or "title" column ──────────────
    for table in soup.find_all("table", class_=re.compile(r"wikitable")):
        th_texts = {th.get_text(strip=True).lower() for th in table.find_all("th")}
        if th_texts & {"song", "title", "song title", "track", "track title"}:
            rows = _parse_table(table, film_label)
            if rows:
                songs.extend(rows)
                return songs

    return songs


def _parse_table(table, film: str) -> list[dict]:
    rows = []
    headers = []

    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        texts = [clean_cell(c.get_text(separator=" ", strip=True)) for c in cells]

        # Detect header row: all <th>, or first row before any data
        if not headers and any(c.name == "th" for c in cells):
            headers = [COLUMN_MAP.get(t.lower(), t.lower()) for t in texts]
            continue

        # Skip rows that are entirely empty or single-cell colspan headers
        if not any(texts) or (len(texts) == 1 and cells[0].get("colspan")):
            continue

        if not headers:
            headers = [f"col_{i}" for i in range(len(texts))]

        record = {"film": film}
        for i, val in enumerate(texts):
            col = headers[i] if i < len(headers) else f"col_{i}"
            if col in KEEP_COLS and val:
                record[col] = val

        # Only keep rows that have at least a song title
        if record.get("song"):
            rows.append(record)

    return rows


def _parse_list(ul, film: str) -> list[dict]:
    rows = []
    for li in ul.find_all("li", recursive=False):
        text = clean_cell(li.get_text(separator=" ", strip=True))
        if text:
            rows.append({"film": film, "song": text})
    return rows


# ── Step 5: Main pipeline ─────────────────────────────────────────────────────

def scrape_film(film_label: str, article_url: str) -> list[dict]:
    """
    Try to get songs for a film by:
    1. Scraping the film's own Wikipedia page
    2. Finding a linked soundtrack article and scraping that
    3. Trying common naming conventions for soundtrack articles
    """
    wiki_title = wiki_title_from_url(article_url)

    # ── Attempt 1: film page itself ───────────────────────────────────────────
    html = fetch_wiki_html(wiki_title)
    if html:
        songs = parse_songs_from_html(film_label, html)
        if songs:
            return songs

        # ── Attempt 2: linked soundtrack article ──────────────────────────────
        result = find_soundtrack_article(wiki_title, html)
        candidates = result if isinstance(result, list) else [result]

        for candidate in candidates:
            if not candidate:
                continue
            soundtrack_html = fetch_wiki_html(candidate)
            if not soundtrack_html:
                continue
            songs = parse_songs_from_html(film_label, soundtrack_html)
            if songs:
                log.info(f"    → Found via soundtrack article: '{candidate}'")
                return songs
            time.sleep(WIKI_SLEEP)

    return []


def main():
    # ── Fetch film list via SPARQL ────────────────────────────────────────────
    films = fetch_all_films()

    if not films:
        log.error("No films returned from SPARQL. Check network / endpoint.")
        return

    # ── Scrape songs from Wikipedia ───────────────────────────────────────────
    all_songs: list[dict] = []
    no_songs:  list[str]  = []

    for film_label, article_url in tqdm(films, desc="Scraping films"):
        songs = scrape_film(film_label, article_url)
        if songs:
            all_songs.extend(songs)
        else:
            no_songs.append(film_label)
        time.sleep(WIKI_SLEEP)

    # ── Build DataFrame ───────────────────────────────────────────────────────
    if not all_songs:
        log.error("No songs collected.")
        return

    df = pd.DataFrame(all_songs)
    for col in ["film", "no", "song", "singer", "lyricist", "composer", "duration"]:
        if col not in df.columns:
            df[col] = ""
    df = df[["film", "no", "song", "singer", "lyricist", "composer", "duration"]]
    df = df.drop_duplicates().reset_index(drop=True)

    # ── Save outputs ──────────────────────────────────────────────────────────
    df.to_csv("film_songs.csv", index=False, encoding="utf-8-sig")

    if no_songs:
        pd.Series(no_songs).to_csv(
            "films_no_songs_found.csv", index=False, header=["film"]
        )

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info(f"\n{'='*50}")
    log.info(f"Total films scraped  : {len(films):,}")
    log.info(f"Films with songs     : {df['film'].nunique():,}")
    log.info(f"Films without songs  : {len(no_songs):,}")
    log.info(f"Total song records   : {len(df):,}")
    log.info(f"Unique songs         : {df['song'].nunique():,}")
    log.info(f"{'='*50}")
    log.info("Saved → film_songs.csv")
    log.info("Saved → films_no_songs_found.csv  (films to investigate manually)")

    print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
