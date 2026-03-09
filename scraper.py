"""
Hybrid Parallel Website Scraper for RAG Ingestion
==================================================
Handles BOTH static HTML and JS-rendered / SPA sites (React, Vue, Angular, etc.)
using a smart two-tier strategy:

  Tier 1 — requests + BeautifulSoup (fast, low overhead)
      Used for pages where the static HTML contains enough meaningful content.

  Tier 2 — Playwright headless Chromium (full JS execution)
      Automatically triggered when Tier 1 detects thin/empty content, JS framework
      markers, or explicit --js-mode flag is set.

Designed for large technical docs (Azure, VMware, OpenShift) with:
  • Thread-safe BFS crawl with proper locking
  • Sitemap-first URL discovery (robots.txt → sitemap index → all child sitemaps)
  • Retry with exponential backoff + rate-limit handling
  • Streaming JSONL output (no unbounded RAM growth)
  • RAG-ready rag_chunk field per page
  • Per-page JS detection heuristics (no wasted browser launches)
  • Playwright browser pool — reuses browser contexts across pages

Install deps:
    pip install requests beautifulsoup4 goose3 langdetect tldextract lxml playwright
    playwright install chromium

Usage:
    # Auto-detect JS pages (recommended)
    python app.py --url https://learn.microsoft.com/en-us/azure/ --output azure.jsonl

    # Force JS rendering for every page (slower but thorough for full SPAs)
    python app.py --url https://docs.vmware.com/ --output vmware.jsonl --js-mode always

    # Pure static mode (fastest, no Playwright needed)
    python app.py --url https://docs.openshift.com/ --output openshift.jsonl --js-mode never
"""

import argparse
import json
import logging
import queue
import random
import socket
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from urllib.parse import urljoin, urlparse, urlunparse

import requests
import tldextract
from bs4 import BeautifulSoup
from goose3 import Goose
from langdetect import detect, DetectorFactory

# ── Reproducible language detection ──────────────────────────────────────────
DetectorFactory.seed = 0

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

INDIAN_LANG_CODES = {"hi", "bn", "te", "mr", "ta", "gu", "kn", "ml", "pa", "ur"}

SKIP_EXTENSIONS = {
    ".pdf", ".zip", ".tar", ".gz", ".exe", ".dmg", ".pkg",
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".webp",
    ".mp4", ".mp3", ".avi", ".mov", ".woff", ".woff2", ".ttf",
    ".css", ".js", ".xml", ".json", ".csv", ".xlsx", ".docx",
}

# JS framework fingerprints in raw HTML source -> page needs JS rendering
JS_FRAMEWORK_MARKERS = [
    "data-reactroot", "data-reactid",          # React
    "ng-version", "ng-app", "ng-controller",   # Angular
    "__vue__", "data-v-",                      # Vue
    "data-svelte",                             # Svelte
    "__NEXT_DATA__",                           # Next.js
    "__NUXT__",                                # Nuxt
    "window.__INITIAL_STATE__",                # Generic SSR hydration
    "createApp(", "app.component(",            # Vue 3
    "<app-root>", "<app-component>",           # Angular shell
]

MAX_RETRIES = 3
RETRY_BACKOFF = 2.0
REQUEST_TIMEOUT = 20
CRAWL_DELAY = 0.05

# Playwright settings
PW_TIMEOUT_MS = 30_000
PW_WAIT_UNTIL = "networkidle"

# Minimum text to consider a static fetch successful; below -> try JS
MIN_CONTENT_LENGTH = 200

# Concurrent Playwright browser contexts
BROWSER_POOL_SIZE = 4


# ══════════════════════════════════════════════════════════════════════════════
# URL Utilities
# ══════════════════════════════════════════════════════════════════════════════

def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        fragment="",
    ))


def is_internal_link(base_url: str, link: str) -> bool:
    base = tldextract.extract(base_url)
    target = tldextract.extract(link)
    return base.domain == target.domain and base.suffix == target.suffix


def should_skip(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in SKIP_EXTENSIONS)


# ══════════════════════════════════════════════════════════════════════════════
# JS Detection
# ══════════════════════════════════════════════════════════════════════════════

def needs_js_rendering(raw_html: str, extracted_text: str) -> bool:
    """
    Returns True if the page appears to be JS-rendered and needs Playwright.
    Checks:
      1. Known JS framework fingerprints in raw HTML
      2. Very thin extracted text combined with heavy script usage
    """
    html_sample = raw_html[:50_000]

    for marker in JS_FRAMEWORK_MARKERS:
        if marker in html_sample:
            return True

    if len(extracted_text.strip()) < MIN_CONTENT_LENGTH:
        if html_sample.count("<script") >= 3:
            return True

    return False


# ══════════════════════════════════════════════════════════════════════════════
# Playwright Browser Pool
# ══════════════════════════════════════════════════════════════════════════════

class PlaywrightPool:
    """
    Fixed-size pool of Playwright browser contexts.
    Worker threads check out a context, render one page, then return it.
    Lazy-initialized on first JS page; shut down explicitly after crawl.
    """

    def __init__(self, pool_size: int = BROWSER_POOL_SIZE):
        self._pool_size = pool_size
        self._lock = threading.Lock()
        self._available: queue.Queue = queue.Queue()
        self._initialized = False
        self._browser = None
        self._pw_ctx = None

    def _initialize(self):
        with self._lock:
            if self._initialized:
                return
            try:
                from playwright.sync_api import sync_playwright
                self._pw_ctx = sync_playwright().__enter__()
                self._browser = self._pw_ctx.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--disable-extensions",
                        "--blink-settings=imagesEnabled=false",
                    ],
                )
                for _ in range(self._pool_size):
                    ctx = self._browser.new_context(
                        user_agent=random.choice(USER_AGENTS),
                        java_script_enabled=True,
                        ignore_https_errors=True,
                        viewport={"width": 1280, "height": 900},
                    )
                    self._available.put(ctx)
                self._initialized = True
                log.info(f"Playwright pool ready ({self._pool_size} contexts)")
            except ImportError:
                log.error(
                    "Playwright not installed.\n"
                    "Run: pip install playwright && playwright install chromium"
                )
                raise

    @contextmanager
    def acquire(self):
        """Borrow a browser context; automatically returned after use."""
        self._initialize()
        ctx = self._available.get(timeout=60)
        try:
            yield ctx
        finally:
            self._available.put(ctx)

    def shutdown(self):
        if self._initialized and self._browser:
            try:
                self._browser.close()
                self._pw_ctx.__exit__(None, None, None)
                log.info("Playwright pool shut down")
            except Exception as e:
                log.debug(f"Playwright shutdown error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Sitemap Discovery
# ══════════════════════════════════════════════════════════════════════════════

def fetch_sitemap_urls(base_url: str, session: requests.Session) -> list:
    found = []
    sitemap_roots = []

    try:
        resp = session.get(urljoin(base_url, "/robots.txt"), timeout=10)
        if resp.ok:
            for line in resp.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    sitemap_roots.append(line.split(":", 1)[1].strip())
    except Exception:
        pass

    if not sitemap_roots:
        for path in ["/sitemap.xml", "/sitemap_index.xml", "/sitemap/sitemap.xml"]:
            sitemap_roots.append(urljoin(base_url, path))

    visited_sitemaps = set()

    def parse_sitemap(sm_url):
        if sm_url in visited_sitemaps:
            return
        visited_sitemaps.add(sm_url)
        try:
            resp = session.get(sm_url, timeout=10)
            if not resp.ok:
                return
            soup = BeautifulSoup(resp.content, "lxml-xml")
            for loc in soup.find_all("sitemap"):
                child = loc.find("loc")
                if child:
                    parse_sitemap(child.text.strip())
            for loc in soup.find_all("url"):
                loc_tag = loc.find("loc")
                if loc_tag:
                    found.append(normalize_url(loc_tag.text.strip()))
        except Exception as e:
            log.debug(f"Sitemap parse failed {sm_url}: {e}")

    for root in sitemap_roots:
        parse_sitemap(root)

    log.info(f"Sitemap discovery: {len(found)} URLs found")
    return found


# ══════════════════════════════════════════════════════════════════════════════
# Shared HTTP Session
# ══════════════════════════════════════════════════════════════════════════════

def _make_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=200,
        pool_maxsize=200,
        max_retries=0,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_goose = Goose()
_goose_lock = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
# Content Extraction
# ══════════════════════════════════════════════════════════════════════════════

def extract_content(html: str, page_url: str, base_url: str):
    """Returns (title, clean_text, internal_links, image_urls)."""
    soup = BeautifulSoup(html, "lxml")

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    with _goose_lock:
        article = _goose.extract(raw_html=html)
    clean_text = article.cleaned_text or ""

    if len(clean_text.strip()) < MIN_CONTENT_LENGTH:
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        clean_text = soup.get_text(separator="\n", strip=True)

    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href and not href.startswith(("javascript:", "mailto:", "tel:", "#")):
            abs_link = normalize_url(urljoin(page_url, href))
            if is_internal_link(base_url, abs_link) and not should_skip(abs_link):
                links.append(abs_link)

    images = [urljoin(page_url, img["src"]) for img in soup.find_all("img", src=True)]
    return title, clean_text, links, images


def detect_language(text: str) -> str:
    try:
        return detect(text[:2000]) if text.strip() else "unknown"
    except Exception:
        return "unknown"


def get_ip(url: str) -> str:
    try:
        return socket.gethostbyname(urlparse(url).hostname)
    except Exception:
        return "N/A"


def build_result(url, depth, title, clean_text, links, images,
                 status_code, elapsed, content_length, headers, rendered_by):
    lang = detect_language(clean_text)
    return {
        "url": url,
        "title": title,
        "content": clean_text,
        "rag_chunk": f"[SOURCE: {url}]\n[TITLE: {title}]\n\n{clean_text}",
        "depth": depth,
        "language": lang,
        "is_indian_language": lang in INDIAN_LANG_CODES,
        "rendered_by": rendered_by,
        "status_code": status_code,
        "response_time_s": elapsed,
        "content_length": content_length,
        "server": headers.get("Server", "N/A"),
        "last_modified": headers.get("Last-Modified", "N/A"),
        "cache_control": headers.get("Cache-Control", "N/A"),
        "etag": headers.get("ETag", "N/A"),
        "ip_address": get_ip(url),
        "links": links,
        "images": images,
        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Page Scrapers
# ══════════════════════════════════════════════════════════════════════════════

def scrape_static(url, depth, session, base_url):
    """
    Returns (result_or_None, needs_js_bool).
    needs_js=True tells the caller to retry with Playwright.
    """
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            t0 = time.time()
            resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            elapsed = round(time.time() - t0, 4)

            if "text/html" not in resp.headers.get("Content-Type", ""):
                return None, False

            if resp.status_code == 429:
                wait = RETRY_BACKOFF * (2 ** attempt)
                log.warning(f"Rate limited {url}, retrying in {wait}s")
                time.sleep(wait)
                continue

            if resp.status_code >= 400:
                return None, False

            title, clean_text, links, images = extract_content(resp.text, url, base_url)

            if needs_js_rendering(resp.text, clean_text):
                log.debug(f"JS detected: {url}")
                return None, True

            result = build_result(
                url=url, depth=depth, title=title, clean_text=clean_text,
                links=links, images=images, status_code=resp.status_code,
                elapsed=elapsed, content_length=len(resp.content),
                headers=dict(resp.headers), rendered_by="static",
            )
            return result, False

        except requests.exceptions.Timeout:
            last_error = f"Timeout (attempt {attempt})"
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error: {e}"
            break
        except Exception as e:
            last_error = f"Error: {e}"
            break

        time.sleep(RETRY_BACKOFF * attempt)

    log.warning(f"Static scrape failed {url}: {last_error}")
    return None, False


def scrape_with_playwright(url, depth, base_url, pw_pool):
    """Render page in headless Chromium and extract fully rendered HTML."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with pw_pool.acquire() as ctx:
                page = ctx.new_page()
                try:
                    t0 = time.time()
                    response = page.goto(url, wait_until=PW_WAIT_UNTIL, timeout=PW_TIMEOUT_MS)
                    elapsed = round(time.time() - t0, 4)

                    if response is None or response.status >= 400:
                        return None

                    # Wait for lazy-loaded content
                    page.wait_for_load_state("networkidle", timeout=10_000)

                    # Scroll to trigger any scroll-activated content
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(800)

                    html = page.content()
                    status = response.status
                    resp_headers = dict(response.headers)
                finally:
                    page.close()

            title, clean_text, links, images = extract_content(html, url, base_url)
            return build_result(
                url=url, depth=depth, title=title, clean_text=clean_text,
                links=links, images=images, status_code=status,
                elapsed=elapsed, content_length=len(html.encode()),
                headers=resp_headers, rendered_by="playwright",
            )

        except Exception as e:
            log.warning(f"Playwright attempt {attempt} failed {url}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)

    return None


def scrape_page(url, depth, session, base_url, js_mode, pw_pool):
    """
    Unified entry point. Routes to static or Playwright based on js_mode.

    js_mode:
      "auto"   — static first; auto-fallback to Playwright if JS detected
      "always" — always use Playwright (for known SPAs like VMware docs)
      "never"  — always use static only (fastest)
    """
    time.sleep(CRAWL_DELAY)

    if js_mode == "always":
        return scrape_with_playwright(url, depth, base_url, pw_pool)

    if js_mode == "never":
        result, _ = scrape_static(url, depth, session, base_url)
        return result

    # "auto": try static, detect JS, fallback
    result, needs_js = scrape_static(url, depth, session, base_url)
    if result is not None:
        return result
    if needs_js:
        log.info(f"[JS fallback] {url}")
        return scrape_with_playwright(url, depth, base_url, pw_pool)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Thread-Safe BFS Crawler
# ══════════════════════════════════════════════════════════════════════════════

class ThreadSafeCrawler:
    def __init__(self, base_url, output_path, max_depth=4, max_workers=20,
                 max_pages=None, use_sitemap=True, js_mode="auto", pw_pool_size=BROWSER_POOL_SIZE):
        self.base_url = normalize_url(base_url)
        self.output_path = output_path
        self.max_depth = max_depth
        self.max_workers = max_workers
        self.max_pages = max_pages
        self.use_sitemap = use_sitemap
        self.js_mode = js_mode

        self.visited = set()
        self.visited_lock = threading.Lock()

        self.queue = deque()
        self.queue_lock = threading.Lock()

        self.results_lock = threading.Lock()
        self.pages_scraped = 0
        self.pages_failed = 0
        self.js_rendered = 0

        self.session = _make_session()
        self.pw_pool = PlaywrightPool(pool_size=pw_pool_size)

    def _mark_visited(self, url):
        with self.visited_lock:
            if url in self.visited:
                return False
            self.visited.add(url)
            return True

    def _enqueue(self, url, depth):
        with self.visited_lock:
            if url in self.visited:
                return
        with self.queue_lock:
            self.queue.append((url, depth))

    def _dequeue_batch(self, n):
        batch = []
        with self.queue_lock:
            for _ in range(min(n, len(self.queue))):
                batch.append(self.queue.popleft())
        return batch

    def _write_result(self, result, fh):
        links = result.pop("links", [])
        with self.results_lock:
            fh.write(json.dumps(result, ensure_ascii=False) + "\n")
            fh.flush()
            self.pages_scraped += 1
            if result.get("rendered_by") == "playwright":
                self.js_rendered += 1
        return links

    def run(self):
        log.info("=" * 60)
        log.info(f"Crawl start: {self.base_url}")
        log.info(f"  depth={self.max_depth} | workers={self.max_workers} | js_mode={self.js_mode}")
        log.info("=" * 60)

        self._enqueue(self.base_url, 1)

        if self.use_sitemap:
            for u in fetch_sitemap_urls(self.base_url, self.session):
                if is_internal_link(self.base_url, u) and not should_skip(u):
                    self._enqueue(u, 1)
            log.info(f"Queue seeded: {len(self.queue)} URLs")

        try:
            with open(self.output_path, "w", encoding="utf-8") as fh, \
                 ThreadPoolExecutor(max_workers=self.max_workers) as executor:

                active_futures = set()

                def submit_batch():
                    slots = self.max_workers - len(active_futures)
                    if slots <= 0:
                        return
                    for url, depth in self._dequeue_batch(slots * 2):
                        if not self._mark_visited(url):
                            continue
                        if self.max_pages and self.pages_scraped >= self.max_pages:
                            break
                        f = executor.submit(
                            scrape_page, url, depth, self.session,
                            self.base_url, self.js_mode, self.pw_pool,
                        )
                        active_futures.add(f)

                submit_batch()

                while active_futures:
                    done = {f for f in active_futures if f.done()}

                    if not done:
                        time.sleep(0.05)
                        submit_batch()
                        continue

                    for future in done:
                        active_futures.discard(future)
                        try:
                            result = future.result()
                        except Exception as e:
                            log.error(f"Future error: {e}")
                            with self.results_lock:
                                self.pages_failed += 1
                            continue

                        if result is None:
                            with self.results_lock:
                                self.pages_failed += 1
                            continue

                        depth = result["depth"]
                        links = self._write_result(result, fh)

                        total = self.pages_scraped + self.pages_failed
                        if total % 50 == 0:
                            log.info(
                                f"  scraped={self.pages_scraped} (js={self.js_rendered})"
                                f" | failed={self.pages_failed} | queued={len(self.queue)}"
                            )

                        if depth < self.max_depth:
                            for link in links:
                                self._enqueue(link, depth + 1)

                        if self.max_pages and self.pages_scraped >= self.max_pages:
                            log.info(f"max_pages limit ({self.max_pages}) reached.")
                            executor.shutdown(wait=False, cancel_futures=True)
                            return

                    submit_batch()

        finally:
            self.pw_pool.shutdown()

        log.info("=" * 60)
        log.info(f"Done! scraped={self.pages_scraped} | js_rendered={self.js_rendered} | failed={self.pages_failed}")
        log.info(f"Output: {self.output_path}")
        log.info("=" * 60)


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def scrape_website(
    base_url: str,
    output_path: str = "output.jsonl",
    max_depth: int = 4,
    max_workers: int = 20,
    max_pages=None,
    use_sitemap: bool = True,
    js_mode: str = "auto",
    pw_pool_size: int = BROWSER_POOL_SIZE,
) -> str:
    """
    Crawl base_url completely and write results to output_path as JSONL.

    js_mode options:
      "auto"   — static first, auto Playwright fallback when JS detected  [default]
      "always" — always use Playwright (best for known SPAs)
      "never"  — static only, no Playwright (fastest)

    Each JSONL record contains:
      url, title, content, rag_chunk, depth, language, rendered_by,
      status_code, response_time_s, content_length, server, last_modified,
      cache_control, etag, ip_address, images, scraped_at
    """
    ThreadSafeCrawler(
        base_url=base_url, output_path=output_path,
        max_depth=max_depth, max_workers=max_workers,
        max_pages=max_pages, use_sitemap=use_sitemap,
        js_mode=js_mode, pw_pool_size=pw_pool_size,
    ).run()
    return output_path


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Hybrid parallel scraper — static + JS-rendered sites — for RAG ingestion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Azure docs (auto-detect JS pages)
  python app.py --url https://learn.microsoft.com/en-us/azure/ --output azure.jsonl

  # VMware (known SPA — force JS rendering)
  python app.py --url https://docs.vmware.com/ --output vmware.jsonl --js-mode always --pw-pool 6

  # OpenShift (static HTML — fastest)
  python app.py --url https://docs.openshift.com/ --output openshift.jsonl --js-mode never

  # Quick test run (50 pages, depth 2)
  python app.py --url https://docs.example.com/ --output test.jsonl --max-pages 50 --depth 2
        """,
    )
    parser.add_argument("--url", required=True, help="Base URL to crawl")
    parser.add_argument("--output", default="output.jsonl", help="Output JSONL file")
    parser.add_argument("--depth", type=int, default=4, help="Max crawl depth (default: 4)")
    parser.add_argument("--max-workers", type=int, default=20, help="Parallel threads (default: 20)")
    parser.add_argument("--max-pages", type=int, default=None, help="Stop after N pages")
    parser.add_argument("--no-sitemap", action="store_true", help="Skip sitemap discovery")
    parser.add_argument("--js-mode", choices=["auto", "always", "never"], default="auto",
                        help="JS rendering: auto (default) | always | never")
    parser.add_argument("--pw-pool", type=int, default=BROWSER_POOL_SIZE,
                        help=f"Playwright context pool size (default: {BROWSER_POOL_SIZE})")
    args = parser.parse_args()

    scrape_website(
        base_url=args.url,
        output_path=args.output,
        max_depth=args.depth,
        max_workers=args.max_workers,
        max_pages=args.max_pages,
        use_sitemap=not args.no_sitemap,
        js_mode=args.js_mode,
        pw_pool_size=args.pw_pool,
    )
