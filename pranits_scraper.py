import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import tldextract
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from goose3 import Goose
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
]

INDIAN_LANG_CODES = ['hi', 'bn', 'te', 'mr', 'ta', 'gu', 'kn', 'ml', 'pa', 'ur']

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def is_internal_link(base_url, link):
    base = tldextract.extract(base_url)
    target = tldextract.extract(link)
    return base.domain == target.domain and base.suffix == target.suffix

def detect_language(text):
    try:
        lang_code = detect(text)
        return lang_code
    except:
        return "unknown"

def scrape_page(url, depth, session):
    try:
        headers = {
            "User-Agent": get_random_user_agent()
        }

        start_time = time.time()
        response = session.get(url, headers=headers, timeout=10)
        end_time = time.time()

        soup = BeautifulSoup(response.text, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else "No Title Found"

        g = Goose()
        article = g.extract(raw_html=response.text)
        structured_content = article.cleaned_text or soup.get_text(separator="\n", strip=True)

        detected_lang = detect_language(structured_content)
        is_indian_lang = detected_lang in INDIAN_LANG_CODES

        links = [urljoin(url, a["href"]) for a in soup.find_all("a", href=True)]
        images = [urljoin(url, img["src"]) for img in soup.find_all("img", src=True)]

        try:
            ip_address = socket.gethostbyname(urlparse(url).hostname)
        except:
            ip_address = "Unavailable"

        metadata = {
            "Title": title,
            "URL": url,
            "Detected Language": detected_lang,
            "Is Indian Language": is_indian_lang,
            "Server": response.headers.get("Server", "N/A"),
            "Last Modified": response.headers.get("Last-Modified", "N/A"),
            "Status Code": response.status_code,
            "Response Time (s)": round(end_time - start_time, 4),
            "Content Length": len(response.content),
            "Depth": depth,
            "Cache Control": response.headers.get("Cache-Control", "N/A"),
            "ETag": response.headers.get("ETag", "N/A"),
            "IP Address": ip_address,
            "User Agent": headers["User-Agent"]
        }

        return {
            "title": title,
            "content": structured_content,
            "metadata": metadata,
            "response_data": {
                "Status Code": response.status_code,
                "Content-Type": response.headers.get("Content-Type"),
                "Content-Length": response.headers.get("Content-Length"),
                "Encoding": response.encoding,
                "URL": response.url,
            },
            "images": images,
            "links": links
        }

    except Exception as e:
        return {"error": str(e), "url": url}

def scrape_website(base_url, max_depth=2, max_links_per_page=10, max_workers=10):
    visited = set()
    results = []
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    def scrape_recursive(url, depth):
        if depth > max_depth or url in visited:
            return None
        visited.add(url)
        return scrape_page(url, depth, session)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(scrape_recursive, base_url, 1): base_url}
        while future_to_url:
            done, _ = as_completed(future_to_url), future_to_url.copy()
            for future in done:
                future_to_url.pop(future, None)
                result = future.result()
                if result and "error" not in result:
                    results.append(result)
                    links = result.get("links", [])[:max_links_per_page]
                    for link in links:
                        if is_internal_link(base_url, link) and link not in visited:
                            future_to_url[executor.submit(scrape_recursive, link, result["metadata"]["Depth"] + 1)] = link
    return results