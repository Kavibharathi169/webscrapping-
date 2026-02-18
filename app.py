import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime
import hashlib
import sys

# ---------------- CONFIG ---------------- #

HEADERS = {
    "User-Agent": "Mozilla/5.0 (GovernanceScraper/1.0)"
}

ALLOWED_PATH_KEYWORDS = [
    "/governance",
    "/ir",
    "/policy",
    "/compliance"
]

BLOCKED_EXTENSIONS = (
    ".pdf", ".jpg", ".jpeg", ".png", ".gif",
    ".zip", ".doc", ".docx", ".xls", ".xlsx"
)

MAX_PAGES = 25
visited_urls = set()
chunks = []

# ---------------- HELPERS ---------------- #

def is_allowed_url(url: str, base_domain: str) -> bool:
    parsed = urlparse(url)

    if parsed.netloc != base_domain:
        return False

    if any(parsed.path.lower().endswith(ext) for ext in BLOCKED_EXTENSIONS):
        return False

    if not any(k in parsed.path.lower() for k in ALLOWED_PATH_KEYWORDS):
        return False

    return True


def chunk_id(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def extract_metadata(soup: BeautifulSoup, url: str) -> dict:
    title = soup.title.get_text(strip=True) if soup.title else None

    org = None
    for tag in soup.find_all(["footer", "address", "p"]):
        if "Co., Ltd" in tag.get_text():
            org = tag.get_text(strip=True)
            break

    return {
        "source_url": url,
        "document_title": title,
        "organization": org,
        "document_type": "governance_policy",
        "extracted_at": datetime.utcnow().isoformat()
    }


# ---------------- CORE EXTRACTION ---------------- #

def extract_page(url: str, base_domain: str):
    if url in visited_urls or len(visited_urls) >= MAX_PAGES:
        return

    print(f"Scraping: {url}")
    visited_urls.add(url)

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except Exception as e:
        print(f"Skipped (error): {url} -> {e}")
        return

    soup = BeautifulSoup(response.text, "html.parser")
    metadata = extract_metadata(soup, url)

    current_heading = None

    for element in soup.find_all(["h1", "h2", "h3", "p", "li"]):
        text = element.get_text(" ", strip=True)

        if not text or len(text) < 30:
            continue

        if element.name in ["h1", "h2", "h3"]:
            current_heading = text
            continue

        chunk = {
            **metadata,
            "section_title": current_heading,
            "content_type": element.name,
            "text": text,
            "char_count": len(text),
            "chunk_id": chunk_id(text)
        }

        chunks.append(chunk)

    # -------- FOLLOW INTERNAL LINKS -------- #

    for link in soup.find_all("a", href=True):
        next_url = urljoin(url, link["href"])
        next_url = next_url.split("#")[0]

        if is_allowed_url(next_url, base_domain):
            extract_page(next_url, base_domain)


# ---------------- SAVE OUTPUT ---------------- #

def save_to_text_file(filename="governance_output.txt"):
    with open(filename, "w", encoding="utf-8") as f:
        for c in chunks:
            f.write("=" * 80 + "\n")
            f.write(f"URL: {c['source_url']}\n")
            f.write(f"Title: {c['document_title']}\n")
            f.write(f"Organization: {c['organization']}\n")
            f.write(f"Section: {c['section_title']}\n")
            f.write(f"Type: {c['content_type']}\n")
            f.write(f"Characters: {c['char_count']}\n")
            f.write("-" * 80 + "\n")
            f.write(c["text"] + "\n\n")


# ---------------- MAIN ---------------- #

def main():
    start_url = input("Enter the URL of the governance document to extract: ").strip()

    parsed = urlparse(start_url)
    base_domain = parsed.netloc

    extract_page(start_url, base_domain)

    save_to_text_file()

    print(f"\nExtracted {len(chunks)} governance chunks")
    print("Saved to governance_output.txt")


if __name__ == "__main__":
    main()
