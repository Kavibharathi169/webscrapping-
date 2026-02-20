import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
from pathlib import Path
from urllib.parse import urljoin, urlparse
from collections import deque


class GovernanceContentExtractor:
    def __init__(self, timeout=20, max_depth=2):
        self.timeout = timeout
        self.max_depth = max_depth
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Governance-Ingestion/1.0)"
        }
        self.visited = set()

    def fetch_page(self, url: str) -> BeautifulSoup | None:
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"⚠️ Skipping {url} ({e})")
            return None

    def generate_chunk_id(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

    def extract_tables(self, table):
        rows = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
            if cells:
                rows.append(" | ".join(cells))
        return "\n".join(rows)

    def extract_page_content(self, soup, url):
        # Remove noise
        for tag in soup(["script", "style", "noscript", "iframe"]):
            tag.decompose()

        document_title = soup.title.get_text(strip=True) if soup.title else "Unknown"
        extracted_at = datetime.utcnow().isoformat()

        chunks = []

        current_section = None
        current_section_level = None
        current_chapter = None
        current_article = None

        allowed_tags = ["h1", "h2", "h3", "h4", "p", "li", "td", "dd"]

        for element in soup.find_all(allowed_tags):
            text = element.get_text(" ", strip=True)

            if not text or len(text) < 20:
                continue

            if element.name in {"h1", "h2", "h3", "h4"}:
                current_section = text
                current_section_level = element.name

                if text.lower().startswith("chapter"):
                    current_chapter = text
                if text.lower().startswith("article"):
                    current_article = text

            chunks.append({
                "source_url": url,
                "document_title": document_title,
                "organization": "Halows Co., Ltd.",
                "document_type": "governance_policy",
                "section_title": current_section,
                "section_level": current_section_level,
                "chapter": current_chapter,
                "article": current_article,
                "content_type": element.name,
                "text": text,
                "char_count": len(text),
                "chunk_id": self.generate_chunk_id(text),
                "extracted_at": extracted_at
            })

        # TABLE EXTRACTION
        for table in soup.find_all("table"):
            table_text = self.extract_tables(table)
            if table_text:
                chunks.append({
                    "source_url": url,
                    "document_title": document_title,
                    "organization": "Halows Co., Ltd.",
                    "document_type": "governance_policy",
                    "section_title": current_section,
                    "section_level": current_section_level,
                    "chapter": current_chapter,
                    "article": current_article,
                    "content_type": "table",
                    "text": table_text,
                    "char_count": len(table_text),
                    "chunk_id": self.generate_chunk_id(table_text),
                    "extracted_at": extracted_at
                })

        return chunks

    def crawl(self, start_url: str) -> list[dict]:
        base_domain = urlparse(start_url).netloc
        queue = deque([(start_url, 0)])
        all_chunks = []

        while queue:
            url, depth = queue.popleft()

            if url in self.visited or depth > self.max_depth:
                continue

            self.visited.add(url)
            print(f"🔍 Scraping: {url}")

            soup = self.fetch_page(url)
            if not soup:
                continue

            all_chunks.extend(self.extract_page_content(soup, url))

            for link in soup.find_all("a", href=True):
                next_url = urljoin(url, link["href"])
                parsed = urlparse(next_url)

                if (
                    parsed.netloc == base_domain
                    and not next_url.lower().endswith((".pdf", ".jpg", ".png"))
                    and next_url not in self.visited
                ):
                    queue.append((next_url, depth + 1))

        return all_chunks


def save_to_text_file(chunks, output_dir="output"):
    Path(output_dir).mkdir(exist_ok=True)
    file_path = Path(output_dir) / "governance_extracted.txt"

    with open(file_path, "w", encoding="utf-8") as f:
        for i, chunk in enumerate(chunks, 1):
            f.write("=" * 80 + "\n")
            f.write(f"CHUNK {i}\n")
            f.write("=" * 80 + "\n")
            for k, v in chunk.items():
                if k != "text":
                    f.write(f"{k}: {v}\n")
            f.write("\nCONTENT:\n")
            f.write(chunk["text"] + "\n\n")

    print(f"\n✅ Saved extracted content to: {file_path.resolve()}")


def main():
    url = input("Enter the URL to crawl: ").strip()

    extractor = GovernanceContentExtractor(max_depth=2)

    print("\n🚀 Starting governance crawling...\n")
    chunks = extractor.crawl(url)

    print(f"\n✅ Extracted {len(chunks)} content blocks")
    save_to_text_file(chunks)


if __name__ == "__main__":
    main()