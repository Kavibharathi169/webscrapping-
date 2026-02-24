import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import json
import time
import urllib.robotparser
from pathlib import Path
from urllib.parse import urljoin, urlparse
from collections import deque


class GovernanceContentExtractor:
    def __init__(self, timeout=20, max_depth=2, document_type="governance_policy", delay=1.0):
        """
        Args:
            timeout     : HTTP request timeout in seconds
            max_depth   : How deep to crawl from the start URL
            document_type: Type label attached to every chunk (e.g. 'governance_policy')
            delay       : Seconds to wait between requests (politeness)
        """
        self.timeout = timeout
        self.max_depth = max_depth
        self.document_type = document_type          # FIX #2 – no longer hardcoded
        self.delay = delay                          # FIX #6 – rate limiting
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Governance-Ingestion/1.0)"
        }
        self.visited = set()
        self.robot_parsers = {}                     # FIX #7 – robots.txt cache per domain

    # ------------------------------------------------------------------
    # robots.txt helper
    # ------------------------------------------------------------------
    def _can_fetch(self, url: str) -> bool:
        """Return True if robots.txt allows fetching this URL."""
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"

        if domain not in self.robot_parsers:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(urljoin(domain, "/robots.txt"))
            try:
                rp.read()
            except Exception:
                # If robots.txt can't be fetched, allow crawling
                rp.allow_all = True
            self.robot_parsers[domain] = rp

        return self.robot_parsers[domain].can_fetch("*", url)

    # ------------------------------------------------------------------
    # Page fetcher
    # ------------------------------------------------------------------
    def fetch_page(self, url: str) -> BeautifulSoup | None:
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            print(f"⚠️  Skipping {url} ({e})")
            return None

    # ------------------------------------------------------------------
    # Chunk ID  –  FIX #3: include URL so identical text on different
    # pages gets a unique ID
    # ------------------------------------------------------------------
    def generate_chunk_id(self, text: str, url: str) -> str:
        unique = f"{url}::{text}"
        return hashlib.sha256(unique.encode("utf-8")).hexdigest()[:16]

    # ------------------------------------------------------------------
    # Organisation name  –  FIX #1: extract from page meta, not hardcoded
    # ------------------------------------------------------------------
    def extract_organization(self, soup: BeautifulSoup, url: str) -> str:
        for attr in [{"property": "og:site_name"}, {"name": "author"}, {"name": "organization"}]:
            tag = soup.find("meta", attr)
            if tag and tag.get("content"):
                return tag["content"].strip()
        return urlparse(url).netloc  # fallback: use domain name

    # ------------------------------------------------------------------
    # Table extractor (unchanged logic, called inline now – FIX #9)
    # ------------------------------------------------------------------
    def extract_table_text(self, table) -> str:
        rows = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
            if cells:
                rows.append(" | ".join(cells))
        return "\n".join(rows)

    # ------------------------------------------------------------------
    # Main content extractor
    # ------------------------------------------------------------------
    def extract_page_content(self, soup: BeautifulSoup, url: str) -> list[dict]:
        # Remove noise tags
        for tag in soup(["script", "style", "noscript", "iframe", "nav", "footer", "header"]):
            tag.decompose()

        document_title = soup.title.get_text(strip=True) if soup.title else "Unknown"
        organization = self.extract_organization(soup, url)   # FIX #1
        extracted_at = datetime.utcnow().isoformat()

        # FIX #5 – prefer main/article/section to avoid nav menus
        content_root = (
            soup.find("main")
            or soup.find("article")
            or soup.find("div", {"id": "content"})
            or soup.find("div", {"class": "content"})
            or soup.body
            or soup
        )

        chunks = []
        current_section = None
        current_section_level = None
        current_chapter = None
        current_article = None

        allowed_tags = {"h1", "h2", "h3", "h4", "p", "li", "dd", "table"}

        # FIX #9 – process ALL elements (including tables) in document order
        # so section context is always correct when a table is encountered
        for element in content_root.find_all(allowed_tags):
            tag_name = element.name

            # ---- Headings: update section context ----------------------
            if tag_name in {"h1", "h2", "h3", "h4"}:
                text = element.get_text(" ", strip=True)
                if not text or len(text) < 5:
                    continue

                current_section = text
                current_section_level = tag_name

                # FIX #8 – reset article when a new chapter starts
                if text.lower().startswith("chapter"):
                    current_chapter = text
                    current_article = None          # ← reset here
                if text.lower().startswith("article"):
                    current_article = text

            # ---- Tables: extract inline --------------------------------
            elif tag_name == "table":
                table_text = self.extract_table_text(element)
                if not table_text or len(table_text) < 50:  # FIX #4
                    continue
                chunks.append(self._build_chunk(
                    text=table_text,
                    content_type="table",
                    url=url,
                    document_title=document_title,
                    organization=organization,
                    extracted_at=extracted_at,
                    current_section=current_section,
                    current_section_level=current_section_level,
                    current_chapter=current_chapter,
                    current_article=current_article,
                ))
                continue

            # ---- Regular text elements ---------------------------------
            else:
                text = element.get_text(" ", strip=True)
                if not text or len(text) < 50:      # FIX #4 – raised threshold
                    continue

            chunks.append(self._build_chunk(
                text=text,
                content_type=tag_name,
                url=url,
                document_title=document_title,
                organization=organization,
                extracted_at=extracted_at,
                current_section=current_section,
                current_section_level=current_section_level,
                current_chapter=current_chapter,
                current_article=current_article,
            ))

        return chunks

    # ------------------------------------------------------------------
    # Helper: build a chunk dict
    # ------------------------------------------------------------------
    def _build_chunk(self, text, content_type, url, document_title, organization,
                     extracted_at, current_section, current_section_level,
                     current_chapter, current_article) -> dict:
        return {
            "source_url": url,
            "document_title": document_title,
            "organization": organization,                # FIX #1
            "document_type": self.document_type,         # FIX #2
            "section_title": current_section,
            "section_level": current_section_level,
            "chapter": current_chapter,
            "article": current_article,
            "content_type": content_type,
            "text": text,
            "char_count": len(text),
            "chunk_id": self.generate_chunk_id(text, url),  # FIX #3
            "extracted_at": extracted_at,
        }

    # ------------------------------------------------------------------
    # BFS Crawler
    # ------------------------------------------------------------------
    def crawl(self, start_url: str) -> list[dict]:
        base_domain = urlparse(start_url).netloc
        queue = deque([(start_url, 0)])
        all_chunks = []

        while queue:
            url, depth = queue.popleft()

            if url in self.visited or depth > self.max_depth:
                continue

            # FIX #7 – respect robots.txt
            if not self._can_fetch(url):
                print(f"🚫 Blocked by robots.txt: {url}")
                continue

            self.visited.add(url)
            print(f"🔍 Scraping (depth={depth}): {url}")

            # FIX #6 – politeness delay
            time.sleep(self.delay)

            soup = self.fetch_page(url)
            if not soup:
                continue

            page_chunks = self.extract_page_content(soup, url)
            all_chunks.extend(page_chunks)
            print(f"   → {len(page_chunks)} chunks extracted")

            if depth < self.max_depth:
                for link in soup.find_all("a", href=True):
                    next_url = urljoin(url, link["href"])
                    parsed = urlparse(next_url)
                    # Stay on same domain, skip binary files and fragments
                    if (
                        parsed.netloc == base_domain
                        and not next_url.lower().endswith((".pdf", ".jpg", ".png", ".zip", ".xlsx"))
                        and "#" not in next_url
                        and next_url not in self.visited
                    ):
                        queue.append((next_url, depth + 1))

        return all_chunks


# ----------------------------------------------------------------------
# Save outputs  –  FIX #10: save JSONL (easy to parse) + readable TXT
# ----------------------------------------------------------------------
def save_outputs(chunks: list[dict], output_dir: str = "output"):
    Path(output_dir).mkdir(exist_ok=True)

    # --- JSONL (primary output for chunking pipeline) -----------------
    jsonl_path = Path(output_dir) / "governance_extracted.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + "\n")
    print(f"✅ JSONL saved  : {jsonl_path.resolve()}")

    # --- Human-readable TXT (for inspection) -------------------------
    txt_path = Path(output_dir) / "governance_extracted.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        for i, chunk in enumerate(chunks, 1):
            f.write("=" * 80 + "\n")
            f.write(f"CHUNK {i}\n")
            f.write("=" * 80 + "\n")
            for k, v in chunk.items():
                if k != "text":
                    f.write(f"{k}: {v}\n")
            f.write("\nCONTENT:\n")
            f.write(chunk["text"] + "\n\n")
    print(f"✅ TXT saved    : {txt_path.resolve()}")

    # --- Summary stats ------------------------------------------------
    urls = {c["source_url"] for c in chunks}
    types = {}
    for c in chunks:
        types[c["content_type"]] = types.get(c["content_type"], 0) + 1

    print(f"\n📊 Summary:")
    print(f"   Pages crawled : {len(urls)}")
    print(f"   Total chunks  : {len(chunks)}")
    print(f"   Content types : {types}")


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------
def main():
    url = input("Enter the URL to crawl: ").strip()
    doc_type = input("Document type label (default: governance_policy): ").strip() or "governance_policy"
    delay = float(input("Delay between requests in seconds (default: 1.0): ").strip() or "1.0")

    extractor = GovernanceContentExtractor(
        max_depth=2,
        document_type=doc_type,
        delay=delay,
    )

    print("\n🚀 Starting crawl...\n")
    chunks = extractor.crawl(url)

    print(f"\n✅ Total chunks extracted: {len(chunks)}")
    save_outputs(chunks)


if __name__ == "__main__":
    main()