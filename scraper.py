import requests
import trafilatura
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
            timeout       : HTTP request timeout in seconds
            max_depth     : How deep to crawl from the start URL
            document_type : Label attached to every chunk
            delay         : Seconds to wait between requests (politeness)
        """
        self.timeout = timeout
        self.max_depth = max_depth
        self.document_type = document_type
        self.delay = delay
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Governance-Ingestion/1.0)"
        }
        self.visited = set()
        self.robot_parsers = {}

    # ------------------------------------------------------------------
    # robots.txt
    # ------------------------------------------------------------------
    def _can_fetch(self, url: str) -> bool:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"

        if domain not in self.robot_parsers:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(urljoin(domain, "/robots.txt"))
            try:
                rp.read()
            except Exception:
                rp.allow_all = True
            self.robot_parsers[domain] = rp

        return self.robot_parsers[domain].can_fetch("*", url)

    # ------------------------------------------------------------------
    # Fetch raw HTML
    # ------------------------------------------------------------------
    def fetch_html(self, url: str) -> str | None:
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"  ⚠️  Skipping {url} ({e})")
            return None

    # ------------------------------------------------------------------
    # Chunk ID — url + text combined for uniqueness
    # ------------------------------------------------------------------
    def generate_chunk_id(self, text: str, url: str) -> str:
        unique = f"{url}::{text}"
        return hashlib.sha256(unique.encode("utf-8")).hexdigest()[:16]

    # ------------------------------------------------------------------
    # Organisation — extracted from page meta tags
    # ------------------------------------------------------------------
    def extract_organization(self, soup: BeautifulSoup, url: str) -> str:
        for attr in [
            {"property": "og:site_name"},
            {"name": "author"},
            {"name": "organization"},
            {"name": "publisher"},
        ]:
            tag = soup.find("meta", attr)
            if tag and tag.get("content"):
                return tag["content"].strip()
        return urlparse(url).netloc

    # ------------------------------------------------------------------
    # STEP 1 — trafilatura: get clean text as a whitelist
    # Only text that trafilatura approves passes through BS4 extraction.
    # This removes nav/sidebar/footer noise from ANY website.
    # ------------------------------------------------------------------
    def get_clean_text_set(self, html: str) -> set[str]:
        clean_text = trafilatura.extract(
            html,
            include_tables=False,
            include_links=False,
            include_comments=False,
            no_fallback=False,
        )
        if not clean_text:
            return set()

        lines = set()
        for line in clean_text.splitlines():
            line = line.strip().strip("-•* ").strip()
            if line and len(line) >= 20:
                lines.add(line.lower())
        return lines

    # ------------------------------------------------------------------
    # STEP 2 — BeautifulSoup: walk DOM in order
    # Preserves real heading levels (h1-h4), paragraphs, tables.
    # Filters elements against trafilatura whitelist.
    # ------------------------------------------------------------------
    def extract_page(self, html: str, url: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        extracted_at = datetime.utcnow().isoformat()
        document_title = soup.title.get_text(strip=True) if soup.title else "Unknown"
        organization = self.extract_organization(soup, url)

        # Get trafilatura whitelist for this page
        clean_set = self.get_clean_text_set(html)

        # Remove noise tags before walking DOM
        for tag in soup(["script", "style", "noscript", "iframe", "nav", "footer", "header"]):
            tag.decompose()

        blocks = []
        current_section = None
        current_section_level = None
        current_chapter = None
        current_article = None

        HEADING_TAGS = {"h1", "h2", "h3", "h4"}
        CONTENT_TAGS = {"h1", "h2", "h3", "h4", "p", "li", "dd", "table"}

        for element in soup.find_all(CONTENT_TAGS):
            tag = element.name

            # ── HEADINGS ───────────────────────────────────────────────
            if tag in HEADING_TAGS:
                text = element.get_text(" ", strip=True)
                if not text or len(text) < 5:
                    continue

                # Update section context
                current_section = text
                current_section_level = tag

                # Reset article when new chapter starts
                if text.lower().startswith("chapter"):
                    current_chapter = text
                    current_article = None
                if text.lower().startswith("article"):
                    current_article = text

                blocks.append(self._build_block(
                    text=text,
                    content_type="heading",
                    url=url,
                    document_title=document_title,
                    organization=organization,
                    extracted_at=extracted_at,
                    section=current_section,
                    section_level=current_section_level,
                    chapter=current_chapter,
                    article=current_article,
                ))

            # ── TABLES ─────────────────────────────────────────────────
            elif tag == "table":
                table_text = self._parse_table(element)
                if not table_text or len(table_text) < 50:
                    continue

                blocks.append(self._build_block(
                    text=table_text,
                    content_type="table",
                    url=url,
                    document_title=document_title,
                    organization=organization,
                    extracted_at=extracted_at,
                    section=current_section,
                    section_level=current_section_level,
                    chapter=current_chapter,
                    article=current_article,
                ))

            # ── PARAGRAPHS / LIST ITEMS ────────────────────────────────
            else:
                text = element.get_text(" ", strip=True)
                if not text or len(text) < 30:
                    continue

                # KEY FIX: filter against trafilatura whitelist
                # If clean_set is empty (trafilatura failed), allow all
                if clean_set:
                    normalized = text.lower().strip("-•* ")
                    in_clean = normalized in clean_set
                    if not in_clean:
                        # Fuzzy check: is this text a substring of any clean line?
                        in_clean = any(
                            normalized in c or c in normalized
                            for c in clean_set
                        )
                    if not in_clean:
                        continue  # nav/sidebar noise — skip

                # Extra check: skip li items that are purely navigation links
                if tag == "li":
                    anchors = element.find_all("a")
                    non_link_text = text
                    for a in anchors:
                        non_link_text = non_link_text.replace(
                            a.get_text(" ", strip=True), ""
                        ).strip()
                    if len(non_link_text) < 20:
                        continue

                blocks.append(self._build_block(
                    text=text,
                    content_type="paragraph" if tag in {"p", "dd"} else "list_item",
                    url=url,
                    document_title=document_title,
                    organization=organization,
                    extracted_at=extracted_at,
                    section=current_section,
                    section_level=current_section_level,
                    chapter=current_chapter,
                    article=current_article,
                ))

        return blocks

    # ------------------------------------------------------------------
    # Build a block dict
    # ------------------------------------------------------------------
    def _build_block(self, text, content_type, url, document_title, organization,
                     extracted_at, section, section_level, chapter, article) -> dict:
        return {
            "source_url": url,
            "document_title": document_title,
            "organization": organization,
            "document_type": self.document_type,
            "section_title": section,
            "section_level": section_level,
            "chapter": chapter,
            "article": article,
            "content_type": content_type,
            "text": text,
            "char_count": len(text),
            "chunk_id": self.generate_chunk_id(text, url),
            "extracted_at": extracted_at,
        }

    # ------------------------------------------------------------------
    # Parse HTML table → pipe-delimited text
    # ------------------------------------------------------------------
    def _parse_table(self, table) -> str:
        rows = []
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
            if any(cells):
                rows.append(" | ".join(cells))
        return "\n".join(rows)

    # ------------------------------------------------------------------
    # Extract internal links for BFS
    # ------------------------------------------------------------------
    def extract_internal_links(self, html: str, base_url: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        base_domain = urlparse(base_url).netloc
        links = []

        for a in soup.find_all("a", href=True):
            full_url = urljoin(base_url, a["href"])
            parsed = urlparse(full_url)

            if (
                parsed.netloc == base_domain
                and parsed.scheme in {"http", "https"}
                and not full_url.lower().endswith(
                    (".pdf", ".jpg", ".png", ".zip", ".xlsx", ".docx")
                )
                and "#" not in full_url
                and full_url not in self.visited
            ):
                links.append(full_url)

        return list(set(links))

    # ------------------------------------------------------------------
    # BFS Crawler
    # ------------------------------------------------------------------
    def crawl(self, start_url: str) -> list[dict]:
        queue = deque([(start_url, 0)])
        all_blocks = []

        while queue:
            url, depth = queue.popleft()

            if url in self.visited or depth > self.max_depth:
                continue

            if not self._can_fetch(url):
                print(f"  🚫 Blocked by robots.txt: {url}")
                continue

            self.visited.add(url)
            print(f"🔍 Scraping (depth={depth}): {url}")

            time.sleep(self.delay)

            html = self.fetch_html(url)
            if not html:
                continue

            blocks = self.extract_page(html, url)
            all_blocks.extend(blocks)

            # Per-type count for logging
            counts = {}
            for b in blocks:
                counts[b["content_type"]] = counts.get(b["content_type"], 0) + 1
            print(f"   → {len(blocks)} blocks {counts}")

            if depth < self.max_depth:
                links = self.extract_internal_links(html, url)
                for link in links:
                    queue.append((link, depth + 1))

        return all_blocks


# ----------------------------------------------------------------------
# Save outputs
# ----------------------------------------------------------------------
def save_outputs(blocks: list[dict], output_dir: str = "output"):
    Path(output_dir).mkdir(exist_ok=True)

    # JSONL — primary output for chunker.py
    jsonl_path = Path(output_dir) / "scraped.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for block in blocks:
            f.write(json.dumps(block, ensure_ascii=False) + "\n")
    print(f"\n✅ JSONL saved : {jsonl_path.resolve()}")

    # TXT — human readable inspection
    txt_path = Path(output_dir) / "scraped.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        for i, block in enumerate(blocks, 1):
            f.write("=" * 80 + "\n")
            f.write(f"BLOCK {i}  [{block['content_type'].upper()}]\n")
            f.write("=" * 80 + "\n")
            for k, v in block.items():
                if k != "text":
                    f.write(f"{k}: {v}\n")
            f.write("\nCONTENT:\n")
            f.write(block["text"] + "\n\n")
    print(f"✅ TXT saved   : {txt_path.resolve()}")

    # Summary stats
    urls = {b["source_url"] for b in blocks}
    type_counts = {}
    for b in blocks:
        type_counts[b["content_type"]] = type_counts.get(b["content_type"], 0) + 1

    null_sections = sum(1 for b in blocks if b["section_title"] is None)

    print(f"\n📊 Scraping Summary:")
    print(f"   Pages crawled       : {len(urls)}")
    print(f"   Total blocks        : {len(blocks)}")
    print(f"   Content types       : {type_counts}")
    print(f"   Blocks with section : {len(blocks) - null_sections} "
          f"({(len(blocks) - null_sections) / max(len(blocks), 1) * 100:.1f}%)")
    print(f"   Blocks without      : {null_sections} "
          f"({null_sections / max(len(blocks), 1) * 100:.1f}%)")
    print(f"\n➡️  Next step: run chunker.py on scraped.jsonl")


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------
def main():
    print("=" * 60)
    print("  Governance Web Scraper v2")
    print("=" * 60)

    url = input("\nEnter the URL to crawl: ").strip()
    doc_type = (
        input("Document type label (default: governance_policy): ").strip()
        or "governance_policy"
    )
    max_depth = int(input("Max crawl depth (default: 2): ").strip() or "2")
    delay = float(
        input("Delay between requests in seconds (default: 1.0): ").strip() or "1.0"
    )

    extractor = GovernanceContentExtractor(
        max_depth=max_depth,
        document_type=doc_type,
        delay=delay,
    )

    print(f"\n🚀 Starting crawl from: {url}\n")
    blocks = extractor.crawl(url)

    print(f"\n✅ Total blocks extracted: {len(blocks)}")
    save_outputs(blocks)


if __name__ == "__main__":
    main()