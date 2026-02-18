import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
from pathlib import Path


class GovernanceContentExtractor:
    def __init__(self, timeout: int = 20):
        self.timeout = timeout
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Governance-Ingestion/1.0)"
        }

    def fetch_page(self, url: str) -> BeautifulSoup:
        response = requests.get(url, headers=self.headers, timeout=self.timeout)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def generate_chunk_id(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

    def extract(self, url: str) -> list[dict]:
        soup = self.fetch_page(url)

        # Remove non-content noise
        for tag in soup(["script", "style", "noscript", "iframe"]):
            tag.decompose()

        document_title = soup.title.get_text(strip=True) if soup.title else "Unknown"
        extracted_at = datetime.utcnow().isoformat()

        chunks = []

        # Context trackers
        current_section = None
        current_section_level = None
        current_chapter = None
        current_article = None

        allowed_tags = [
            "h1", "h2", "h3", "h4",
            "p", "li", "div", "span", "td", "dd"
        ]

        for element in soup.find_all(allowed_tags):
            text = element.get_text(" ", strip=True)

            if not text or len(text) < 20:
                continue

            # Update hierarchy
            if element.name in {"h1", "h2", "h3", "h4"}:
                current_section = text
                current_section_level = element.name

                if text.lower().startswith("chapter"):
                    current_chapter = text
                if text.lower().startswith("article"):
                    current_article = text

            chunk = {
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
            }

            chunks.append(chunk)

        return chunks


def save_to_text_file(chunks: list[dict], output_dir="output"):
    Path(output_dir).mkdir(exist_ok=True)
    file_path = Path(output_dir) / "governance_extracted.txt"

    with open(file_path, "w", encoding="utf-8") as f:
        for i, chunk in enumerate(chunks, start=1):
            f.write(f"\n{'=' * 80}\n")
            f.write(f"CHUNK {i}\n")
            f.write(f"{'=' * 80}\n")
            for key, value in chunk.items():
                if key != "text":
                    f.write(f"{key}: {value}\n")
            f.write("\nCONTENT:\n")
            f.write(chunk["text"])
            f.write("\n")

    print(f"\nSaved extracted content to: {file_path.resolve()}")


def main():
    url = input("Enter the URL of the governance document to extract: ").strip()

    print("\nStarting governance content ingestion...\n")

    extractor = GovernanceContentExtractor()
    chunks = extractor.extract(url)

    print(f"Extracted {len(chunks)} content chunks\n")

    if chunks:
        print("Sample chunk:\n")
        print(chunks[0])

    save_to_text_file(chunks)


if __name__ == "__main__":
    main()
