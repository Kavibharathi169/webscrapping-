import requests
from bs4 import BeautifulSoup
import os

URL = input("Enter the website URL: ").strip()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

OUTPUT_DIR = "output"
OUTPUT_FILE = "full_website_text.txt"


def extract_all_text(url: str) -> list[str]:
    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    # ❌ Remove non-visible elements
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()

    # ✅ Extract ALL visible text
    text_lines = []

    for string in soup.stripped_strings:
        text_lines.append(string)

    return text_lines


def save_to_txt(lines: list[str], filepath: str):
    with open(filepath, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Extracting ALL text from website...")

    all_text = extract_all_text(URL)

    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    save_to_txt(all_text, output_path)

    print(f"✔ Extracted {len(all_text)} text lines")
    print(f"✔ Saved to {output_path}")


if __name__ == "__main__":
    main()
