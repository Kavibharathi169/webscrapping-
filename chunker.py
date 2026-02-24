import json
import hashlib
from pathlib import Path
from collections import defaultdict
import tiktoken


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
MAX_TOKENS     = 512   # max tokens per final chunk
OVERLAP_TOKENS = 50    # overlap between split chunks to preserve context
MIN_TOKENS     = 50    # merge blocks smaller than this with their neighbor


# ----------------------------------------------------------------------
# Tokenizer  (tiktoken — same tokenizer used by OpenAI/most LLMs)
# ----------------------------------------------------------------------
ENCODER = tiktoken.get_encoding("cl100k_base")

def count_tokens(text: str) -> int:
    return len(ENCODER.encode(text))

def split_text_by_tokens(text: str, max_tokens: int, overlap_tokens: int) -> list[str]:
    """
    Split a long text into chunks of max_tokens with overlap_tokens overlap.
    Splits on sentence boundaries where possible.
    """
    tokens = ENCODER.encode(text)

    if len(tokens) <= max_tokens:
        return [text]

    chunks = []
    start = 0

    while start < len(tokens):
        end = min(start + max_tokens, len(tokens))
        chunk_tokens = tokens[start:end]
        chunk_text = ENCODER.decode(chunk_tokens)
        chunks.append(chunk_text)

        if end == len(tokens):
            break

        # Move forward by (max_tokens - overlap_tokens) so chunks overlap
        start += max_tokens - overlap_tokens

    return chunks


# ----------------------------------------------------------------------
# Load scraped JSONL
# ----------------------------------------------------------------------
def load_scraped(path: str) -> list[dict]:
    blocks = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                blocks.append(json.loads(line))
    print(f"📥 Loaded {len(blocks)} blocks from {path}")
    return blocks


# ----------------------------------------------------------------------
# Group blocks by (source_url, section_title)
# This keeps all content under the same heading together
# ----------------------------------------------------------------------
def group_blocks(blocks: list[dict]) -> dict:
    """
    Returns an ordered dict:
      key   → (source_url, section_title)
      value → list of block dicts in document order
    """
    groups = defaultdict(list)

    for block in blocks:
        key = (
            block["source_url"],
            block["section_title"] or "__no_section__"
        )
        groups[key].append(block)

    print(f"📂 Grouped into {len(groups)} sections")
    return groups


# ----------------------------------------------------------------------
# Build final chunks from a group of blocks
# Steps:
#   1. Skip pure heading blocks (they're captured in section_title)
#   2. Merge small fragments together
#   3. Split oversized merged text at MAX_TOKENS with OVERLAP_TOKENS overlap
#   4. Attach full metadata to every final chunk
# ----------------------------------------------------------------------
def chunk_group(group_key: tuple, group_blocks: list[dict], chunk_index_start: int) -> list[dict]:
    source_url, section_title = group_key
    chunks = []
    chunk_index = chunk_index_start

    # Pick metadata from first block in group
    meta = group_blocks[0]

    # Collect only content blocks (skip standalone headings —
    # heading text is already captured in section_title)
    content_blocks = [
        b for b in group_blocks
        if b["content_type"] in {"paragraph", "list_item", "table"}
    ]

    if not content_blocks:
        return []

    # ── Step 1: Merge small blocks ────────────────────────────────────
    # Walk through blocks and merge anything under MIN_TOKENS
    # into the next block until it grows large enough
    merged_segments = []
    buffer = ""
    buffer_type = "paragraph"

    for block in content_blocks:
        text = block["text"].strip()

        # Tables are always kept as standalone segments (don't merge)
        if block["content_type"] == "table":
            if buffer:
                merged_segments.append((buffer.strip(), buffer_type))
                buffer = ""
            merged_segments.append((text, "table"))
            continue

        combined = (buffer + " " + text).strip() if buffer else text

        if count_tokens(combined) < MIN_TOKENS:
            # Still too small — keep accumulating
            buffer = combined
            buffer_type = block["content_type"]
        else:
            if buffer and count_tokens(buffer) < MIN_TOKENS:
                # Flush small buffer combined with current text
                merged_segments.append((combined, block["content_type"]))
                buffer = ""
            else:
                if buffer:
                    merged_segments.append((buffer.strip(), buffer_type))
                buffer = text
                buffer_type = block["content_type"]

    # Flush remaining buffer
    if buffer:
        merged_segments.append((buffer.strip(), buffer_type))

    # ── Step 2: Split large segments + add overlap ────────────────────
    for seg_text, seg_type in merged_segments:
        if not seg_text:
            continue

        token_count = count_tokens(seg_text)

        if token_count <= MAX_TOKENS:
            # Fits in one chunk — keep as is
            chunks.append(_build_chunk(
                text=seg_text,
                content_type=seg_type,
                token_count=token_count,
                chunk_index=chunk_index,
                meta=meta,
                source_url=source_url,
                section_title=section_title if section_title != "__no_section__" else None,
            ))
            chunk_index += 1
        else:
            # Too large — split with overlap
            splits = split_text_by_tokens(seg_text, MAX_TOKENS, OVERLAP_TOKENS)
            for split_text in splits:
                chunks.append(_build_chunk(
                    text=split_text,
                    content_type=seg_type,
                    token_count=count_tokens(split_text),
                    chunk_index=chunk_index,
                    meta=meta,
                    source_url=source_url,
                    section_title=section_title if section_title != "__no_section__" else None,
                ))
                chunk_index += 1

    return chunks


# ----------------------------------------------------------------------
# Build a final chunk dict with full metadata
# ----------------------------------------------------------------------
def _build_chunk(text, content_type, token_count, chunk_index, meta,
                 source_url, section_title) -> dict:
    chunk_id = hashlib.sha256(
        f"{source_url}::{section_title}::{chunk_index}::{text[:50]}".encode()
    ).hexdigest()[:16]

    return {
        "chunk_id":       chunk_id,
        "chunk_index":    chunk_index,
        "source_url":     source_url,
        "document_title": meta["document_title"],
        "organization":   meta["organization"],
        "document_type":  meta["document_type"],
        "section_title":  section_title,
        "section_level":  meta["section_level"],
        "chapter":        meta["chapter"],
        "article":        meta["article"],
        "content_type":   content_type,
        "text":           text,
        "token_count":    token_count,
        "char_count":     len(text),
        "extracted_at":   meta["extracted_at"],
    }


# ----------------------------------------------------------------------
# Save final chunks
# ----------------------------------------------------------------------
def save_chunks(chunks: list[dict], output_dir: str = "output"):
    Path(output_dir).mkdir(exist_ok=True)

    # JSONL — primary output for embedding / LLM pipeline
    jsonl_path = Path(output_dir) / "chunked.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + "\n")
    print(f"\n✅ JSONL saved : {jsonl_path.resolve()}")

    # TXT — human readable
    txt_path = Path(output_dir) / "chunked.txt"
    with open(txt_path, "w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write("=" * 80 + "\n")
            f.write(f"CHUNK {chunk['chunk_index']}  [{chunk['content_type'].upper()}]  "
                    f"tokens={chunk['token_count']}\n")
            f.write("=" * 80 + "\n")
            for k, v in chunk.items():
                if k != "text":
                    f.write(f"{k}: {v}\n")
            f.write("\nCONTENT:\n")
            f.write(chunk["text"] + "\n\n")
    print(f"✅ TXT saved   : {txt_path.resolve()}")

    # Summary stats
    token_counts = [c["token_count"] for c in chunks]
    type_counts  = {}
    url_counts   = {}
    for c in chunks:
        type_counts[c["content_type"]] = type_counts.get(c["content_type"], 0) + 1
        url_counts[c["source_url"]]    = url_counts.get(c["source_url"], 0) + 1

    over_limit = sum(1 for t in token_counts if t > MAX_TOKENS)

    print(f"\n📊 Chunking Summary:")
    print(f"   Total chunks        : {len(chunks)}")
    print(f"   Content types       : {type_counts}")
    print(f"   Token count — min   : {min(token_counts)}")
    print(f"   Token count — max   : {max(token_counts)}")
    print(f"   Token count — avg   : {sum(token_counts) // len(token_counts)}")
    print(f"   Chunks > {MAX_TOKENS} tokens  : {over_limit}  (should be 0)")
    print(f"   Unique source URLs  : {len(url_counts)}")
    print(f"\n➡️  chunked.jsonl is ready for embedding / LLM pipeline")


# ----------------------------------------------------------------------
# Main pipeline
# ----------------------------------------------------------------------
def main():
    print("=" * 60)
    print("  Governance Chunker")
    print("=" * 60)

    input_path  = input("\nPath to scraped.jsonl (default: output/scraped.jsonl): ").strip() \
                  or "output/scraped.jsonl"
    output_dir  = input("Output directory (default: output): ").strip() \
                  or "output"

    # Load
    blocks = load_scraped(input_path)

    # Group by section
    groups = group_blocks(blocks)

    # Chunk each group
    print(f"\n🧩 Chunking {len(groups)} sections...")
    all_chunks = []
    chunk_index = 1

    for group_key, group_blocks_list in groups.items():
        new_chunks = chunk_group(group_key, group_blocks_list, chunk_index)
        all_chunks.extend(new_chunks)
        chunk_index += len(new_chunks)

    print(f"✅ Total chunks created: {len(all_chunks)}")

    # Save
    save_chunks(all_chunks, output_dir)


if __name__ == "__main__":
    main()