import os
import json
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter



# -----------------------------------
# Load PDF with page-level metadata
# -----------------------------------
def load_pdf(pdf_path: str):
    loader = PyPDFLoader(pdf_path)
    return loader.load()


# -----------------------------------
# Recursive Chunking
# -----------------------------------
def chunk_documents(documents, source_name):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=500,
        chunk_overlap=50,
        separators=[
            "\n\nArticle ",
            "\n\nSection ",
            "\n\n",
            "\n",
            ". ",
            " ",
            ""
        ]
    )

    chunks = []
    chunk_id = 0

    for doc in documents:
        page_text = doc.page_content.strip()
        page_number = doc.metadata.get("page", -1)

        if not page_text:
            continue

        split_chunks = splitter.split_text(page_text)

        for chunk in split_chunks:
            chunk_id += 1
            chunks.append({
                "text": chunk,
                "metadata": {
                    "source": source_name,
                    "page": page_number,
                    "chunk_id": chunk_id
                }
            })

    return chunks


# -----------------------------------
# Save chunks to JSONL
# -----------------------------------
def save_chunks(chunks, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + "\n")


# -----------------------------------
# MAIN
# -----------------------------------
def main():
    pdf_path = input("Enter full PDF path: ").strip()

    if not os.path.exists(pdf_path):
        print("❌ File not found.")
        return

    source_name = os.path.basename(pdf_path)
    output_file = f"{source_name}_chunks.jsonl"

    print("\n📄 Loading PDF...")
    documents = load_pdf(pdf_path)

    print("✂️ Applying recursive chunking...")
    chunks = chunk_documents(documents, source_name)

    print("💾 Saving chunks...")
    save_chunks(chunks, output_file)

    print("\n✅ DONE")
    print(f"Total chunks created: {len(chunks)}")
    print(f"Output file: {output_file}")


if __name__ == "__main__":
    main()
