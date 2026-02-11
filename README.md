# English Catalogue of Books (1912–1922)

Structured data extracted from the English Catalogue of Books, a historical bibliography of books published in England and Ireland. Source texts are OCR scans from HathiTrust (Princeton and NYPL editions).

## Directory Structure

```
ecb_ocr_text/              Raw OCR text files (1902–1922)
entries/
  extracted_entries/        Regex-extracted entries from OCR text (1902–1922)
  hand_corrected_entries/   Manually reviewed entries (1912–1922)
parsed_dataframes/          LLM-parsed entries with structured fields (1912–1922)
scripts/
  create_entries.py         Extract entries from OCR text
  llm_parser.py             Parse entries into structured fields via Google Gemini
  ai_output_accuracy_check.ipynb   Quality check on LLM output
  splitters.txt             Year-specific regex patterns for entry extraction
```

## Pipeline

1. `create_entries.py` splits raw OCR into individual entries using regex
2. Extracted entries are manually reviewed and corrected
3. `llm_parser.py` sends entries to Gemini to extract author, title, format, publisher, date, etc.
4. `ai_output_accuracy_check.ipynb` flags parsing errors using Levenshtein/Jaccard similarity

## Parsed Fields

`author(s)`, `title`, `format`, `publisher`, `date`
