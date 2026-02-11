import pandas as pd, time, json, csv, ast, regex as re, socket
from tqdm import tqdm
from google import genai
from google.genai import types
from pathlib import Path

# load API key from local file
API_KEY_FILE = Path(__file__).parent / ".api_key"
if not API_KEY_FILE.exists():
    raise FileNotFoundError(
        f"API key file not found at {API_KEY_FILE}. "
        "Create it with your Google API key as the sole contents."
    )
client = genai.Client(api_key=API_KEY_FILE.read_text().strip())

# paths
current_dir = Path.cwd()
entries_directory = current_dir.parent / 'entries'
corrected_entries_directory = entries_directory / 'hand_corrected_entries'  # input: hand-corrected CSVs
parsed_dataframe_directory = current_dir.parent / 'dataframes' / 'parsed_dataframes'  # output: parsed batches

file_batch_size = 64  # entries per API batch

# output fields gemini returns
fieldnames = ["opening_bits", "author(s)", "title", "format", "little_bits", "publisher", "date"]


prompt = """
Task:

You are given raw publishing data. Each entry includes a title, author name, publisher, and publishing month and year. Your job is to extract the following:Output format: 

{{
"opening_bits": "",
"author(s)":  "",
"title": "",
"format": "",
"little_bits": "",
"publisher": "",
"date": ""
}}

Special instructions:
* The publishing year is written in shorthand (e.g., '17), and all dates are from the 1900s. Do not convert '17 to 1917, just save it as '17.
* Do not make any copy edits or corrections. Do not correct spelling, punctuation, or special characters.
* If there are abbreviations, do not change those abbreviations. So Jan 17 should remain Jan 17.
* Entries that start with a word followed by a dash instead of parentheses, with the author's first name included within, are entries without an author. Entries starting with words like “Admiralty”, “Army”, and “Acts” are words that appear at the beginning of the entries, but are not last names. Thus, entries such as “Acts-Grey seals (protection)” should have no author. Their title should be: “Acts-Grey seals (protection)”
* The title field should contain the main descriptive name of the publication. Any information that directly modifies or describes the core title (e.g., editors, volumes, editions, or subtitles) and appears before the format (if a format is present) should be included in the title.
* The little_bits field should capture any remaining descriptive information about the publication that is not part of the core title, author, publisher, or date. This includes, but is not limited to, page counts, dimensions, prices, series information, or specific publication notes. If a format is present, little_bits will contain information appearing after the format. If no format is present in the entry, any information that follows the main descriptive title and is not the publisher or date should be placed in little_bits.
* The format is usually a number and a format type (fol, vo, to, mo). E.g., 12mo, 8vo. If there is 'Cr. ' before that, please include 'Cr. ' in the format too. E.g., Cr. 8vo.
* In extremely rare instances, there are multiples month and years listed. For those cases, all the months in the date as a single string. Similarly, there is sometimes extra information between month and year like in “Jan., &c., '19.” Include all the extra in-between information in the date as well.
* Maintain the order of the original entry. If what seems like an author name appears within the title, keep it as part of the title. Do not interpret that as author name.
* When generating JSON, properly escape any quotation marks within text fields using backslashes (e.g., "title": "The book \"special\" chapter").

Here are some example inputs and outputs:

Example input:

Abbott (E. W.)- The Colliery official's, work. man's and bill clerk's friend. 16mo. pp. 45 (South Shields : 49, Northcote St.) E. W. ABBOTT, Dec. '17

Example output:

{{
"opening_bits": "",
"author(s)":  "Abbott (E. W.)",
"title": "The Colliery official's, work. man's and bill clerk's friend.",
"format": "16mo.",
"little_bits": "pp. 45 (South Shields : 49, Northcote St.)",
"publisher": "E. W. ABBOTT",
"date": "Dec. '17"
}}


—

Example input: 

Pollock (John)--War and revolution in Russia : sketches and studies. Cr. 8vo. 71 x 5, pp. 298, 6s. net . CONSTABLE, Mar '18

Example output:

{{
"opening_bits": "",
"author(s)":  "Pollock (John)",
"title": "War and revolution in Russia : sketches and studies.",
"format": "Cr. 8vo.",
"little_bits": "71 x 5, pp. 298, 6s. net . ",
"publisher": "CONSTABLE",
"date": "Mar '18"
}}

—

Example input:

38. 6d. M. (D. R.)-A Silver lining. IS. net. Dec. 16

Example output:

{{
"opening_bits": "38. 6d. ",
"author(s)":  "M. (D. R.)",
"title": "A Silver lining.",
"format": "",
"little_bits": "IS. net.",
"publisher": "",
"date": "Dec. 16"
}}

—

Example input:
. Qru mbino (J. C. F.)-Clairvoyance : the system of philosophy concerning the divinity of clair. voyance, also a treatise on divination and crystal reading. Cr. 8vo. 7} x 41, pp. 140, 25. 6d. net FOWLER, Mar. 15

Example output:

{{
"opening_bits": ". ",
"author(s)": "Qru mbino (J. C. F.)",
"title": "Clairvoyance : the system of philosophy concerning the divinity of clair. voyance, also a treatise on divination and crystal reading.",
"format": "Cr. 8vo.",
"little_bits": "7} x 41, pp. 140, 25. 6d. net",
"publisher": "FOWLER",
"date": "Mar. 15"
}}

—

Example input:
Admiralty-Hydrographic. China Sea pilot. Vol. I, 1916 : Rev. Supp., 1918. Vol. 2, 1915 : Rev. supp., 1918. Vol. 3, 1912 : Rev. supp. (11), 1918. Vol. 4, 1912 : Rev. supp. (2), 1917. Vol. 5, 1912 : Rev. slipp. (11), 1918 POTTER, Oct., Feb., June, Apr. '18

Example output:

{{
"opening_bits": "",
"author(s)": "",
"title": "Admiralty-Hydrographic. China Sea pilot. Vol. I, 1916 : Rev. Supp., 1918. Vol. 2, 1915 : Rev. supp., 1918. Vol. 3, 1912 : Rev. supp. (11), 1918. Vol. 4, 1912 : Rev. supp. (2), 1917. Vol. 5, 1912 : Rev. slipp. (11), 1918",
"format": "",
"little_bits": "",
"publisher": "POTTER",
"date": "Oct., Feb., June, Apr. '18"
}}

—

Example input:
Some facts relating to internal respiration, from personal observation covering a period of over torty years, 1866-1910. By “ H. B." 71 x 5, pp. 62, 2s.6d. net (Portobello, N.B.: 11, Rosefield Pl.) J. THOMSON, Feb. '18


Example output:

{{
"opening_bits": "",
"author(s)": "",
"title": "Some facts relating to internal respiration, from personal observation covering a period of over torty years, 1866-1910. By \" H. B.\"",
"format": "",
"little_bits": "71 x 5, pp. 62, 2s.6d. net (Portobello, N.B.: 11, Rosefield Pl.)",
"publisher": "J. THOMSON",
"date": "Feb. '18"
}}

—

Now here is the real input:

"""


def clean_parse(entry_str):
    """Parse gemini's JSON output, stripping markdown fences if present."""
    s = entry_str.strip()
    if s.startswith("```json"):
        s = s[len("```json"):].strip()
    if s.endswith("```"):
        s = s[:-3].strip()

    try:
        return json.loads(s)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(s)  # fallback for weird quotes
        except Exception as e:
            raise ValueError(f"Clean parse failed: {e}")


def check_internet_connection(host="8.8.8.8", port=53, timeout=3):
    """Check internet by attempting a connection to Google DNS."""
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error:
        return False

def wait_for_internet(pause_minutes=1, max_wait_minutes=None):
    """Wait for internet to be available, pausing if down."""
    waited = 0
    while not check_internet_connection():
        print(f"[!] Internet down. Pausing for {pause_minutes} minute(s)... (Waited {waited} min)")
        time.sleep(pause_minutes * 60)
        waited += pause_minutes
        if max_wait_minutes and waited >= max_wait_minutes:
            raise TimeoutError("Internet not restored within max wait time. Aborting.")

mega_error_list = {}

# loop through each hand-corrected entries CSV
for file in sorted(corrected_entries_directory.iterdir()):

    if file.name.startswith('.'):
        continue

    # extract year identifier like 'entries_1912'
    file_name_regex = r"entries_[0-9]{4}"
    file_name_match = re.findall(file_name_regex, str(file))
    if not file_name_match:
        print(f"⚠️ Skipping file with unexpected name format: {file.name}")
        continue
    file_name = file_name_match[0]

    error_list = []

    entries_df = pd.read_csv(file)

    # some CSVs have mangled column names from R export
    cols = list(entries_df.columns)
    if cols[2] == "entry.1":
        cols[2] = "page_num"
    if cols[3] == "entry.2":
        cols[3] = "doc_page_num"
    entries_df.columns = cols

    print(entries_df.columns)

    # only parse entries flagged as main (have publisher + date)
    main_entries_df = entries_df[entries_df["main_entry"] == True][["entry", "page_num", "doc_page_num"]].reset_index(drop=True)
    main_entries = main_entries_df["entry"]

    with tqdm(total=len(main_entries)) as progress_bar:
        progress_bar.set_description(f"Processing {file_name}")

        batch_num = 1

        for i in range((batch_num-1)*file_batch_size, len(main_entries), file_batch_size):

            # set up batch output file
            batch_file_number = (i // file_batch_size) + 1
            batch_filename = f"{file_name}_batch_{batch_file_number}.csv"
            batch_directory = parsed_dataframe_directory / file_name
            batch_directory.mkdir(exist_ok=True, parents=True)
            batch_file = batch_directory / batch_filename

            # skip already-completed batches (resume support)
            if batch_file.exists():
                try:
                    existing_df = pd.read_csv(batch_file)
                    actual_batch_size = len(main_entries[i:i+file_batch_size])
                    if len(existing_df) >= actual_batch_size:
                        print(f"Batch file {batch_filename} already exists with {len(existing_df)} rows, skipping batch...")
                        progress_bar.update(len(main_entries[i:i+file_batch_size]))
                        continue
                    else:
                        print(f"Batch file {batch_filename} exists but has only {len(existing_df)} rows, reprocessing batch...")
                except Exception as e:
                    print(f"Error reading existing batch file {batch_filename}: {e}. Reprocessing batch...")

            # send each entry to gemini one at a time
            results = []

            for entry in main_entries[i:i+file_batch_size]:
                wait_for_internet(pause_minutes=1)

                try:
                    response = client.models.generate_content(
                        model="gemini-2.5-flash",
                        contents=prompt + str(entry),
                        config=types.GenerateContentConfig(
                            temperature=0,
                            max_output_tokens=500,
                            thinking_config=types.ThinkingConfig(thinking_budget=0)  # no chain-of-thought
                        )
                    )
                    output = response.text.strip()
                    results.append(output)
                    time.sleep(1.2)  # rate limit

                except Exception as e:
                    error_list.append({"entry": entry, "error": str(e)})
                    print(f"Error in batch {batch_num} on entry {i}: {e}")
                    results.append({"input": entry, "output": "ERROR"})

            # parse gemini's JSON responses
            parsed = []
            for j, entry in enumerate(results):
                try:
                    parsed.append(clean_parse(entry))
                except Exception as err:
                    error_list.append({"entry": entry, "error": str(err)})
                    print(f"Error parsing entry {j}: {err}")
                    parsed.append({})

            # write batch CSV with original entry + page info + parsed fields
            with open(batch_file, "w", newline="", encoding="utf-8") as f:
                combined_rows = []
                page_info_batch = main_entries_df.iloc[i:i+file_batch_size]

                for idx, item in enumerate(parsed):
                    original_entry = page_info_batch.iloc[idx]["entry"]
                    page_num = page_info_batch.iloc[idx]["page_num"]
                    doc_page_num = page_info_batch.iloc[idx]["doc_page_num"]

                    if isinstance(item, dict):
                        row = {**item, "original_entry": original_entry, "page_num": page_num, "doc_page_num": doc_page_num}
                        combined_rows.append(row)
                    elif isinstance(item, list):  # gemini sometimes returns a list of dicts
                        for subitem in item:
                            if isinstance(subitem, dict):
                                row = {**subitem, "original_entry": original_entry, "page_num": page_num, "doc_page_num": doc_page_num}
                                combined_rows.append(row)
                            else:
                                print("⚠️ Skipping unexpected subitem type:", type(subitem), subitem)
                    else:
                        print("⚠️ Skipping unexpected item type:", type(item), item)

                final_fieldnames = ["original_entry", "page_num", "doc_page_num"] + fieldnames

                writer = csv.DictWriter(f, fieldnames=final_fieldnames)
                writer.writeheader()
                for row in combined_rows:
                    writer.writerow({k: row.get(k, "") for k in final_fieldnames})

            progress_bar.update(len(main_entries[i:i+file_batch_size]))

    mega_error_list.update({file_name: error_list})

    # save errors after each year (so we don't lose them on crash)
    error_list_filename = f"mega_error_list_{file_name}.json"
    with open(error_list_filename, "w", encoding="utf-8") as f:
        json.dump(mega_error_list, f, indent=2, ensure_ascii=False)

print("Processing complete.")
