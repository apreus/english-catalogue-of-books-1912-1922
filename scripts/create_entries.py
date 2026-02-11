import os
import re
import csv
import sys
from tqdm import tqdm
import argparse

def argparse_create(args):
    parser = argparse.ArgumentParser(description='Argument parser for creating the genereated dataset CSVs.')
    parser.add_argument("--verbose", type=str,
            help="Prints out clean entry metrics into the CLI.",
            default="False")
    parsed_args = parser.parse_args(args)
    return parsed_args

def remove_patterns(page, patterns):
    """Strip header/page-number lines from a single page."""
    for pattern in patterns:
        page = re.sub(pattern, '', page, flags=re.MULTILINE)
    return page

def get_splitters_by_year(year):
    """Load year-specific front-matter, appendix, and year-variation regexes from splitters.txt."""
    with open("splitters.txt", "r") as file:
            exec(file.read(), globals())
    return (patternFrontDict[year], appendixPatternDict[year], yearPatterns[year])

def get_entries(year_string, file_path, pattern, verbose):
    """Extract all entries from a single ECB OCR file for a given year."""
    infile = open(file_path, "r", encoding="utf-8", errors="ignore")
    contents = infile.read()
    infile.close()

    if verbose:
        print("CATALOGUE YEAR:", year_string, "\n")

    front_pattern, appendix_pattern, year_variations = get_splitters_by_year(year_string)

    # chop off front matter (intro pages) and back matter (appendix)
    text_raw = re.split(front_pattern, contents)
    if len(text_raw) < 2:
        print("The year that's not working is: ", year_string)
        print(front_pattern)
        raise IndexError(f"No match found for patternFront: {front_pattern} in ecb_content.")

    front_matter = text_raw[0]
    document_page_delta = len(front_matter.split("\f")) - 2  # offset to get real doc page numbers

    ecb_content = text_raw[1]

    appendix_list = re.split(appendix_pattern, ecb_content, flags=re.DOTALL)
    if len(appendix_list) < 2:
        print("The year that's not working is: ", year_string)
        print(appendix_pattern)
        raise IndexError(f"No match found for appendix_pattern: {appendix_pattern} in ecb_content.")

    ecb_content = appendix_list[0]

    # split on form feeds into pages, strip headers from each
    ecb_pages = ecb_content.split("\f")
    ecb_pe = [remove_patterns(page, pattern) for page in ecb_pages]

    # mark entry boundaries: lines ending with a year variant (e.g. "'17") get tagged + cut
    entry_terminator_regex = r'(\W({})\.?$)'.format('|'.join(year_variations))
    ecb_pe = [re.sub(entry_terminator_regex, "<PAGE_NUM:{}><DOCUMENT_PAGE_NUM:{}>\\1<ENTRY_CUT>".format(i, i+document_page_delta), page, flags=re.M) for i, page in enumerate(ecb_pe, start=1)]
    ecb_pe = [re.split(r"<ENTRY_CUT>", page, flags=re.M) for page in ecb_pe]

    # flatten pages into one entry list, collapsing newlines
    entries = [
        re.sub(r"\n", " ", entry.strip()) for entries in ecb_pe for entry in entries
    ]

    total_entries = len(entries)

    if verbose:
        print(f"Total Entries: {total_entries}")

    # fix line-mid entries: OCR sometimes merges two entries on one line
    # detect them by finding month+year patterns that aren't at end of line
    month_abbrvs = [
        "Jan", "Feb", "Mar", "Apr", "May", "June",
        "July", "Aug", "Sept", "Oct", "Nov", "Dec",
    ]

    line_mid_re = re.compile(r".*({})\.?\W{}\.?[^\.]+".format("|".join(month_abbrvs),year))
    line_mid_entries = [entry for entry in entries if line_mid_re.search(entry)]

    if verbose:
        print(f"\nTotal Line Mid Entries: {len(line_mid_entries)}")
        print(f"Percent Line Mid Entries: {len(line_mid_entries) / len(entries)}")

    # split each line-mid entry into two separate entries
    split_line_mid_re = re.compile(r"(({})\.?\W{}\.?(?!$))".format("|".join(month_abbrvs), year))
    line_mid_index = [entries.index(entry) for entry in line_mid_entries]

    counter = 0  # tracks index shift from insertions
    for index in line_mid_index:
        match = re.search(r"<PAGE_NUM:([0-9]{0,3})><DOCUMENT_PAGE_NUM:([0-9]{0,3})>", entries[index + counter])
        if match:
            # carry page tags to both halves
            page_num, document_page_num = match.group(1), match.group(2)
            entries[index + counter] = re.sub(split_line_mid_re, "<PAGE_NUM:{}><DOCUMENT_PAGE_NUM:{}>\\1<ENTRY_CUT>\\1<ENTRY_CUT>".format(page_num, document_page_num), entries[index + counter])
        else:
            print("main is empty, here's the index", index+counter)
            entries[index + counter] = re.sub(split_line_mid_re, "\\1<ENTRY_CUT>", entries[index + counter])
        new_entry = re.split(r"<ENTRY_CUT>", entries[index + counter], flags=re.M)
        new_entry[1] = re.sub(r"^\W+(?=[A-Z])", "", new_entry[1])  # strip leading junk from second half
        entries[index + counter] = new_entry[1]
        entries.insert(index + counter, new_entry[0])
        counter += 1

    if verbose:
        print(f"\nNew Total Entries After Line Mid Correction: {len(entries)}")

    return entries

if __name__ == "__main__":

    args = argparse_create((sys.argv[1:]))

    #verbose_string = args.verbose
    verbose_string = "True"

    if verbose_string == "True":
        verbose = True
    else:
        verbose = False

    data_folder_path = '/ecb_ocr_text/'
    entries_directory = "/entries/"

    # loop through 1902-1922
    for year in tqdm(range(2,23)):
        if year < 10:
            year = "0" + str(year)

        year_string = str(year)

        cwd_path = os.path.abspath(os.getcwd()).replace("scripts", "")

        # pick the right OCR file: princeton re-scans for early years, nypl for 1919/1921
        if int(year) < 8:
            file_name = "ecb_19" + year_string + "_princeton_070724.txt"
            file_path = cwd_path + os.path.join(data_folder_path, file_name)

        elif year == 19 or year == 21:
            file_name = "ecb_19" + year_string + "_nypl_070724.txt"
            file_path = cwd_path + os.path.join(data_folder_path, file_name)

        else:
            file_name = "ecb_19" + year_string + ".txt"
            file_path = cwd_path + os.path.join(data_folder_path, file_name)

        header_patterns = [
            r"(^\b[A-Z ]+\b\s?\n)",  # alphabetical guide words (e.g. "ABBOTT—ADAMS")
            r"(##(?s:.*?)$)",  # page number markers
            r"(^.?19{}.?\n)".format(year), # year in header
            r"(^\d+\n)", # stray page numbers
        ]

        entries = get_entries(year_string, file_path, header_patterns, verbose)

        # Save entries to CSV
        if not os.path.exists(f"{cwd_path}/{entries_directory}"):
            os.makedirs(f"{cwd_path}/{entries_directory}")

        with open(f"{cwd_path}/{entries_directory}/entries_19{year_string}.csv",
            "w", newline='', encoding="utf-8", errors="ignore") as f:
            csv_writer = csv.writer(f, quotechar='"')
            for entry in entries:
                csv_writer.writerow([entry])
