import streamlit as st
import os, sys

st.set_page_config(page_title="4.1 Awards Data Scraper", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# Third-party imports
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
# User-Agent identifies our script to Wikipedia to avoid being blocked
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
# Range includes 2015 up to 2025 (stop at 2026)
TARGET_YEARS = range(2015, 2026)

def clean_text(text):
    """
    Standardizes text by removing Wikipedia citations (e.g., [1]),
    stripping quotes, and cleaning up whitespace.
    """
    text = re.sub(r'\[.*\]', '', text)
    return text.replace('"', '').replace("'", "").strip()

def get_award_data(url, ceremony_name, category_name):
    """
    The main engine that parses Wikipedia 'wikitables'.
    It dynamically maps columns and handles 'rowspans' where a single Year
    cell covers multiple nominee rows.
    """
    print(f"Processing {ceremony_name}: {category_name}...")

    try:
        response = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return pd.DataFrame()

    valid_rows = []

    # Wikipedia stores award lists in 'wikitable' classes
    for table in soup.find_all('table', class_='wikitable'):

        # 1. COLUMN MAPPING
        # We find the header row to determine which index belongs to Film, Nominee, etc.
        headers_row = table.find('tr')
        if not headers_row: continue
        header_cells = [th.get_text(strip=True).lower() for th in headers_row.find_all(['th', 'td'])]

        col_map = {}
        for i, h in enumerate(header_cells):
            if any(word in h for word in ['film', 'motion picture', 'work']):
                col_map['film'] = i
            if any(word in h for word in ['nominee', 'composer', 'recipient', 'music', 'sound', 'artist', 'writer']):
                col_map['nominee'] = i
            if 'year' in h:
                col_map['year'] = i
            if 'song' in h:
                col_map['song_title'] = i

        # If we can't find a 'film' column, this table likely isn't a main award list
        if 'film' not in col_map: continue

        current_year = ""
        rows = table.find_all('tr')[1:] # Skip the header row

        for row in rows:
            cells = row.find_all(['td', 'th'])
            if not cells: continue

            # 2. ROWSPAN & YEAR DETECTION
            # In many tables, the 'Year' cell is only present in the first row of a group.
            # We look for a 4-digit number in the first cell to update our current_year.
            first_cell_text = cells[0].get_text(strip=True)
            year_match = re.search(r'(\d{4})', first_cell_text)
            has_year_cell = False

            if year_match and len(first_cell_text) <= 15:
                current_year = year_match.group(1)
                has_year_cell = True

            # Filter out years outside our 2015-2025 requirement
            if not current_year or int(current_year) not in TARGET_YEARS:
                continue

            # 3. WINNER IDENTIFICATION
            # Different pages use different background colors or Bold tags for winners.
            # We check for blue, green, or gold background styles and <b> tags.
            is_winner = "Nominee"
            style = row.get('style', '').upper()
            winner_colors = ['#B0C4DE', '#CCFFCC', '#FAEB86', 'GOLD', 'CHARTREUSE']

            if any(color in style for color in winner_colors) or row.find('b'):
                is_winner = "Yes"

            # 4. INDEX SHIFTING
            # If a row DOES NOT have a year cell (inherited from the row above),
            # all subsequent data indices (Film, Nominee) shift left by 1.
            f_idx, n_idx = col_map['film'], col_map['nominee']
            s_idx = col_map.get('song_title')

            if not has_year_cell and 'year' in col_map:
                f_idx -= 1
                n_idx -= 1
                if s_idx is not None: s_idx -= 1

            # 5. DATA EXTRACTION
            try:
                film = clean_text(cells[f_idx].get_text(strip=True))
                nominee = clean_text(cells[n_idx].get_text(strip=True))

                # For "Best Song", we often want "Song Title - Artist" as the Nominee
                if s_idx is not None:
                    song_title = clean_text(cells[s_idx].get_text(strip=True))
                    nominee = f"{song_title} - {nominee}"

                valid_rows.append({
                    'Year': current_year,
                    'Ceremony': ceremony_name,
                    'Category': category_name,
                    'Nominee': nominee,
                    'Film': film,
                    'Winner': is_winner
                })
            except (IndexError, KeyError):
                # This skips 'Notes' rows or meta-rows that don't match the data format
                continue

    return pd.DataFrame(valid_rows)

# --- EXECUTION ---
if __name__ == "__main__":
    # List of tasks: (Wikipedia URL, Ceremony Display Name, Category Display Name)
    tasks = [
        ("https://en.wikipedia.org/wiki/Academy_Award_for_Best_Original_Score", "Oscars", "Best Original Score"),
        ("https://en.wikipedia.org/wiki/Academy_Award_for_Best_Original_Song", "Oscars", "Best Original Song"),
        ("https://en.wikipedia.org/wiki/BAFTA_Award_for_Best_Sound", "BAFTA", "Best Sound"),
        ("https://en.wikipedia.org/wiki/Critics%27_Choice_Movie_Award_for_Best_Score", "Critics Choice", "Best Score"),
        ("https://en.wikipedia.org/wiki/Critics%27_Choice_Movie_Award_for_Best_Song", "Critics Choice", "Best Song"),
        ("https://en.wikipedia.org/wiki/Golden_Globe_Award_for_Best_Original_Score", "Golden Globes", "Best Original Score"),
        ("https://en.wikipedia.org/wiki/Golden_Globe_Award_for_Best_Original_Song", "Golden Globes", "Best Original Song")
    ]

    all_data = []
    for url, ceremony, category in tasks:
        df = get_award_data(url, ceremony, category)
        all_data.append(df)

    # Combine all individual category DataFrames into one master list
    master_df = pd.concat(all_data, ignore_index=True).drop_duplicates()

    # Sort for clarity: Newest Years first, then Ceremony name
    master_df = master_df.sort_values(by=['Year', 'Ceremony', 'Category'], ascending=[False, True, True])

    # Save to a single CSV file
    master_df.to_csv("/work/pipeline/4.1.All_Awards_2015_2025.csv", index=False, quoting=1)
    print(f"\nDone! Saved {len(master_df)} records to '4.1.All_Awards_2015_2025.csv'.")
