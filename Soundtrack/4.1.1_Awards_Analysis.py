import streamlit as st
import os, sys

st.set_page_config(page_title="4.1.1 Awards Analysis", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(DATA_DIR)
# Ensure repo root is on path so utils/ can be imported from any subdirectory
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if DATA_DIR not in sys.path:
    sys.path.insert(0, DATA_DIR)

# Standard library imports
import re
import unicodedata

# Third-party imports
import numpy as np
import pandas as pd

# ==========================================================
# 1. Text Normalizer
# Standardizes titles to ensure maximum match rates
# ==========================================================
def robust_normalize(text):
    """
    Standardizes text by removing accents, special characters,
    and extra whitespace to improve join match rates.
    """
    if not isinstance(text, str):
        return ""

    # Remove Accents
    t = text.lower().strip()
    t = "".join(c for c in unicodedata.normalize('NFD', t) if unicodedata.category(c) != 'Mn')
    # Strip "The " prefix
    if t.startswith("the "): t = t[4:]
    # Standardize ampersands
    t = t.replace(" & ", " and ")
    # Convert Roman Numerals (I to X) like frozen ii -> Frozen 2
    roman_map = {r'\bi\b': '1', r'\bii\b': '2', r'\biii\b': '3', r'\biv\b': '4',
                 r'\bv\b': '5', r'\bvi\b': '6', r'\bvii\b': '7', r'\bviii\b': '8',
                 r'\bix\b': '9', r'\bx\b': '10'}
    for roman, digit in roman_map.items():
        t = re.sub(roman, digit, t)
    # Remove all punctuation like La La Land! -> la la land
    # Special character stripping like Dune: Part Two -> dune part two
    # Froeign character handling like Amélie -> amelie
    t = re.sub(r'[^a-z0-9 ]', '', t)
    return " ".join(t.split())

# ==========================================================
# 2. THE Boolean FLAG GENERATOR (15 Requested Columns)
# ==========================================================
def calculate_award_flags(matches):
    res = {
        'award_year': 0,
        'oscar_score_nominee': False, 'oscar_score_winner': False,
        'oscar_song_nominee': False, 'oscar_song_winner': False,
        'globes_score_nominee': False, 'globes_score_winner': False,
        'globes_song_nominee': False, 'globes_song_winner': False,
        'critics_score_nominee': False, 'critics_score_winner': False,
        'critics_song_nominee': False, 'critics_song_winner': False,
        'bafta_score_nominee': False, 'bafta_score_winner': False
    }
    if matches.empty: return pd.Series(res)
    res['award_year'] = int(matches['Year'].max())
    for _, row in matches.iterrows():
        cer, cat = row['Ceremony'], str(row['Category']).lower()
        win = str(row['Winner']).strip().lower() == 'yes'
        is_score = ('score' in cat or 'sound' in cat)
        is_song = ('song' in cat)

        # Mapping Ceremony + Category to Flags
        mapping = {
            'Oscars': ('oscar_score', 'oscar_song'),
            'Golden Globes': ('globes_score', 'globes_song'),
            'Critics Choice': ('critics_score', 'critics_song'),
            'BAFTA': ('bafta_score', None)
        }
        if cer in mapping:
            score_prefix, song_prefix = mapping[cer]
            if is_score and score_prefix:
                res[f'{score_prefix}_nominee'] = True
                if win: res[f'{score_prefix}_winner'] = True
            if is_song and song_prefix:
                res[f'{song_prefix}_nominee'] = True
                if win: res[f'{song_prefix}_winner'] = True
    return pd.Series(res)

# ==========================================================
# 3. GLOBAL SETUP
# ==========================================================
awards_df = pd.read_csv('./pipeline/4.1.All_Awards_2015_2025.csv')
awards_df['Film_Key'] = awards_df['Film'].apply(robust_normalize)
awards_df['Nominee_Key'] = awards_df['Nominee'].fillna('').str.lower().str.strip()

# ==========================================================
# 4. PROCESSING ALL 4 LEVELS
# ==========================================================

# --- LEVEL 1: ARTISTS ---
print("Processing Artists...")
artists_df = pd.read_csv('./pipeline/3.7.Artists_composer_analysis.csv')
artist_results = artists_df.apply(lambda r: calculate_award_flags(
    awards_df[awards_df['Nominee_Key'].str.contains(str(r['name']).lower().strip(), na=False, regex=False)]
), axis=1)
pd.concat([artists_df, artist_results], axis=1).to_csv('./pipeline/4.1.1.Artists_awards_appended.csv', index=False)

# --- LEVEL 2: ALBUMS ---
print("Processing Albums...")
albums_df = pd.read_csv('./pipeline/3.7.Albums_composer_analysis.csv')
album_results = albums_df.apply(lambda r: calculate_award_flags(
    awards_df[awards_df['Film_Key'] == robust_normalize(r['film_title'])]
), axis=1)
pd.concat([albums_df, album_results], axis=1).to_csv('./pipeline/4.1.1.Albums_awards_appended.csv', index=False)

# --- LEVEL 3: TRACKS ---
print("Processing Tracks...")
tracks_df = pd.read_csv('./pipeline/3.7.Tracks_composer_analysis.csv')
# Tracks match based on the film they belong to
track_results = tracks_df.apply(lambda r: calculate_award_flags(
    awards_df[awards_df['Film_Key'] == robust_normalize(r['film_title'])]
), axis=1)
pd.concat([tracks_df, track_results], axis=1).to_csv('./pipeline/4.1.1.Tracks_awards_appended.csv', index=False)

# --- LEVEL 4: WIDE FILE ---
print("Processing Wide File...")
wide_df = pd.read_csv('./pipeline/3.7.Wide_composer_analysis.csv')
wide_results = wide_df.apply(lambda r: calculate_award_flags(
    awards_df[
        (awards_df['Film_Key'] == robust_normalize(r['film_title'])) |
        (awards_df['Nominee_Key'].str.contains(str(r['recording_artist_credit']).lower().strip(), na=False, regex=False))
    ]
), axis=1)
pd.concat([wide_df, wide_results], axis=1).to_csv('./pipeline/4.1.1.Wide_awards_appended.csv', index=False)

print("Done! All 4 files saved to ./pipeline/ with suffix 4.1.1")

import pandas as pd

# Paths to the generated files
files = {
    "Artists": "./pipeline/4.1.1.Artists_awards_appended.csv",
    "Albums": "./pipeline/4.1.1.Albums_awards_appended.csv",
    "Tracks": "./pipeline/4.1.1.Tracks_awards_appended.csv",
    "Wide": "./pipeline/4.1.1.Wide_awards_appended.csv"
}

# The specific films we want to verify are now matching
test_films = ['Frozen 2', 'Belfast', 'Tár', 'The Boy and the Heron', "Don't Look Up", 'Guillermo del Toro\'s Pinocchio']

def perform_qa():
    print("====================================================")
    print("         QA ANALYSIS: AWARDS MATCHING (v4.1)")
    print("====================================================\n")

    for level, path in files.items():
        try:
            df = pd.read_csv(path)

            # 1. Basic Stats
            total_rows = len(df)
            matched_rows = df[df['award_year'] > 0]
            count_matched = len(matched_rows)
            match_rate = (count_matched / total_rows) * 100 if total_rows > 0 else 0

            print(f"--- LEVEL: {level.upper()} ---")
            print(f"File Path: {path}")
            print(f"Total Records: {total_rows:,}")
            print(f"Records with Award Matches: {count_matched:,} ({match_rate:.2f}%)")

            # 2. Check for Specific Film Matches (for Albums, Tracks, and Wide)
            if level in ['Albums', 'Tracks', 'Wide']:
                found_test_films = df[df['film_title'].isin(test_films)]
                matched_test_films = found_test_films[found_test_films['award_year'] > 0]['film_title'].unique()

                print(f"Priority Film Matches found: {list(matched_test_films)}")

                missing = [f for f in test_films if f in df['film_title'].unique() and f not in matched_test_films]
                if missing:
                    print(f"⚠️  STILL MISSING: {missing}")
                else:
                    print(f"✅ All priority films present in this file were matched!")

            # 3. Artist Specific Check (Top winners)
            if level == 'Artists' or level == 'Wide':
                top_winners = df[df['oscar_score_winner'] == True]
                if not top_winners.empty:
                    # Depending on column names in your source, we use 'name' or 'recording_artist_credit'
                    name_col = 'name' if level == 'Artists' else 'recording_artist_credit'
                    winners = top_winners[name_col].unique()[:5]
                    print(f"Sample Oscar Winners matched: {list(winners)}")

            print("-" * 30 + "\n")

        except Exception as e:
            print(f"Error analyzing {level}: {e}\n")

if __name__ == "__main__":
    perform_qa()
