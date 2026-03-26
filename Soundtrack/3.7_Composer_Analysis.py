import streamlit as st
import os, sys

st.set_page_config(page_title="3.7 Composer Analysis", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# Standard library imports
import re
import shutil

# Third-party imports
import pandas as pd

# LOAD THE DATA
# We are loading the three main files needed for the analysis
df_albums = pd.read_csv('/work/pipeline/3.6.Albums_vote_count_analysis.csv')
df_bridge = pd.read_csv('/work/pipeline/2.2.MUSICBRAINZ_mv_tmdb_soundtrack_album_artist_bridge_2015_2025.csv')
df_wide   = pd.read_csv('/work/pipeline/3.6.Wide_vote_count_analysis.csv')

# 1. --- DATA QUALITY ANALYSIS ---

# Define the splitting logic to reuse it
def get_tmdb_list(raw_val):
    raw_str = str(raw_val).strip()
    if raw_str.lower() in ['unknown', 'nan', '']:
        return []
    # Split by comma or semicolon and remove whitespace
    return [name.strip() for name in re.split(r',|;', raw_str) if name.strip()]

# Apply the helper function to get lists of composers
tmdb_composer_lists = df_albums['film_soundtrack_composer_raw'].apply(get_tmdb_list)

# 1. Count Unknowns
unknown_count = tmdb_composer_lists.apply(lambda x: len(x) == 0).sum()

# 2. Count Unranked (Multiple Composers)
# We look for lists with a length greater than 1
unranked_mask = tmdb_composer_lists.apply(lambda x: len(x) > 1)
unranked_count = unranked_mask.sum()

# 3. Calculate Percentages
total_rows = len(df_albums)
unknown_pct = (unknown_count / total_rows) * 100
unranked_pct = (unranked_count / total_rows) * 100

print(f"--- TMDB Composer Analysis ---")
print(f"Total Albums Analyzed: {total_rows}")
print(f"Unknown Composers:     {unknown_count} ({unknown_pct:.2f}%)")
print(f"Unranked (Multiple):   {unranked_count} ({unranked_pct:.2f}%)")
print(f"------------------------------\n")

# To see a few examples of "Unranked" rows:
if unranked_count > 0:
    print("Examples of Unranked Composers:")
    print(df_albums[unranked_mask]['film_soundtrack_composer_raw'].head(5))
    print("\n")

# 2. BUILD A "TRUTH MAP" FROM MUSICBRAINZ
# We want to know EVERY artist MusicBrainz associates with a specific movie album.
# We create a dictionary where:
# Key = (TMDB ID + Release ID)
# Value = A LIST of all names found in MusicBrainz
mb_truth_map = (
    df_bridge[df_bridge['credited_name'] != 'Various Artists']
    .groupby(['tmdb_id', 'release_group_id'])['credited_name']
    .apply(list)  # This puts all names into a simple list we can check later
    .to_dict()
)

# 3. DEFINE THE DECISION LOGIC
def find_the_best_composer(row):
    """
    This function looks at TMDB and MusicBrainz and picks one name.
    """

    # --- Part A: Get TMDB Names ---
    tmdb_raw = str(row['film_soundtrack_composer_raw'])

    # If the cell is empty or says 'Unknown', we start with an empty list
    if tmdb_raw.lower() in ['unknown', 'nan', '']:
        tmdb_list = []
    else:
        # We split "Name A, Name B" into ['Name A', 'Name B']
        # We handle both commas (,) and semicolons (;)
        tmdb_list = [name.strip() for name in re.split(r',|;', tmdb_raw) if name.strip()]

    # --- Part B: Get MusicBrainz Names ---
    # We look up the list of names we saved in our 'Truth Map' earlier
    key = (row['tmdb_id'], row['release_group_id'])
    mb_list = mb_truth_map.get(key, [])

    # --- Part C: The "Handshake" (Cross-Check) ---
    # We check: Is there a name that exists in BOTH TMDB and MusicBrainz?
    # This is the most accurate way to verify composer.
    for t_name in tmdb_list:
        for m_name in mb_list:
            if t_name.lower() == m_name.lower():
                return m_name # Found a match! Return the clean MB name.

    # --- Part D: Fallbacks ---
    # If no match was found, just take the very first name from the TMDB list.
    if len(tmdb_list) > 0:
        return tmdb_list[0]

    # If TMDB was empty, try taking the first name from the MusicBrainz list.
    if len(mb_list) > 0:
        return mb_list[0]

    # If everything is empty, return Unknown
    return 'Unknown'

# 4. RUN THE LOGIC ON THE ALBUMS FILE
# This creates the new column 'composer_primary_clean'
df_albums['composer_primary_clean'] = df_albums.apply(find_the_best_composer, axis=1)

# 5. SYNC THE RESULTS TO THE "WIDE" FILE
# We create a small lookup table so we can copy the new names to the Wide file.
composer_lookup = (
    df_albums[['tmdb_id', 'release_group_id', 'composer_primary_clean']]
    .drop_duplicates(['tmdb_id', 'release_group_id'])
)

# We use 'merge' to attach the new column to the Wide file
df_wide = pd.merge(
    df_wide,
    composer_lookup,
    on=['tmdb_id', 'release_group_id'],
    how='left'
)

# 6. SAVE EVERYTHING
df_albums.to_csv('/work/pipeline/3.7.Albums_composer_analysis.csv', index=False)
df_wide.to_csv('/work/pipeline/3.7.Wide_composer_analysis.csv', index=False)

print("Process Complete! Files saved with the 'composer_primary_clean' column.")

# Artist and track CSV's carry over

# Question for Jaime/Tony: Should we do anything for Artist level here?

import shutil

shutil.copy(
    "./pipeline/3.6.Artists_vote_count_analysis.csv",
    "./pipeline/3.7.Artists_composer_analysis.csv"
)

shutil.copy(
    "./pipeline/3.6.Tracks_vote_count_analysis.csv",
    "./pipeline/3.7.Tracks_composer_analysis.csv"
)
