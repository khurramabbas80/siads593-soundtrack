import streamlit as st
import os, sys

st.set_page_config(page_title="3.5.2 Genre Analysis between Film and Album", layout="wide")

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

st.markdown("""
This script calculates the % of films within each of the film genres that feature specific album genres and generates the heatmap to visualiz the matching strength\\. 


""")

# Standard library imports
import os

# Third-party imports
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Load the data
df = pd.read_csv('./pipeline/3.5.Wide_exploded_genre.csv')

# 2. Map raw column names to clean display labels
album_genre_map = {
    'album_ambient_experimental': 'Ambient Experimental',
    'album_classical_orchestral': 'Classical Orchestral',
    'album_electronic': 'Electronic',
    'album_hip_hop_rnb': 'Hip Hop/RnB',
    'album_pop': 'Pop',
    'album_rock': 'Rock',
    'album_world_folk': 'World/Folk'
}
album_genre_cols = list(album_genre_map.keys())

# 3. Extract and expand Film Genres (since one film can have multiple)
df['film_genres'] = df['film_genres'].fillna('')
unique_film_genres = sorted(list(set([g.strip() for entries in df['film_genres'] for g in entries.split(',') if g])))

# Calculate the "Match Up" Statistics
# We create a matrix showing: What % of [Film Genre] movies have [Album Genre] soundtracks?
stats_matrix = pd.DataFrame(index=unique_film_genres, columns=album_genre_cols)

for f_genre in unique_film_genres:
    # Filter rows containing this specific film genre
    subset = df[df['film_genres'].str.contains(f_genre)]
    total_count = len(subset)

    if total_count > 0:
        for a_genre in album_genre_cols:
            # Count how many of these films have the specific album genre marked as True/1
            matches = subset[a_genre].fillna(0).astype(bool).sum()
            stats_matrix.loc[f_genre, a_genre] = (matches / total_count) * 100
    else:
        stats_matrix.loc[f_genre, :] = 0

# RENAME the columns for a cleaner chart
stats_matrix = stats_matrix.rename(columns=album_genre_map).astype(float)

# Visualize the results
plt.figure(figsize=(12, 8))
sns.heatmap(stats_matrix.astype(float), annot=True, cmap='YlGnBu', fmt='.1f')
plt.title('Matching Study: % of Film Genres featuring specific Album Genres')
plt.xlabel('Album Musical Genres')
plt.ylabel('Film Genres')
plt.savefig('genre_matching_study.png')

# Save the stats to a CSV for further inspection
# stats_matrix.to_csv('film_album_genre_correlation.csv')
