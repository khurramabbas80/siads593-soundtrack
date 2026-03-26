import streamlit as st
import os, sys

st.set_page_config(page_title="3.5.1 Derive Film Genre Columns", layout="wide")

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

import pandas as pd

filepath = './pipeline/3.5.Wide_exploded_genre.csv'
df = pd.read_csv(filepath)
df.head()

# Handle missing values to avoid errors during string split
df['film_genres'] = df['film_genres'].fillna('')

# Extract all unique genres from the entire dataset
unique_genres = set()
for entry in df['film_genres']:
    if entry:
        # Split by comma and strip whitespace from each genre
        genres = [g.strip() for g in entry.split(',')]
        unique_genres.update(genres)

# Sort them alphabetically for consistent column ordering
sorted_genres = sorted(list(unique_genres))
print(f"Detected Genres: {sorted_genres}")

# Create a binary column for each unique genre found
for genre in sorted_genres:
    # Format the column name (e.g., 'Science Fiction' becomes 'film_is_science_fiction')
    col_name = f"film_is_{genre.lower().replace(' ', '_')}"

    # Check if the genre string exists within the comma-separated list for each row
    df[col_name] = df['film_genres'].apply(
        lambda x: genre in [g.strip() for g in x.split(',')] if x else False
    )

df.head()

# Save the result to a new CSV file
output_file = './pipeline/3.5.Wide_exploded_genre.csv'
df.to_csv(output_file, index=False)

print(f"Transformation complete. Saved to {output_file}")

st.markdown("""
Below is really the same code, I modularized it so we can run this for albums 
""")
