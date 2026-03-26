import streamlit as st
import os, sys

st.set_page_config(page_title="3.6 Vote Count Analysis", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
Due to significant slowdowns with the Soundnet API, we prioritized films with a vote count greater than or equal to 500\\. In this notebook, we perform analysis on well\\-known composers \\(e\\.g\\., John Williams, Hans Zimmer\\) to ensure that this filter isn't too restrictive\\. We also inspect a sample of excluded films as a sanity check\\. Finally, we create a column to flag records greater than or equal to 500 votes for reference in downstream notebooks\\.
""")

# Third-party imports
import pandas as pd

filepath = '/work/pipeline/3.5.Wide_exploded_genre.csv'
df = pd.read_csv(filepath)
df.head()

# Narrow down to the columns of interest

columns = [
    'film_vote_count',
    'recording_artist_credit',
    'track_title',
    'album_title',
    'tmdb_id',
    'film_title',
    'film_revenue',
    'film_genres',
    'film_imdb_id',
    'release_group_mbid',
    'release_mbid',
    'rg_primary_type',
    'rg_secondary_types',
    'album_soundtrack_type',
    'is_canonical_soundtrack',
    'is_canonical_songtrack',
    'album_artist_mbids_text',
    'album_artist_names_text',
    'album_artist_types_text',
    'film_soundtrack_composer_raw',
    'composer_names_text',
    'track_number',
    'track_length_ms',
    'recording_mbid',
    'isrcs_text',
    ]

df_view = df[columns]
df_view.head()

# View total records and nulls

df_view.info()

# Check number of unique films, albums, tracks and artists

unique_film_titles = df_view['film_title'].nunique()
unique_tmdb_id = df_view['tmdb_id'].nunique()
unique_albums = df_view['album_title'].nunique()
unique_tracks = df_view['track_title'].nunique()
unique_artists = df_view['recording_artist_credit'].nunique()

print(f"Number of unique film titles: {unique_film_titles}")
print(f"Number of unique tmdb id's: {unique_tmdb_id}")
print(f"Number of unique album names: {unique_albums}")
print(f"Number of unique track names: {unique_tracks}")
print(f"Number of unique artists: {unique_artists}")

# Determine the number of unique track-artist pairs

grouped_df = df_view.groupby([
    'track_title',
    'recording_artist_credit'
    ]).count()

grouped_df.shape[0]

# Total tracks where artist contains John Williams

williams_df = df_view[df_view["recording_artist_credit"].str.contains("John Williams", case=False, na=False)]
print(f"Number of tracks by John Williams: {len(williams_df)}")
williams_df.head()

# Unique tracks where artist contains John Williams

williams_df = df_view[
    df_view["recording_artist_credit"].str.contains("John Williams", case=False, na=False)
].drop_duplicates(
    subset=["recording_artist_credit", "track_title"]
)
unique_williams = len(williams_df)

print(f"Number of unique John Williams tracks: {unique_williams}")
williams_df.head()

# Total tracks where artist contains Hans Zimmer

zimmer_df = df_view[df_view["recording_artist_credit"].str.contains("Hans Zimmer", case=False, na=False)]
print(f"Number of tracks by Hans Zimmer: {len(zimmer_df)}")
zimmer_df.head()

# Unique tracks where artist contains Hans Zimmer

zimmer_df = df_view[
    df_view["recording_artist_credit"].str.contains("Hans Zimmer", case=False, na=False)
].drop_duplicates(
    subset=["recording_artist_credit", "track_title"]
)

unique_zimmer = len(zimmer_df)
print(f"Number of unique Hans Zimmer tracks: {unique_zimmer}")
zimmer_df.head()

# Total tracks where artist contains Danny Elfman

elfman_df = df_view[df_view["recording_artist_credit"].str.contains("Danny Elfman", case=False, na=False)]
print(f"Number of tracks by Danny Elfman: {len(elfman_df)}")
elfman_df.head()

# Unique tracks where artist contains Danny Elfman

elfman_df = df_view[
    df_view["recording_artist_credit"].str.contains("Danny Elfman", case=False, na=False)
].drop_duplicates(
    subset=["recording_artist_credit", "track_title"]
)

unique_elfman = len(elfman_df)
print(f"Number of unique Danny Elfman tracks: {unique_elfman}")
elfman_df.head()

# Filter the dataframe by films with vote count >= 500

filtered_df = df_view[df_view["film_vote_count"] >= 500]

filtered_df['film_title'].nunique()

# Determine the number of unique track-artist pairs in filtered dataframe

grouped_df = filtered_df.groupby([
    'track_title',
    'recording_artist_credit'
    ]).count()

grouped_df.shape[0]

filtered_df.info()

# Determine the number of unique tracks in filtered data for the major composers

filtered_williams_df = filtered_df[
    filtered_df["recording_artist_credit"].str.contains("John Williams", case=False, na=False)
].drop_duplicates(
    subset=["recording_artist_credit", "track_title"]
)
filtered_unique_williams = len(filtered_williams_df)


filtered_zimmer_df = filtered_df[
    filtered_df["recording_artist_credit"].str.contains("Hans Zimmer", case=False, na=False)
].drop_duplicates(
    subset=["recording_artist_credit", "track_title"]
)

filtered_unique_zimmer = len(filtered_zimmer_df)


filtered_elfman_df = filtered_df[
    filtered_df["recording_artist_credit"].str.contains("Danny Elfman", case=False, na=False)
].drop_duplicates(
    subset=["recording_artist_credit", "track_title"]
)

filtered_unique_elfman = len(filtered_elfman_df)

print(f"Number of unique John Williams tracks after filter: {filtered_unique_williams}")
print(f"Number of unique Hans Zimmer tracks after filter: {filtered_unique_zimmer}")
print(f"Number of unique Danny Elfman tracks after filter: {filtered_unique_elfman}")

# Calculate percentages of the remaining tracks after filtering

pct_williams = filtered_unique_williams / unique_williams
pct_zimmer = filtered_unique_zimmer / unique_zimmer
pct_elfman = filtered_unique_elfman / unique_elfman

print(f"Percentage of Williams tracks remaining after filtering {pct_williams:.0%}")
print(f"Percentage of Zimmer tracks remaining after filtering {pct_zimmer:.0%}")
print(f"Percentage of Elfman tracks remaining after filtering {pct_elfman:.0%}")

filtered_df.sample(10)

# Filtered dataframe sorted by revenue

filtered_df = (
    filtered_df
        .sort_values(by="film_revenue", ascending=False)
        .drop_duplicates(subset="film_title")
)
# auto-detected possible Altair chart: filtered_df
try:
    st.altair_chart(filtered_df, use_container_width=True)
except Exception:
    st.write(filtered_df)

excluded_films_df = df_view[df_view["film_vote_count"] < 500]

excluded_films_df['film_title'].nunique()

excluded_films_df.sample(10)

# Excluded films sorted by revenue

excluded_films_df = (
    excluded_films_df
        .sort_values(by="film_revenue", ascending=False)
        .drop_duplicates(subset="film_title")
)

# auto-detected possible Altair chart: excluded_films_df
try:
    st.altair_chart(excluded_films_df, use_container_width=True)
except Exception:
    st.write(excluded_films_df)

# Adding flag for vote count >= 500 to the original dataframe (all columns)

df['vote_count_above_500'] = df['film_vote_count'] >= 500
df.head()

len(df[df['vote_count_above_500'] == True])

# Exporting wide dataframe with all columns to CSV

out_path = "./pipeline/3.6.Wide_vote_count_analysis.csv"

df.to_csv(out_path, index=False)

# Adding vote count flag to the album CSV

filepath = '/work/pipeline/3.5.Albums_exploded_genre.csv'
df = pd.read_csv(filepath)
df['vote_count_above_500'] = df['film_vote_count'] >= 500

out_path = "./pipeline/3.6.Albums_vote_count_analysis.csv"
df.to_csv(out_path, index=False)

# Artist and track CSV's carry over

import shutil

shutil.copy(
    "./pipeline/3.5.Artists_exploded_genre.csv",
    "./pipeline/3.6.Artists_vote_count_analysis.csv"
)

shutil.copy(
    "./pipeline/3.5.Tracks_exploded_genre.csv",
    "./pipeline/3.6.Tracks_vote_count_analysis.csv"
)

st.markdown("""
# 
""")

st.markdown("""
### 
""")
