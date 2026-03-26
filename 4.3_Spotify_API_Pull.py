import streamlit as st
import os, sys

st.set_page_config(page_title="4.3 Spotify API Pull", layout="wide")

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
import os
import random
import time
from collections import deque

# Third-party imports
import pandas as pd
import requests
import spotipy
from ftfy import fix_text
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials

# Import the CSV from the last stage of the pipeline

filepath = './pipeline/3.7.Wide_composer_analysis.csv'
df = pd.read_csv(filepath)
df.head()

df.info()

# Create a variable to store a subset of the columns for convenience

columns = [
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
    'vote_count_above_500'
    ]

# Clean text for instances where apostrophe displays as extraneous symbols

df["track_title_cleaned"] = (
    df["track_title"]
    .astype(str)
    .map(fix_text)
    .str.replace("’", "'", regex=False)
)

st.markdown("""
# Spotify API Code
""")

st.markdown("""
The Spotify API was queried in a local environment \\(PyCharm\\) in two separate runs\\. The code below is for reference only and should not be re\\-run\\. The results are saved in the api\\_dumps folder on Deepnote and will be merged into the wider dataframe\\.
""")

# # Load credentials
# SPOTIPY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
# SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# #------------------------
# # Spotipy client (one-time)
# #------------------------

# def make_spotify_client(client_id=None, client_secret=None):
#     """
#     If client_id/client_secret are None, Spotipy reads from:
#       SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET
#     """
#     auth_manager = SpotifyClientCredentials(
#         client_id=(client_id.strip() if isinstance(client_id, str) else None),
#         client_secret=(client_secret.strip() if isinstance(client_secret, str) else None),
#     )
#     return spotipy.Spotify(auth_manager=auth_manager, requests_timeout=30, retries=0)

# #------------------------
# # Throttling helpers
# #------------------------

# def throttle(base_delay=0.12, jitter=0.10):
#     """
#     Sleep a little between requests to reduce rate-limit hits.
#     base_delay: fixed seconds
#     jitter: random seconds added [0, jitter]
#     """
#     time.sleep(base_delay + random.random() * jitter)


# def backoff_from_retry_after(exc: SpotifyException, default_wait=2.0, extra_jitter=0.25):
#     retry_after = None

#     try:
#         hdrs = getattr(exc, "headers", None) or {}
#         retry_after = hdrs.get("Retry-After") or hdrs.get("retry-after")
#     except Exception:
#         retry_after = None

#     try:
#         wait_s = float(retry_after) if retry_after is not None else float(default_wait)
#     except Exception:
#         wait_s = float(default_wait)

#     # sleep happens outside now
#     return wait_s + random.random() * extra_jitter


# #------------------------
# # Helper function to keep queries safely under Spotify's ~250 character limit
# #------------------------

# def safe_text(s, max_len=200):
#     return (s or "").strip()[:max_len]

# #------------------------
# # simple in-memory cache (define this once, above the function)
# #------------------------

# spotify_cache = {}

# #------------------------
# # Track lookup helper function
# # -------------------------

# def lookup_track_id(sp, track_title: str, artist_name: str, market=None, limit=3):
#     track_title = safe_text(track_title, 200)
#     artist_name = safe_text(artist_name, 200)

#     if not track_title or not artist_name:
#         return None

#     # Cache key (case-insensitive)
#     cache_key = (track_title.lower(), artist_name.lower())

#     # Return cached result if we’ve seen this before
#     if cache_key in spotify_cache:
#         return spotify_cache[cache_key]

#     q = f'track:"{track_title}" artist:"{artist_name}"'

#     results = sp.search(q=q, type="track", limit=limit, market=market)
#     items = results.get("tracks", {}).get("items", [])

#     if not items:
#         spotify_cache[cache_key] = None   # cache the miss
#         return None

#     t = items[0]
#     result = {
#         "spotify_track_id": t.get("id"),
#         "spotify_url": (t.get("external_urls") or {}).get("spotify"),
#         "matched_track_name": t.get("name"),
#         "matched_artists": ", ".join([a.get("name", "") for a in t.get("artists", [])]),
#         "matched_album": (t.get("album") or {}).get("name"),
#         "matched_release_date": (t.get("album") or {}).get("release_date"),
#         "matched_popularity": t.get("popularity"),
#         "query_used": q,
#     }

#     # Store successful result in cache
#     spotify_cache[cache_key] = result
#     return result

# #------------------------
# # Bulk lookup function
# #------------------------

# def bulk_lookup_spotify_ids(
#     df_in: pd.DataFrame,
#     client_id=None,
#     client_secret=None,
#     base_delay=0.12,
#     jitter=0.10,
#     max_attempts=3,
#     market=None,
#     batch_size=5000,          # size of each chunk
#     start_row=0,              # resume from this row index (0-based)
#     checkpoint_path=None,     # write progress as you go
#     checkpoint_every=500,     # how often to flush progress
# ):
#     sp = make_spotify_client(client_id=client_id, client_secret=client_secret)

#     out_rows = []
#     n_total = len(df_in)

#     # Process only one batch
#     end_row = min(start_row + batch_size, n_total)
#     df_batch = df_in.iloc[start_row:end_row].copy()

#     # Use enumerate so progress is clean even if df index isn't 0..n-1
#     for k, (_, row) in enumerate(df_batch.iterrows(), start=1):
#         artist = row.get("recording_artist_credit", "")
#         title = row.get("track_title_cleaned", "")

#         result = None
#         error = None

#         for attempt in range(1, max_attempts + 1):
#             try:
#                 throttle(base_delay=base_delay, jitter=jitter)
#                 result = lookup_track_id(sp, track_title=title, artist_name=artist, market=market)
#                 error = None
#                 break

#             except SpotifyException as exc:
#                 if exc.http_status == 429:
#                     wait_s = backoff_from_retry_after(exc, default_wait=2.0)

#                     # Hard-stop if Spotify tells us to wait "too long"
#                     if wait_s > 300:   # 5 minutes (tune if you want)
#                         print(
#                             f"\n🚨 Spotify rate limit cooldown too long ({int(wait_s)}s). "
#                             f"Checkpointing and exiting safely."
#                         )

#                         # flush anything we have
#                         if checkpoint_path and out_rows:
#                             tmp = pd.DataFrame(out_rows)
#                             header = not os.path.exists(checkpoint_path)
#                             tmp.to_csv(checkpoint_path, mode="a", header=header, index=False)
#                             out_rows.clear()

#                         raise SystemExit(
#                             f"Spotify requested long cooldown ({int(wait_s)}s). "
#                             f"Resume later."
#                         )

#                     # normal short backoff
#                     time.sleep(wait_s)
#                     error = "rate_limited_429"
#                     continue

#             except Exception as exc:
#                 time.sleep(0.75 + random.random() * 0.25)
#                 error = f"other_error_{type(exc).__name__}"
#                 continue

#         out_rows.append({
#             "sp_tracking_id": row.get("sp_tracking_id", None),
#             "recording_artist_credit": artist,
#             "track_title_cleaned": title,  # NOTE: changed key name to match your merge below
#             "spotify_track_id": (result or {}).get("spotify_track_id") if result else None,
#             "spotify_url": (result or {}).get("spotify_url") if result else None,
#             "matched_track_name": (result or {}).get("matched_track_name") if result else None,
#             "matched_artists": (result or {}).get("matched_artists") if result else None,
#             "matched_album": (result or {}).get("matched_album") if result else None,
#             "matched_release_date": (result or {}).get("matched_release_date") if result else None,
#             "matched_popularity": (result or {}).get("matched_popularity") if result else None,
#             "query_used": (result or {}).get("query_used") if result else None,
#             "lookup_error": error,
#             "attempts_used": attempt,
#         })

#         # progress within the batch + absolute progress
#         if k % 100 == 0:
#             print(f"Processed {start_row + k}/{n_total} rows...")

#         # Periodic checkpoint append
#         if checkpoint_path and (k % checkpoint_every == 0):
#             tmp = pd.DataFrame(out_rows)
#             header = not os.path.exists(checkpoint_path)
#             tmp.to_csv(checkpoint_path, mode="a", header=header, index=False)
#             out_rows.clear()  # avoid holding lots of rows in memory

#     # flush remaining rows for this batch
#     batch_df = pd.DataFrame(out_rows)
#     if checkpoint_path and len(batch_df):
#         header = not os.path.exists(checkpoint_path)
#         batch_df.to_csv(checkpoint_path, mode="a", header=header, index=False)
#         out_rows.clear()
#         return pd.DataFrame()  # results are in the checkpoint file

#     return batch_df

# # Loop to run in batches

# checkpoint = "scoped_spotify_id_lookup_results.csv"

# # If rerunning from scratch, delete the checkpoint first:
# # import os
# # if os.path.exists(checkpoint): os.remove(checkpoint)

# batch_size = 5000

# for start in range(0, len(df), batch_size):
#     print(f"\n=== Batch starting at row {start} ===")
#     bulk_lookup_spotify_ids(
#         df_in=df,
#         client_id=SPOTIPY_CLIENT_ID,
#         client_secret=SPOTIPY_CLIENT_SECRET,
#         base_delay=0.25, # Increased from 0.12 due to repeated 429 errors
#         jitter=0.10,
#         max_attempts=3,
#         market=None,
#         batch_size=batch_size,
#         start_row=start,
#         checkpoint_path=checkpoint,   # writes progress continuously
#         checkpoint_every=500,
#     )

# print("Checkpoint saved at:", checkpoint)

st.markdown("""
In the first Spotify run, we tried to query the full population of movies without applying the \\>= 500 vote count filter\\. We collected as many results as we could before hitting the rate limit several times and shifting our approach\\.
""")

# Read CSV from the first Spotify API run

filepath_first_run = '/work/api_dumps/pycharm_spotify_id_lookup_results.csv'
df_first_run = pd.read_csv(filepath_first_run)
df_first_run.head()

len(df_first_run)

# Rename popularity column for convenience

df_first_run.rename(columns={"matched_popularity" : "spotify_popularity"}, inplace=True)
df_first_run.head()

num_rows_df = len(df_first_run)
num_rows_spotify_ids = df_first_run["spotify_track_id"].count()
print(f"In the 1st run file, there are {num_rows_spotify_ids} out of {num_rows_df} total rows with spotify ID values")

# De-duplicate on artist-track before merging (if duplicates found, keep row with lowest Spotify ID)

df_spotify_ids_deduped = (
    df_first_run
    .sort_values("spotify_track_id")
    .drop_duplicates(
        subset=["recording_artist_credit", "track_title_cleaned"],
        keep="first"
    )
)

print(f"Number of records after de-dupe: {len(df_spotify_ids_deduped)}")
df_spotify_ids_deduped.head()

# Merging the wide dataframe with the 1st run dataframe - Left Join

first_run_merged_df = df.merge(
    df_spotify_ids_deduped[
        ["recording_artist_credit", "track_title_cleaned", "spotify_track_id", "spotify_url", "spotify_popularity"]
    ],
    on=["recording_artist_credit", "track_title_cleaned"],
    how="left"
)

print(f"Number of records: {len(first_run_merged_df)}")
first_run_merged_df.head()

st.markdown("""
In the second Spotify run, we limited the population of movies to those with \\>= 500 vote count for which we did not obtain the ID's in the first run\\.
""")

# For the second run, create a dataframe that only has films with vote count >= 500

df_above_500 = df[df['vote_count_above_500'] == True]

print(f"Number of records: {len(df_above_500)}")
df_above_500.head()

# Determine the number of unique track-artist pairs in the wide dataframe where vote count >= 500

grouped_df = df_above_500.groupby([
    'track_title',
    'recording_artist_credit'
    ]).count()

grouped_df.shape[0]

# Sort by vote_count

df_above_500 = df_above_500.sort_values(by=['film_vote_count'], ascending=False)

# Create a column that has a unique identifier that can be used for de-bugging calls to the Spotify API

df_above_500 = df_above_500.reset_index(drop=True)
df_above_500["sp_tracking_id"] = df_above_500.index
df_above_500.head()

# Read CSV from the second Spotify API run

filepath_second_run = '/work/api_dumps/scoped_spotify_id_lookup_results.csv'
df_spotify_second_run = pd.read_csv(filepath_second_run)

print(f"Number of records: {len(df_spotify_second_run)}")
df_spotify_second_run.head()

# Rename popularity column for convenience

df_spotify_second_run.rename(columns={"matched_popularity" : "spotify_popularity"}, inplace=True)
df_spotify_second_run.head()

# De-duplicate on artist-track before merging (if duplicates found, keep row with lowest Spotify ID)

df_spotify_ids_deduped = (
    df_spotify_second_run
    .sort_values("spotify_track_id")
    .drop_duplicates(
        subset=["recording_artist_credit", "track_title_cleaned"],
        keep="first"
    )
)

# Merge the wide file (with first run IDs) with the second run (left join)

second_run_merged_df = first_run_merged_df.merge(
    df_spotify_ids_deduped[
        ["recording_artist_credit", "track_title_cleaned", "spotify_track_id", "spotify_url", "spotify_popularity"]
    ],
    on=["recording_artist_credit", "track_title_cleaned"],
    how="left"
)

print(f"Number of records after merge: {len(first_run_merged_df)}")
second_run_merged_df.head()

# Combine the _x and _y columns after merge

second_run_merged_df["spotify_track_id"] = second_run_merged_df["spotify_track_id_y"].combine_first(
    second_run_merged_df["spotify_track_id_x"]
)

second_run_merged_df["spotify_url"] = (
    second_run_merged_df["spotify_url_y"]
    .combine_first(second_run_merged_df["spotify_url_x"])
)

second_run_merged_df["spotify_popularity"] = (
    second_run_merged_df["spotify_popularity_y"]
    .combine_first(second_run_merged_df["spotify_popularity_x"])
)

# Drop the old columns

second_run_merged_df = second_run_merged_df.drop(
    columns=[
        "spotify_track_id_x",
        "spotify_track_id_y",
        "spotify_url_x",
        "spotify_url_y",
        "spotify_popularity_x",
        "spotify_popularity_y",
    ],
    errors="ignore"
)

second_run_merged_df.head()

num_rows_df = len(second_run_merged_df)
num_rows_spotify_ids = second_run_merged_df["spotify_track_id"].count()
print(f"After merging both runs into the wide dataframe, there are {num_rows_spotify_ids} out of {num_rows_df} total rows with spotify ID values")

# Save wide file to CSV

out_path = "./pipeline/4.3.Wide_spotify_ids.csv"

second_run_merged_df.to_csv(out_path, index=False)

# Check track file

filepath = './pipeline/3.7.Tracks_composer_analysis.csv'
df_track = pd.read_csv(filepath)

print(f"Number of records in track file before merge: {len(df_track)}")
df_track.head()

# De-duplicate on artist-track before merging (if duplicates found, keep row with lowest Spotify ID)

df_spotify_ids_deduped = (
    second_run_merged_df
    .sort_values("spotify_track_id")
    .drop_duplicates(
        subset=["recording_artist_credit", "track_title_cleaned"],
        keep="first"
    )
)

print(f"Number of records after de-dupe: {len(df_spotify_ids_deduped)}")

# Merge track file with the Spotify ID's from wide file (left join)

df_track_with_ids = df_track.merge(
    df_spotify_ids_deduped[[
        "recording_artist_credit",
        "track_title",
        "track_title_cleaned",
        "spotify_track_id",
        "spotify_url",
        "spotify_popularity"
        ]],
    on=["recording_artist_credit", "track_title"],
    how="left"
)

print(f"Number of records after merge: {len(df_track_with_ids)}")
df_track_with_ids.head()

# Save track file to CSV

out_path = "./pipeline/4.3.Tracks_spotify_ids.csv"

df_track_with_ids.to_csv(out_path, index=False)

# Albums and Artists carry over

import shutil

shutil.copy(
    "./pipeline/3.7.Albums_composer_analysis.csv",
    "./pipeline/4.3.Albums_spotify_ids.csv"
)

shutil.copy(
    "./pipeline/3.7.Artists_composer_analysis.csv",
    "./pipeline/4.3.Artists_spotify_ids.csv"
)
