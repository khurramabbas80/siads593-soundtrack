import streamlit as st
import os, sys

st.set_page_config(page_title="4.4 Rapid API (Soundnet) Pull", layout="wide")

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
import json
import os
import random
import time

# Third-party imports
import pandas as pd
import requests
from tqdm.auto import tqdm

filepath = './pipeline/4.3.Wide_spotify_ids.csv'
df_wide = pd.read_csv(filepath)
df_wide.head()

df_wide.info()

df_above_500 = df_wide[df_wide['vote_count_above_500'] == True]
df_above_500 = df_above_500.sort_values(by=['film_vote_count'], ascending=False)
# auto-detected possible Altair chart: df_above_500
try:
    st.altair_chart(df_above_500, use_container_width=True)
except Exception:
    st.write(df_above_500)

len(df_above_500)

# Create a column that has a unique identifier that can be used for de-bugging calls to the Soundnet API

df_above_500 = df_above_500.reset_index(drop=True)
df_above_500["soundnet_tracking_id"] = df_above_500.index
df_above_500.head()

st.markdown("""
We ran the following code in a local environment to pull from Rapid API \\(Soundnet\\)\\. Due to extremely slow API speeds and consistent errors, it was time\\-prohibitive to recover 100% of the audio features we requested\\. We called the API in several phases, attempting to improve our code in multiple iterations\\. We will merge the CSVs from the various API runs here in the notebook\\.
""")

# CLIENT_SECRET = os.environ["RAPID_API_KEY"]

# # Test the API for single request

# test_id = '6EVxyHPACBrQjjql3g90gC'

# base_url = 'https://track-analysis.p.rapidapi.com/pktx/spotify/'

# url = base_url + str(test_id)

# headers = {
# 	"x-rapidapi-key": CLIENT_SECRET,
# 	"x-rapidapi-host": "track-analysis.p.rapidapi.com"
# }

# response = requests.get(url, headers=headers)

# print(response.json())

# """
# Soundnet Track Analysis API runner

# DROP-IN FRIENDLY VERSION (with accurate tqdm on resume)

# Key updates in this version
# ---------------------------
# ✅ Resume-aware tqdm:
#    - Computes how many rows in *this df* are already done (based on processed_ids)
#    - Starts tqdm with initial=already_done_in_df
#    - Does NOT pbar.update(1) for rows skipped due to resume

# ✅ Resume counters:
#    - already_in_outfile is now the number of UNIQUE spotify_id_used values in OUTFILE

# ✅ Optional clarity in tqdm postfix:
#    - Shows saved_run and saved_total

# Behavior summary
# ----------------
# 1) Iterates df (expects spotify_track_id per row)
# 2) Calls Soundnet Track Analysis API per track id
# 3) Writes append-only CSV with fixed schema
# 4) Supports resume by skipping spotify IDs already written
# 5) Handles rate limits / transient failures via retry/backoff/cooldown
# 6) Flushes partial batch on Ctrl+C or unexpected exceptions
# """

# # ============================================================
# # 0) INPUT: set the DataFrame you want to process
# # ============================================================

# df = df_above_500  # <- assumes this exists


# # ============================================================
# # 1) API configuration
# # ============================================================

# headers = {
#     "x-rapidapi-key": CLIENT_SECRET,  # <- assumes this exists
#     "x-rapidapi-host": "track-analysis.p.rapidapi.com",
# }
# base_url = "https://track-analysis.p.rapidapi.com/pktx/spotify/"


# # ============================================================
# # 2) Runtime parameters (tune these first)
# # ============================================================

# SLEEP_SECONDS = 0.15     # per-row sleep when not in cooldown
# TIMEOUT_SECONDS = 20     # request timeout
# BATCH_SIZE = 1000        # flush frequency
# OUTFILE = "Soundnet_run_0201_2.csv"  # append-only output file


# # ============================================================
# # 3) Retry / backoff / cooldown tuning
# # ============================================================

# MAX_RETRIES_429 = 4
# MAX_RETRIES_NET = 3
# BACKOFF_BASE = 1.0
# BACKOFF_CAP = 60.0
# JITTER = 0.25


# # ============================================================
# # 4) Session + in-run cache
# # ============================================================

# session = requests.Session()

# # Cache only successful results (so failures can recover if the ID repeats later)
# soundnet_cache = {}


# # ============================================================
# # 5) Output schema (fixed columns in fixed order)
# # ============================================================

# FIXED_COLS = [
#     "spotify_id_used",
#     "query_url",
#     "soundnet_tracking_id",
#     "film_title",
#     "recording_artist_credit",
#     "track_title_cleaned",
#     "cache_hit",
#     "error",
#     "error_type",
#     "status_code",
#     "retry_after",
#     "error_message",
#     "raw_json",
# ]


# def row_to_fixed_schema(
#     *,
#     spotify_id_used,
#     query_url,
#     soundnet_tracking_id,
#     film_title,
#     recording_artist_credit,
#     track_title_cleaned,
#     cache_hit,
#     payload,
#     status_code=None,
#     error=False,
#     error_type=None,
#     retry_after=None,
#     error_message=None,
# ):
#     """
#     Convert one API result (or error) into a dict matching FIXED_COLS.

#     - payload stored as JSON string in raw_json (safe for CSV).
#     - error fields provide consistent auditing.
#     """
#     try:
#         raw_json = json.dumps(payload, ensure_ascii=False)
#     except TypeError:
#         raw_json = json.dumps(str(payload), ensure_ascii=False)

#     return {
#         "spotify_id_used": spotify_id_used,
#         "query_url": query_url,
#         "soundnet_tracking_id": soundnet_tracking_id,
#         "film_title": film_title,
#         "recording_artist_credit": recording_artist_credit,
#         "track_title_cleaned": track_title_cleaned,
#         "cache_hit": bool(cache_hit),
#         "error": bool(error),
#         "error_type": error_type,
#         "status_code": status_code,
#         "retry_after": retry_after,
#         "error_message": error_message,
#         "raw_json": raw_json,
#     }


# def append_checkpoint(outpath, new_rows):
#     """
#     Append rows to OUTFILE using a fixed schema.

#     Safe to call repeatedly and safe to call on shutdown.
#     """
#     if not new_rows:
#         return

#     df_new = pd.DataFrame(new_rows)

#     # Ensure fixed columns exist + enforce order
#     for c in FIXED_COLS:
#         if c not in df_new.columns:
#             df_new[c] = pd.NA
#     df_new = df_new[FIXED_COLS]

#     write_header = not os.path.exists(outpath)
#     df_new.to_csv(outpath, index=False, mode="a", header=write_header)


# # ============================================================
# # 6) Resume support: skip IDs already written to OUTFILE
# # ============================================================

# processed_ids = set()
# already_in_outfile = 0  # unique ids already written

# if os.path.exists(OUTFILE):
#     try:
#         prev_ids = pd.read_csv(OUTFILE, usecols=["spotify_id_used"])
#         processed_ids = set(
#             str(x).strip()
#             for x in prev_ids["spotify_id_used"].dropna().tolist()
#             if str(x).strip() != ""
#         )
#         already_in_outfile = len(processed_ids)
#     except Exception:
#         processed_ids = set()
#         already_in_outfile = 0


# # ============================================================
# # 7) Metrics for tqdm (live health + progress)
# # ============================================================

# metrics = {
#     "saved_this_run": 0,
#     "ok": 0,
#     "missing_id": 0,
#     "cache_hit": 0,
#     "err_429": 0,
#     "err_http": 0,
#     "err_net": 0,
#     "err_unexpected": 0,
#     "retries_429": 0,
#     "retries_net": 0,
#     "sleep_seconds": 0.0,
#     "skipped_already_done": 0,
# }


# # ============================================================
# # 8) Sleep helpers (with accounting)
# # ============================================================

# cooldown_until = 0.0  # global cooldown timestamp (seconds since epoch)


# def _parse_retry_after(v):
#     """Parse Retry-After header if it's numeric seconds; otherwise return None."""
#     if v is None:
#         return None
#     try:
#         return float(v)
#     except Exception:
#         return None


# def _sleep_with_jitter(seconds):
#     """
#     Sleep with jitter and record time slept.

#     Jitter reduces synchronized bursts that can worsen rate limiting.
#     """
#     if seconds <= 0:
#         return
#     factor = 1.0 + random.uniform(-JITTER, JITTER)  # in [1-JITTER, 1+JITTER]
#     s = max(0.0, seconds * factor)
#     metrics["sleep_seconds"] += s
#     time.sleep(s)


# # ============================================================
# # 9) Progress bar + loop state (resume-aware)
# # ============================================================

# seen = 0
# batch_rows = []

# # Count how many rows in *this df* are already done, so tqdm starts correctly.
# if "spotify_track_id" in df.columns and processed_ids:
#     df_ids = df["spotify_track_id"].dropna().astype(str).str.strip()
#     already_done_in_df = int(df_ids.isin(processed_ids).sum())
# else:
#     already_done_in_df = 0

# pbar = tqdm(
#     total=len(df),
#     initial=already_done_in_df,
#     desc="Soundnet (rows processed)",
#     unit="row",
#     dynamic_ncols=True,
# )

# if already_in_outfile > 0:
#     pbar.write(
#         f"Resuming from {OUTFILE}: unique ids in outfile={already_in_outfile} | "
#         f"already_done_in_this_df={already_done_in_df}"
#     )


# # ============================================================
# # 10) Main loop
# # ============================================================

# stop_reason = None  # for final summary (e.g., "keyboard_interrupt", "exception", None)

# try:
#     for _, row in df.iterrows():
#         seen += 1

#         # ----------------------------
#         # 10.1) Extract identifiers + base fields
#         # ----------------------------
#         track_id = row.get("spotify_track_id")
#         track_id_str = None if pd.isna(track_id) else str(track_id).strip()
#         url = None if not track_id_str else (base_url + track_id_str)

#         # IMPORTANT: compute this BEFORE modifying processed_ids in this iteration
#         was_already_done = bool(track_id_str) and (track_id_str in processed_ids)

#         base_kwargs = dict(
#             spotify_id_used=track_id,
#             query_url=url,
#             soundnet_tracking_id=row.get("soundnet_tracking_id"),
#             film_title=row.get("film_title"),
#             recording_artist_credit=row.get("recording_artist_credit"),
#             track_title_cleaned=row.get("track_title_cleaned"),
#         )

#         # ----------------------------
#         # 10.2) Missing ID => write an error row (no API call)
#         # ----------------------------
#         if not track_id_str:
#             out_row = row_to_fixed_schema(
#                 **base_kwargs,
#                 cache_hit=False,
#                 payload=None,
#                 status_code=None,
#                 error=True,
#                 error_type="missing_spotify_track_id",
#                 retry_after=None,
#                 error_message="spotify_track_id was blank/NA",
#             )
#             batch_rows.append(out_row)
#             metrics["missing_id"] += 1
#             metrics["saved_this_run"] += 1

#         # ----------------------------
#         # 10.3) Already processed in a prior run => skip
#         # ----------------------------
#         elif was_already_done:
#             metrics["skipped_already_done"] += 1

#         # ----------------------------
#         # 10.4) Need to fetch (or cache hit)
#         # ----------------------------
#         else:
#             cached = soundnet_cache.get(track_id_str)
#             if cached is not None:
#                 # Cache hit (in-run): reuse successful stored row
#                 out_row = dict(cached)
#                 out_row["cache_hit"] = True
#                 metrics["cache_hit"] += 1

#             else:
#                 payload = None
#                 status_code = None
#                 retry_after = None
#                 error = False
#                 error_type = None
#                 error_message = None

#                 # Obey global cooldown before attempting
#                 now = time.time()
#                 if now < cooldown_until:
#                     _sleep_with_jitter(cooldown_until - now)

#                 attempt_429 = 0
#                 attempt_net = 0

#                 while True:
#                     try:
#                         response = session.get(url, headers=headers, timeout=TIMEOUT_SECONDS)
#                         status_code = response.status_code
#                         retry_after = response.headers.get("Retry-After")

#                         try:
#                             payload = response.json()
#                         except ValueError:
#                             payload = response.text

#                         # Success
#                         if status_code < 400:
#                             break

#                         # 429 rate limit => retry a few times
#                         if status_code == 429 and attempt_429 < MAX_RETRIES_429:
#                             attempt_429 += 1
#                             metrics["retries_429"] += 1

#                             ra = _parse_retry_after(retry_after)
#                             wait = ra if (ra is not None and ra > 0) else min(
#                                 BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt_429 - 1))
#                             )

#                             cooldown_until = max(cooldown_until, time.time() + wait)
#                             _sleep_with_jitter(wait)
#                             continue

#                         # Retry some 5xx as transient
#                         if 500 <= status_code < 600 and attempt_net < MAX_RETRIES_NET:
#                             attempt_net += 1
#                             metrics["retries_net"] += 1
#                             wait = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt_net - 1)))
#                             _sleep_with_jitter(wait)
#                             continue

#                         # Terminal HTTP error
#                         error = True
#                         error_type = "rate_limited" if status_code == 429 else "http_error"
#                         break

#                     except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
#                         # Retry transient network errors
#                         attempt_net += 1
#                         if attempt_net <= MAX_RETRIES_NET:
#                             metrics["retries_net"] += 1
#                             wait = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt_net - 1)))
#                             _sleep_with_jitter(wait)
#                             continue

#                         # Terminal network error
#                         error = True
#                         error_type = "network_exception"
#                         error_message = str(e)
#                         payload = None
#                         break

#                     except Exception as e:
#                         # Terminal unexpected error
#                         error = True
#                         error_type = "unexpected_exception"
#                         error_message = repr(e)
#                         payload = None
#                         break

#                 out_row = row_to_fixed_schema(
#                     **base_kwargs,
#                     cache_hit=False,
#                     payload=payload,
#                     status_code=status_code,
#                     error=error,
#                     error_type=error_type,
#                     retry_after=retry_after,
#                     error_message=error_message,
#                 )

#                 if out_row["error"]:
#                     if out_row["error_type"] == "rate_limited":
#                         metrics["err_429"] += 1
#                     elif out_row["error_type"] == "http_error":
#                         metrics["err_http"] += 1
#                     elif out_row["error_type"] == "network_exception":
#                         metrics["err_net"] += 1
#                     else:
#                         metrics["err_unexpected"] += 1
#                 else:
#                     metrics["ok"] += 1

#                 # Cache successful results only
#                 if not out_row["error"]:
#                     soundnet_cache[track_id_str] = out_row

#             # Persist + mark processed for resume
#             batch_rows.append(out_row)
#             processed_ids.add(track_id_str)
#             metrics["saved_this_run"] += 1

#             # Per-row sleep (only when not already in cooldown)
#             if time.time() >= cooldown_until:
#                 _sleep_with_jitter(SLEEP_SECONDS)

#         # ----------------------------
#         # 10.5) Update progress bar
#         # ----------------------------
#         # Resume-aware: do NOT update the bar for rows that were already done,
#         # because pbar.initial already accounted for them.
#         if not was_already_done:
#             pbar.update(1)

#         if seen % 25 == 0:
#             cooldown_remaining = max(0.0, cooldown_until - time.time())
#             done_pct = (100.0 * pbar.n / pbar.total) if pbar.total else 0.0

#             pbar.set_postfix({
#                 "done_%": f"{done_pct:5.1f}%",
#                 "saved_run": metrics["saved_this_run"],
#                 "saved_total": already_in_outfile + metrics["saved_this_run"],
#                 "ok": metrics["ok"],
#                 "skip": metrics["skipped_already_done"],
#                 "429": metrics["err_429"],
#                 "http": metrics["err_http"],
#                 "net": metrics["err_net"],
#                 "r429": metrics["retries_429"],
#                 "rnet": metrics["retries_net"],
#                 "sleep_s": int(metrics["sleep_seconds"]),
#                 "cool_s": int(cooldown_remaining),
#                 "cache": metrics["cache_hit"],
#                 "miss": metrics["missing_id"],
#             })

#         # ----------------------------
#         # 10.6) Batch flush (regular checkpointing)
#         # ----------------------------
#         if len(batch_rows) >= BATCH_SIZE:
#             append_checkpoint(OUTFILE, batch_rows)
#             batch_rows.clear()
#             pbar.write(f"Flushed batch ({BATCH_SIZE}). saved_this_run={metrics['saved_this_run']}")

# except KeyboardInterrupt:
#     stop_reason = "keyboard_interrupt"
#     pbar.write("KeyboardInterrupt received — flushing partial batch to disk and stopping...")

# except Exception as e:
#     stop_reason = f"exception: {repr(e)}"
#     pbar.write(f"Unexpected exception — flushing partial batch to disk and stopping...\n{repr(e)}")

# finally:
#     # ALWAYS flush remaining rows and close progress bar cleanly
#     if batch_rows:
#         try:
#             append_checkpoint(OUTFILE, batch_rows)
#             pbar.write(f"Final flush complete: wrote {len(batch_rows)} rows from partial batch.")
#         except Exception as flush_err:
#             pbar.write(f"ERROR: Final flush failed: {repr(flush_err)}")
#         finally:
#             batch_rows.clear()

#     pbar.close()

#     if stop_reason is None:
#         stop_reason = "completed"

#     print(
#         f"Done ({stop_reason}). Input rows iterated={seen}/{len(df)} | "
#         f"saved_this_run={metrics['saved_this_run']} | ok={metrics['ok']} | "
#         f"skipped_already_done={metrics['skipped_already_done']} | "
#         f"429={metrics['err_429']} http={metrics['err_http']} net={metrics['err_net']} "
#         f"missing_id={metrics['missing_id']} cache_hit={metrics['cache_hit']} | "
#         f"retries_429={metrics['retries_429']} retries_net={metrics['retries_net']} | "
#         f"sleep_seconds≈{metrics['sleep_seconds']:.1f} | "
#         f"already_done_in_this_df={already_done_in_df} unique_ids_in_outfile={already_in_outfile}"
#     )

filepath = '/work/api_dumps/4.4.rapid_api_track_parts/07_Soundnet_run_0201_2.csv'
df_final_results = pd.read_csv(filepath)

print(f"Total records in final batch: {df_final_results.shape[0]}")
df_final_results.head()

# We changed our schema to record the API results mid-way through in order to improve de-bugging.
# Here, we are loading in the result files that contained the final schema.

files = [
    "/work/api_dumps/4.4.rapid_api_track_parts/02_Soundnet_Results.csv",
    "/work/api_dumps/4.4.rapid_api_track_parts/03_Soundnet_Results.csv",
    "/work/api_dumps/4.4.rapid_api_track_parts/04_Soundnet_minimal_sleep_batches_run_INCOMPLETE_0129.csv",
    "/work/api_dumps/4.4.rapid_api_track_parts/05_Soundnet_missing_audio_features_0131_2.csv",
    "/work/api_dumps/4.4.rapid_api_track_parts/06_Successful Results from Jaime and Khurram.csv",
    "/work/api_dumps/4.4.rapid_api_track_parts/07_Soundnet_run_0201_2.csv"
]

dfs = [pd.read_csv(f) for f in files]
combined_df = pd.concat(dfs, ignore_index=True)

print(f"Total records after concatenation: {combined_df.shape[0]}")
combined_df.head()

# -------------------------------------------------
# Drop rows with API errors
# -------------------------------------------------

combined_df = combined_df[combined_df["error"] == False].copy()
combined_df = combined_df[
    ~combined_df["raw_json"].astype("string").str.contains("error", case=False, na=False)
].copy()

print(f"Total records in these batches after dropping errors: {combined_df.shape[0]}")

# -------------------------------------------------
# Parse raw JSON with explicit parse-success flag
# -------------------------------------------------

def safe_json_load_with_ok(x):
    if pd.isna(x) or x == "":
        return {}, True
    if isinstance(x, dict):
        return x, True
    try:
        return json.loads(x), True
    except Exception:
        return {}, False

parsed = combined_df["raw_json"].apply(safe_json_load_with_ok)

combined_df["raw_json_parsed"] = parsed.apply(lambda t: t[0])
combined_df["json_parse_ok"] = parsed.apply(lambda t: t[1])

# -------------------------------------------------
# Explode JSON into columns
# -------------------------------------------------

# Explode JSON into columns (force same index as combined_df)
features_df = (
    pd.json_normalize(combined_df["raw_json_parsed"])
    .set_index(combined_df.index)   # <- force alignment
    .drop(columns=["error"], errors="ignore")
)

# Join back to original dataframe
df_exploded = (
    combined_df
    .drop(columns=["raw_json"])
    .join(features_df)
)

# Backfill key directly from raw_json_parsed where key is missing/blank
key_s = df_exploded["key"].astype("string")
mask_without_key = key_s.isna() | (key_s.str.strip() == "")

df_exploded.loc[mask_without_key, "key"] = df_exploded.loc[mask_without_key, "raw_json_parsed"].apply(
    lambda d: d.get("key") if isinstance(d, dict) else None
)

# Re-check
key_s2 = df_exploded["key"].astype("string")
mask_without_key2 = key_s2.isna() | (key_s2.str.strip() == "")

print(f"Count without key (after backfill): {mask_without_key2.sum()}")
rows_without_key = df_exploded.loc[mask_without_key2]
# auto-detected possible Altair chart: rows_without_key
try:
    st.altair_chart(rows_without_key, use_container_width=True)
except Exception:
    st.write(rows_without_key)


has_key_in_dict = df_exploded["raw_json_parsed"].apply(lambda d: isinstance(d, dict) and ("key" in d))
missing_key_col  = df_exploded["key"].astype("string").isna()

print("Dict has 'key' but column is NA:", (has_key_in_dict & missing_key_col).sum())

# Load the results gathered under the prior schema

filepath = '/work/api_dumps/4.4.rapid_api_track_parts/01_Soundnet_Results.csv'
df_1 = pd.read_csv(filepath)

print(f"Total records in df 1: {df_1.shape[0]}")
df_1.head()

# Drop empty results from this batch

df_1 = df_1[df_1["key"].notna() & (df_1["key"].str.strip() != "")]

print(f"Total records in df_1 after dropping empty results: {df_1.shape[0]}")
df_1.head()

# Re-order the columns to match the final schema

col_order = [
    'spotify_id_used',
    'query_url',
    'soundnet_tracking_id',
    'recording_artist_credit',
    'track_title_cleaned',
    'cache_hit',
    'id',
    'key',
    'mode',
    'camelot',
    'tempo',
    'duration',
    'popularity',
    'energy',
    'danceability',
    'happiness',
    'acousticness',
    'instrumentalness',
    'liveness',
    'speechiness',
    'loudness',
    'name',
    'album'
 ]

df_1 = df_1[col_order]
df_1.head()

# Concatenate the dataframes into one schema

final_schema_df = pd.concat([df_exploded, df_1], ignore_index=True)

print(f"Total records after concatenating all results, all schemas: {final_schema_df.shape[0]}")
final_schema_df.head()

# Rename spotify id column to make joining easier

final_schema_df = final_schema_df.rename(
    columns={"spotify_id_used": "spotify_track_id"}
)

# De-duplicate on artist-track before merging (if duplicates found, keep row with lowest Spotify ID)

final_schema_deduped_df = (
    final_schema_df
    .sort_values("spotify_track_id")
    .drop_duplicates(
        subset=["recording_artist_credit", "track_title_cleaned"],
        keep="first"
    )
)

print(f"Number of records after de-dupe: {len(final_schema_deduped_df)}")
final_schema_deduped_df.head()

# Checking for spotify_track_id duplicates after de-duping on artist-track

final_schema_deduped_df["spotify_track_id"].value_counts()[lambda s: s > 1]

# Examine rows with remaining duplicate spotify_track_id's

duplicate_rows = final_schema_deduped_df[
    final_schema_deduped_df["spotify_track_id"].duplicated(keep=False)
].sort_values("spotify_track_id")

print(f"Number of rows with duplicate Spotify ID's: {len(duplicate_rows)}")
# auto-detected possible Altair chart: duplicate_rows
try:
    st.altair_chart(duplicate_rows, use_container_width=True)
except Exception:
    st.write(duplicate_rows)

# Drop extraneous columns

final_schema_deduped_df = final_schema_deduped_df.drop(columns=["name", "album"])
final_schema_deduped_df.head()

list(final_schema_deduped_df.columns)

print(f"Number of records before merging into wide dataframe: {len(final_schema_deduped_df)}")
final_schema_deduped_df.head()

# Check for duplicate keys before merging

assert not final_schema_deduped_df.duplicated(
    subset=[
        "spotify_track_id",
        "recording_artist_credit",
        "track_title_cleaned",
    ]
).any(), "Duplicate composite keys in final_schema_deduped_df"

# Merge audio features into df_wide

cols_to_add = [
    "spotify_track_id",
    "recording_artist_credit",
    "track_title_cleaned",
    "key",
    "mode",
    "camelot",
    "tempo",
    "duration",
    "popularity",
    "energy",
    "danceability",
    "happiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "speechiness",
    "loudness",
]

df_wide = df_wide.merge(
    final_schema_deduped_df[cols_to_add],
    on=[
        "spotify_track_id",
        "recording_artist_credit",
        "track_title_cleaned",
    ],
    how="left"
)

print(f"Number of records after merging: {len(df_wide)}")
df_wide.head()

df_wide.info()

list(df_wide.columns)

df_wide.sample(10)

# Sanity check - check for number of rows with value for key

count_with_key = (
    df_wide["key"].notna()
    & (df_wide["key"].str.strip() != "")
).sum()

# auto-detected possible Altair chart: count_with_key
try:
    st.altair_chart(count_with_key, use_container_width=True)
except Exception:
    st.write(count_with_key)

# Sanity check - check for number of rows with value for duration

count_with_duration = (
    df_wide["duration"].notna()
    & (df_wide["duration"].str.strip() != "")
).sum()

# auto-detected possible Altair chart: count_with_duration
try:
    st.altair_chart(count_with_duration, use_container_width=True)
except Exception:
    st.write(count_with_duration)

# Sanity check - check for number of rows with value for danceability

count_with_danceability = df_wide["danceability"].notna().sum()
# auto-detected possible Altair chart: count_with_danceability
try:
    st.altair_chart(count_with_danceability, use_container_width=True)
except Exception:
    st.write(count_with_danceability)

# Sanity check - check for number of rows with value for tempo

count_with_tempo = df_wide["tempo"].notna().sum()
# auto-detected possible Altair chart: count_with_tempo
try:
    st.altair_chart(count_with_tempo, use_container_width=True)
except Exception:
    st.write(count_with_tempo)

# Save wide file to CSV

out_path = "./pipeline/4.4.Wide_rapid_api_pull.csv"

df_wide.to_csv(out_path, index=False)

# Check track file

filepath = './pipeline/4.3.Tracks_spotify_ids.csv'
df_track = pd.read_csv(filepath)

print(f"Number of records in track file before merge: {len(df_track)}")
df_track.head()

list(df_track.columns)

# Check for duplicate keys before merging

assert not final_schema_deduped_df.duplicated(
    subset=[
        "spotify_track_id",
        "recording_artist_credit",
        "track_title_cleaned",
    ]
).any(), "Duplicate composite keys in final_schema_deduped_df"

# Merge audio features into track file

cols_to_add = [
    "spotify_track_id",
    "recording_artist_credit",
    "track_title_cleaned",
    "key",
    "mode",
    "camelot",
    "tempo",
    "duration",
    "popularity",
    "energy",
    "danceability",
    "happiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "speechiness",
    "loudness",
]

df_track = df_track.merge(
    final_schema_deduped_df[cols_to_add],
    on=[
        "spotify_track_id",
        "recording_artist_credit",
        "track_title_cleaned",
    ],
    how="left"
)

print(f"Number of records after merging: {len(df_track)}")
df_track.head()

# Sanity check - check for number of rows with value for key

count_with_key = (
    df_track["key"].notna()
    & (df_track["key"].str.strip() != "")
).sum()

# auto-detected possible Altair chart: count_with_key
try:
    st.altair_chart(count_with_key, use_container_width=True)
except Exception:
    st.write(count_with_key)

# Sanity check - check for number of rows with value for danceability

count_with_danceability = df_track["danceability"].notna().sum()
# auto-detected possible Altair chart: count_with_danceability
try:
    st.altair_chart(count_with_danceability, use_container_width=True)
except Exception:
    st.write(count_with_danceability)

# Save track file to CSV

out_path = "./pipeline/4.4.Tracks_rapid_api_pull.csv"

df_track.to_csv(out_path, index=False)

# Albums and Artists carry over

import shutil

shutil.copy(
    "./pipeline/4.3.Albums_spotify_ids.csv",
    "./pipeline/4.4.Albums_rapid_api_pull.csv"
)

shutil.copy(
    "./pipeline/4.3.Artists_spotify_ids.csv",
    "./pipeline/4.4.Artists_rapid_api_pull.csv"
)
