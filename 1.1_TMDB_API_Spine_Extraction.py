import streamlit as st
import os, sys

st.set_page_config(page_title="1.1 TMDB API Spine Extraction", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
# 1\\.1 — TMDB API Spine Extraction \\(2015–2025\\)
""")

st.markdown("""
### What this notebook is for
""")

st.markdown("""
This notebook extracts a film\\-level “spine” from the TMDB API for releases from 2015–2025 and writes the results to a versioned CSV artifact: \\`/work/pipeline/1\\.1\\.TMDB 2015\\-2025\\.csv\\`
""")

st.markdown("""
This CSV is the handoff format used to move TMDB data into our local Postgres environment \\(MusicBrainz database\\) for staging \\+ downstream joins\\.
""")

st.markdown("""
### What this notebook produces
""")

st.markdown("""
Primary artifact
\\- \\`/work/pipeline/1\\.1\\.TMDB 2015\\-2025\\.csv\\`
""")

st.markdown("""
Grain
\\- One row per TMDB film ID \\(movie\\)
""")

st.markdown("""
Fields included \\(high level\\)
\\- Film identifiers \\+ titles \\(TMDB ID, title, original title, language\\)
\\- Release metadata \\(year, release date, MPAA rating when available\\)
\\- Popularity \\+ engagement proxies \\(popularity, vote average, vote count\\)
\\- Production metadata \\(countries, studios, budget, revenue, runtime, genres\\)
\\- Key people fields \\(director, “Original Music Composer” if present, top cast\\)
\\- Limited keywords \\+ external IDs \\(IMDb ID, Wikidata ID\\)
""")

st.markdown("""
### How it works \\(code behavior\\)
""")

st.markdown("""
Authentication
\\- Reads \\`TMDB\\_TOKEN\\` from your \\`\\.env\\` file and uses it as a Bearer token\\.
\\- If the token is missing, the notebook intentionally fails fast\\.
""")

st.markdown("""
Discovery \\(per year\\)
\\- Uses \\`/discover/movie\\` and paginates up to 500 pages, sorted by \\`popularity\\.desc\\`\\.
\\- Applies \\`vote\\_count\\.gte = 10\\` as a minimum activity threshold\\.
\\- \\`include\\_adult = true\\` \\(adult titles are included by design for completeness\\)\\.
""")

st.markdown("""
Details expansion \\(per movie ID\\)
\\- Fetches \\`/movie/\\{id\\}\\` with \\`append\\_to\\_response=credits,keywords,release\\_dates,external\\_ids\\`
\\- Parses:
  \\- Director\\(s\\) from crew where \\`job == "Director"\\`
  \\- Composer\\(s\\) from crew where \\`job == "Original Music Composer"\\`
  \\- Top 5 cast members
  \\- MPAA rating from US release dates when available; otherwise \\`NR\\`
""")

st.markdown("""
Rate limiting \\+ resilience
\\- On TMDB rate limit \\(HTTP 429\\), the notebook pauses and retries\\.
\\- Output is appended incrementally to CSV; the notebook is safe to stop and resume\\.
""")

st.markdown("""
Resume / re\\-run semantics \\(important\\)
""")

st.markdown("""
This notebook supports resume mode:
""")

st.markdown("""
\\- If the CSV already exists, it reads the file and collects existing TMDB IDs\\.
\\- It then skips IDs already written and only pulls “new” titles\\.
""")

st.markdown("""
This means re\\-running will typically append only missing rows, not overwrite the file\\. If you want a clean rebuild, delete the CSV first\\.
""")

st.markdown("""
### Downstream integration \\(where this CSV goes\\)
""")

st.markdown("""
This CSV is not queried directly during analysis\\.
""")

st.markdown("""
Instead, it is imported into Postgres as a staging table \\(layered on top of the local MusicBrainz database\\):
""")

st.markdown("""
1\\. Run this notebook to generate \\`/work/pipeline/1\\.1\\.TMDB 2015\\-2025\\.csv\\`
2\\. Upload/import the CSV via DBeaver into the TMDB staging table
3\\. Use SQL to join TMDB films to MusicBrainz release groups and build the film–album spine
""")

st.markdown("""
So: Notebook = extraction layer → DBeaver/Postgres = integration \\+ transformation layer\\.
""")

st.markdown("""
### Notes for reviewers / reproducibility
""")

st.markdown("""
\\- TMDB attributes like popularity and vote counts can drift over time\\.
\\- If this notebook is re\\-run at a later date, results may not match exactly unless the artifact CSV is treated as the frozen input\\.
\\- For reproducibility, treat the committed CSV as the authoritative snapshot used in downstream work\\.
""")

import csv
import os
import time
from datetime import datetime

import requests
from dotenv import load_dotenv

# --- 1. SETTINGS & AUTHENTICATION ---
load_dotenv()
ACCESS_TOKEN = os.getenv("TMDB_TOKEN")

if not ACCESS_TOKEN:
    raise ValueError("API Token missing! Ensure TMDB_TOKEN is in your .env file.")

BASE_URL = "https://api.themoviedb.org/3"
FILENAME = "/work/pipeline/1.1.TMDB 2015-2025.csv"
START_YEAR = 2015
END_YEAR = 2025

HEADERS_LIST = [
    "ID", "Title", "Adult", "Runtime (min)", "Genres", "Rating (0-10)",
    "Vote Count", "MPAA Rating", "Original Title", "Language Name",
    "IMDb ID", "Wikidata ID", "Countries", "Year", "Release Date",
    "Popularity", "Budget", "Revenue", "Studios", "Director",
    "Soundtrack/Composer", "Top Cast", "Keywords"
]

HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}


# --- 2. RESUME & STORAGE LOGIC ---
def get_existing_progress():
    """Reads the CSV file and returns a Set of IDs already saved."""
    existing_ids = set()
    if not os.path.exists(FILENAME):
        return existing_ids

    with open(FILENAME, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['ID']:
                existing_ids.add(int(row['ID']))
    return existing_ids


def save_row_to_csv(data_row):
    """Appends one movie row to the CSV with a retry mechanism."""
    for attempt in range(3):
        try:
            with open(FILENAME, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=HEADERS_LIST)
                writer.writerow(data_row)
            return
        except OSError:
            print(f"  [File Lock] Disk busy, retrying... ({attempt + 1}/3)")
            time.sleep(2)


# --- 3. API COMMUNICATION ---
def get_movies_by_year(year):
    """Fetches a list of movie IDs for a given year via TMDB Discovery."""
    movie_list = []
    for page in range(1, 501):
        params = {
            "primary_release_year": year,
            "sort_by": "popularity.desc",
            "vote_count.gte": 10,
            "page": page,
            "include_adult": "true"
        }

        response = requests.get(
            f"{BASE_URL}/discover/movie",
            headers=HEADERS,
            params=params
        )
        if response.status_code != 200:
            break

        results = response.json().get('results', [])
        if not results:
            break

        movie_list.extend(results) # takes 20 movies from current page and adds to one giant master list
        time.sleep(0.1)

    return movie_list


def extract_movie_details(m_id):
    """Fetches specific details like Budget and Credits for an ID."""
    url = f"{BASE_URL}/movie/{m_id}"
    params = {"append_to_response": "credits,keywords,release_dates,external_ids"}

    try:
        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            print("  Rate limit reached. Sleeping 5s...")
            time.sleep(5)
            return extract_movie_details(m_id)
    except Exception as e:
        print(f"  Connection error on ID {m_id}: {e}")
        return None


# --- 4. DATA TRANSFORMATION ---
def clean_data(d):
    """Converts nested API JSON into a flat dictionary for CSV."""
    credits = d.get('credits', {})
    crew = credits.get('crew', [])

    # Helper logic for specific roles
    directors = [c['name'] for c in crew if c['job'] == 'Director']
    composers = [c['name'] for c in crew if c['job'] == 'Original Music Composer']
    cast = [c['name'] for c in credits.get('cast', [])[:5]]

    mpaa = "NR"
    for country in d.get('release_dates', {}).get('results', []):
        if country['iso_3166_1'] == 'US':
            for rel in country['release_dates']:
                if rel['certification']:
                    mpaa = rel['certification']
                    break

    return {
        "ID": d['id'],
        "Title": d['title'],
        "Adult": d.get('adult'), # get used so if its blank the script doesnt crash
        "Runtime (min)": d.get('runtime'),
        "Genres": ", ".join([g['name'] for g in d.get('genres', [])]),
        "Rating (0-10)": d.get('vote_average'),
        "Vote Count": d.get('vote_count'),
        "MPAA Rating": mpaa,
        "Original Title": d.get('original_title'),
        "Language Name": d.get('original_language'),
        "IMDb ID": d.get('external_ids', {}).get('imdb_id'),
        "Wikidata ID": d.get('external_ids', {}).get('wikidata_id'),
        "Countries": ", ".join([c['name'] for c in d.get('production_countries', [])]),
        "Year": d['release_date'][:4] if d.get('release_date') else "N/A", # :4 as we only want year YYYY
        "Release Date": d.get('release_date'),
        "Popularity": d.get('popularity'),
        "Budget": d.get('budget'),
        "Revenue": d.get('revenue'),
        "Studios": ", ".join([s['name'] for s in d.get('production_companies', [])]),
        "Director": ", ".join(directors),
        "Soundtrack/Composer": ", ".join(composers),
        "Top Cast": ", ".join(cast),
        "Keywords": ", ".join([k['name'] for k in d.get('keywords', {}).get('keywords', [])[:5]]) # limit to five only
    }


# --- 5. ORCHESTRATION (THE MAIN LOOP) ---
def main():
    """Main execution loop for processing movie data by year."""
    if not os.path.exists(FILENAME):
        with open(FILENAME, mode='w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=HEADERS_LIST).writeheader()

    processed_ids = get_existing_progress()
    print(f"Resuming... {len(processed_ids)} movies already in database.")

    for year in range(START_YEAR, END_YEAR + 1):
        print(f"\n>>> PROCESSING {year} <<<")
        found_movies = get_movies_by_year(year)
        new_movies = [m for m in found_movies if m['id'] not in processed_ids]
        print(f"  Total found: {len(found_movies)} | New to pull: {len(new_movies)}")

        for i, movie in enumerate(new_movies, 1):
            raw_details = extract_movie_details(movie['id'])
            if raw_details:
                row = clean_data(raw_details)
                save_row_to_csv(row)
                processed_ids.add(row['ID'])

                if i % 10 == 0:
                    print(f"  Progress: {i}/{len(new_movies)} rows written.")

            time.sleep(0.05)

    print("\n--- ALL YEARS COMPLETE ---")


if __name__ == "__main__":
    main()
