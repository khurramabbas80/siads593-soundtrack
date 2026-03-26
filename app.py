import streamlit as st

st.set_page_config(page_title="SIADS-593 Film Soundtrack Popularity", layout="wide")

st.title("SIADS-593: Anatomy of Film Soundtrack Popularity")
st.markdown("Use the sidebar to navigate between project notebooks.")

pages = {
    "1.1 TMDB API Spine Extraction": "1.1_TMDB_API_Spine_Extraction.py",
    "20260126-180228\\3.1 QA gate_ Validating the MusicBrainz exports": "20260126-180228\\3.1_QA_gate__Validating_the_MusicBrainz_exports.py",
    "20260126-180228-Jaime-Import\\3.1 Investigating Tables": "20260126-180228-Jaime-Import\\3.1_Investigating_Tables.py",
    "20260126-180228-Jaime-Import\\3.2 Remove unofficial albums": "20260126-180228-Jaime-Import\\3.2_Remove_unofficial_albums.py",
    "20260126-180228-Jaime-Import\\3.3 Cleanup redundant movie-album rows": "20260126-180228-Jaime-Import\\3.3_Cleanup_redundant_movie-album_rows.py",
    "20260126-180228-Jaime-Import\\3.4 Assign canonical soundtrack and songtrack": "20260126-180228-Jaime-Import\\3.4_Assign_canonical_soundtrack_and_songtrack.py",
    "20260126-180228-Jaime-Import\\3.5 Derive album genre and age columns": "20260126-180228-Jaime-Import\\3.5_Derive_album_genre_and_age_columns.py",
    "20260126-180228-Jaime-Import\\3.5 Derive Album Genre Columns": "20260126-180228-Jaime-Import\\3.5_Derive_Album_Genre_Columns.py",
    "20260126-180228-Jaime-Import\\3.5 Genre as a derived columns": "20260126-180228-Jaime-Import\\3.5_Genre_as_a_derived_columns.py",
    "20260126-180228-Jaime-Import\\5.1 Stat Viz": "20260126-180228-Jaime-Import\\5.1_Stat_Viz.py",
    "20260126-180228-Jaime-Import\\Init": "20260126-180228-Jaime-Import\\Init.py",
    "20260126-180228-Jaime-Import\\Last.fm Exploration": "20260126-180228-Jaime-Import\\Last.fm_Exploration.py",
    "20260126-180228-Jaime-Import\\Musicbrainz spine and last.fm join test": "20260126-180228-Jaime-Import\\Musicbrainz_spine_and_last.fm_join_test.py",
    "20260126-180228-Jaime-Import\\Notebook 1": "20260126-180228-Jaime-Import\\Notebook_1.py",
    "20260126-180228-Jaime-Import\\PyTrends Exploration": "20260126-180228-Jaime-Import\\PyTrends_Exploration.py",
    "3.6 Vote Count Analysis": "3.6_Vote_Count_Analysis.py",
    "4.3 Spotify API Pull": "4.3_Spotify_API_Pull.py",
    "Init": "Init.py",
    "Soundtrack\\2.1 Build_Album_MV_based_on_films.sql": "Soundtrack\\2.1_Build_Album_MV_based_on_films.sql.py",
    "Soundtrack\\2.2.Build_artist_track_wide_MVs.sql": "Soundtrack\\2.2.Build_artist_track_wide_MVs.sql.py",
    "Soundtrack\\3.5.1 Derive Film Genre Columns": "Soundtrack\\3.5.1_Derive_Film_Genre_Columns.py",
    "Soundtrack\\3.5.2 Genre Analysis between Film and Album": "Soundtrack\\3.5.2_Genre_Analysis_between_Film_and_Album.py",
    "Soundtrack\\3.7 Composer Analysis": "Soundtrack\\3.7_Composer_Analysis.py",
    "Soundtrack\\3.8 QA gate_ Cleansed spine sanity check": "Soundtrack\\3.8_QA_gate__Cleansed_spine_sanity_check.py",
    "Soundtrack\\4.1 Awards Data Scraper": "Soundtrack\\4.1_Awards_Data_Scraper.py",
    "Soundtrack\\4.1.1 Awards Analysis": "Soundtrack\\4.1.1_Awards_Analysis.py",
    "Soundtrack\\4.2 last.fm API pull": "Soundtrack\\4.2_last.fm_API_pull.py",
    "Soundtrack\\4.4 Rapid API (Soundnet) Pull": "Soundtrack\\4.4_Rapid_API_Soundnet_Pull.py",
    "Soundtrack\\4.5 Join everything": "Soundtrack\\4.5_Join_everything.py",
    "Soundtrack\\4.6 QA gate_ Raw metrics inspection and sanity check": "Soundtrack\\4.6_QA_gate__Raw_metrics_inspection_and_sanity_check.py",
    "Soundtrack\\4.7 Transform and reduce to analytics set": "Soundtrack\\4.7_Transform_and_reduce_to_analytics_set.py",
    "Soundtrack\\5.1 Film and Awards Analysis": "Soundtrack\\5.1_Film_and_Awards_Analysis.py",
    "Soundtrack\\5.3 Album-track popularity analysis": "Soundtrack\\5.3_Album-track_popularity_analysis.py",
    "Soundtrack\\5.4a Audio Features Cleaning": "Soundtrack\\5.4a_Audio_Features_Cleaning.py",
    "Soundtrack\\5.4b Audio Features Visual Exploration": "Soundtrack\\5.4b_Audio_Features_Visual_Exploration.py",
    "Soundtrack\\5.4c Statistical analysis of track listener data": "Soundtrack\\5.4c_Statistical_analysis_of_track_listener_data.py",
    "Soundtrack\\5.5 Statistical analysis of album listener data": "Soundtrack\\5.5_Statistical_analysis_of_album_listener_data.py",
    "Soundtrack\\6.1 Final Visualizations": "Soundtrack\\6.1_Final_Visualizations.py",
    "Soundtrack\\Test_elegant_wedding_theme": "Soundtrack\\Test_elegant_wedding_theme.py",
}

selected = st.sidebar.radio('Notebooks', list(pages.keys()))
st.sidebar.markdown('---')
st.code(f"streamlit run {pages[selected]}")
st.info(f'Run the command above to launch: **{selected}**')