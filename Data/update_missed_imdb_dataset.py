import asyncio
import os
import sys
import aiohttp
import pandas as pd
from dotenv import load_dotenv
import MongoDBContext as MongoDBC
from logger import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# ------------------ Optimized IMDb Data Loading ------------------

def load_filtered_imdb_data(file_path, title_list):
    """Lädt IMDb-Daten effizient und filtert nur relevante Filme."""
    filtered_movies = []

    # Nur benötigte Spalten laden
    usecols = ["tconst", "primaryTitle", "titleType"]
    chunksize = 100000  # Verarbeitung in Chunks, um Speicher zu sparen

    for chunk in pd.read_csv(file_path, sep="\t", dtype=str,encoding='utf-8', usecols=usecols, chunksize=chunksize, na_values="\\N"):
        # Filtere nur Filme (kein TV oder Kurzfilm)
        chunk = chunk[chunk["titleType"] == "movie"]

        # Filtere nach Titeln, die in title_list sind
        chunk_filtered = chunk[chunk["primaryTitle"].isin(title_list)]
        
        if not chunk_filtered.empty:
            filtered_movies.append(chunk_filtered)

    # Alle gefilterten Chunks zusammenfügen
    return pd.concat(filtered_movies, ignore_index=True) if filtered_movies else pd.DataFrame()

# ------------------ Async Functions for Parallel Processing ------------------

async def fetch_imdb_id(movie, df):
    """Sucht die IMDb-ID für einen Film anhand des Titels."""
    try:
        matching_movies = df[df["primaryTitle"].str.lower() == movie.get("title").lower()]
        if not matching_movies.empty:
            return movie["_id"], matching_movies.iloc[0]["tconst"]  # (MongoDB-ID, IMDb-ID)
        return movie["_id"], None
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der IMDb-ID für {movie['title']}: {e}")
        return movie["_id"], None

async def get_missed_imdb_ids(collection, file_path):
    """Lädt IMDb-Daten, sucht parallele IMDb-IDs und speichert sie."""
    try:
        # Lade alle Filme, die noch keine IMDb-ID haben
        movies_missed_imdb = await collection.find({"$or": [{"imdb_id": None}, {"imdb_id": ""}]}).to_list(length=None)
        if not movies_missed_imdb:
            logger.info("Keine Filme ohne IMDb-ID gefunden.")
            return

        logger.info(f"{len(movies_missed_imdb)} Filme ohne IMDb-ID gefunden. Starte parallele Suche...")

        # Liste aller zu suchenden Titel
        title_list = [movie["title"] for movie in movies_missed_imdb]

        # IMDb-Daten filtern (Speicher-Optimierung!)
        df = load_filtered_imdb_data(file_path, title_list)

        # Parallel IMDb-IDs suchen
        tasks = [fetch_imdb_id(movie, df) for movie in movies_missed_imdb]
        results = await asyncio.gather(*tasks)

        # Filtere nur erfolgreiche IMDb-IDs
        updates = [(movie_id, imdb_id) for movie_id, imdb_id in results if imdb_id]

        if not updates:
            logger.warning("Keine IMDb-IDs konnten aktualisiert werden.")
            return

        logger.info(f"{len(updates)} Filme werden in der Datenbank aktualisiert...")

        # MongoDB-Updates parallel durchführen
        update_tasks = [collection.update_one({"_id": movie_id}, {"$set": {"imdb_id": imdb_id}}) for movie_id, imdb_id in updates]
        await asyncio.gather(*update_tasks)

        logger.info("Alle IMDb-IDs wurden erfolgreich gespeichert.")

    except Exception as e:
        logger.error(f"Ein Fehler ist aufgetreten: {e}")

# ------------------ Main Async Function ------------------

async def main():
    load_dotenv()
    mongo_uri = os.getenv('mongo_uri')  # MongoDB-URI aus .env laden

    async with MongoDBC.MongoDBContext(mongo_uri) as (client, db):
        if client is None or db is None:
            logger.error("Failed to get a valid MongoDB client or database")
            return

        collection = db["children_movies"]
        file_path = "Data\\title.basics.tsv.gz"  # IMDb-Daten

        await get_missed_imdb_ids(collection, file_path)
        await client.close()

# ------------------ Script Execution ------------------

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Unexpected error in main: {str(e)}")
