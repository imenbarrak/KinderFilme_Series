import asyncio
import datetime
import sys
import os

import aiohttp


# Füge den Elternordner zum Python-Pfad hinzu
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logger import logger
import API_call.get_Data_API_movie as api_movies


def get_movie_by_title(titel_film,db):
    pass
    

async def get_info_by_id(collection, id):
    movie =await collection.find_one({"imdb_id": id}, {"title": 1, "wikidata_id": 1, "_id": 0})

    if movie:
        title = movie.get("title")
        wikidata_id = movie.get("wikidata_id")
        logger.info(f"Movie Title: {title}")
    else:
        logger.info("Movie not found.")
        
    return title, wikidata_id

def get_all_film_released_after(db, release_Date):
    """get all films released after the given Date
       (can add  parameter Before as a boolean to make it possible getting before 
       or after)
    Args:
        db : database
        release_Date (str): date_to_filter

    Returns:
        list: all films released after the given release_date
    """
    collection = db["children_movies"]

    # Convert the input release date (string) to a datetime object
    release_date = datetime.strptime(release_Date, "%d %b %Y")

    # Fetch all movies where `omdb_details.released` exists
    movies = collection.find({"omdb_details.released": {"$exists": True}})

    # Manually filter by converting `released` from string to datetime
    filtered_movies = []
    for movie in movies:
        try:
            movie_date = datetime.strptime(movie["omdb_details"]["released"], "%d %b %Y")
            if movie_date > release_date:
                filtered_movies.append(movie)
        except ValueError:
            logger.error(f"Skipping movie {movie.get('title', 'Unknown')} due to invalid date format: {movie['omdb_details']['released']}")
    logger.info(f"filtered films extracted from the db after release_date:{release_Date} ")
    return filtered_movies
        
async def update_movie_details_in_db(session, collection):
    """Update movie details in MongoDB for movies missing OMDb details."""
    try:
        # Find movies where `omdb_details` is missing or empty
        movies = await collection.find({"$or": [{"omdb_details": None}, {"omdb_details": {}}]}).to_list(length=None)
        if not movies:
            logger.info("No movies found that need updating.")
            return

        logger.info(f"Found {len(movies)} movies to update.")

        # Create tasks for fetching and updating movie details
        tasks = []
        for task_id, movie in enumerate(movies, start=1):  # Assign a task_id starting from 1
            themoviedb_id = movie.get("id")
            result = await api_movies.get_more_informations(session, themoviedb_id)
            if not result:
                logger.info(f"this is not a film or not yet released {movie.get("title")}")
                collection.delete_one({"id": themoviedb_id})
                logger.info(f"this item  {movie.get("title")} is deleted from the database.")
            else:
                imdb_id = movie.get("imdb_id")
                if not imdb_id:
                    logger.warning(f"[Task {task_id}] Movie {movie['_id']} has no IMDb ID. Skipping.")
                    continue

                # Create a task with a unique task_id
                task = update_single_movie(session, collection, imdb_id, task_id)
                tasks.append(task)

        # Run all tasks in parallel
        await asyncio.gather(*tasks)
        logger.info("All movies have been processed.")

    except Exception as e:
        logger.error(f"Error during update: {e}")



async def update_single_movie(session, collection, imdb_id, task_id):
    """Fetch and update details for a single movie."""
    try:
        title, wikidata_id = await get_info_by_id(collection, imdb_id)
        movie_data = await api_movies.fetch_movie_omdb_wiki(session, title, imdb_id, wikidata_id, task_id)
        
        if not movie_data:
            logger.warning(f"[Task {task_id}] No data found for IMDb ID {imdb_id}. Skipping.")
            return

        # Update MongoDB mit neuen Daten
        await collection.update_one(
            {"imdb_id": imdb_id},
            {"$set": {"omdb_details": movie_data}}
        )
        
        logger.info(f"[Task {task_id}] Updated movie with IMDb ID {imdb_id}.")
        await asyncio.sleep(1)  # Rate-Limit

    except aiohttp.ClientError as e:
        logger.error(f"[Task {task_id}] Network error while updating IMDb ID {imdb_id}: {e}")
    except Exception as e:
        logger.error(f"[Task {task_id}] Unexpected error while updating IMDb ID {imdb_id}: {e}")
