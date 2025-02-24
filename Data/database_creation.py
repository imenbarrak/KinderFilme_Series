from dotenv import load_dotenv
import sys
import os
import aiohttp
import asyncio
import MongoDBContext  as MongoDBC
import database_operation
import time

# Füge den Elternordner zum Python-Pfad hinzu
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from logger import logger
import API_call.get_Data_API_movie as api_movies

async def insert_movies_into_db(collection, movies, page):
    """Speichert Filme in MongoDB mit batch insert und Deduplizierung."""
    if not isinstance(movies, list) or not movies:  # Check if movies is a list and not empty
        logger.info("No movies to save.")
        return
    try:
        # Fetch existing IMDb IDs from the database
        existing_movie_ids = await collection.distinct("id")
        logger.info(f"Length Existing_imdb_ids:{len(existing_movie_ids)}")
        # Filter out movies that already exist in the database
        unique_movies = [movie for movie in movies if movie and movie.get("id") and movie["id"] not in existing_movie_ids]

        # Insert only unique movies into the database
        if unique_movies:
            await collection.insert_many(unique_movies, ordered=False)
            logger.info(f"{len(unique_movies)} movies have been saved to the database from page {page}")
        else:
            logger.info("No movies to insert.")
    except Exception as e:
        logger.error(f"Error inserting movies: {str(e)}")


async def store_data_mongo_local(session, collection, page, limit):
    """Holt Filme von der API und speichert sie in MongoDB."""
    logger.info(f"Processing batch: Page {page}, Limit {limit}")
    try:
        # Fetch movies from the API
        movies = await api_movies.get_kinder_movies_parallel(session, page, limit)

        # Log the fetched movies
        logger.debug(f"Movies received for page {page}: {movies}")

        # Check if movies is None or not a list
        if not isinstance(movies, list) or movies is None:
            logger.warning(f"No valid movies returned for page {page}, limit {limit}")
            return

        # Insert movies into the database
        await insert_movies_into_db(collection, movies, page)
        #logger.info(f"{len(movies)} movies inserted into the database.")

        # Optional: Add a delay to avoid overloading the API
        await asyncio.sleep(5)

    except aiohttp.ClientError as e:
        logger.error(f'Network error in store_data_mongo_local: {e}')
    except Exception as e:
        logger.error(f'Error in store_data_mongo_local: {repr(e)}')
        
        
async def main_creation():
    """Main function to fetch and store movies in batches."""
    load_dotenv()
    items_per_page = int(os.getenv('items_per_page_tmdb', 20))
    num_pages = int(os.getenv('num_pages_tmdb', 5))
    timeout = aiohttp.ClientTimeout(total=15)
    mongo_uri = os.getenv('mongo_uri', 5)
    
    start_time = time.time()  # Record start time
      
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with MongoDBC.MongoDBContext(mongo_uri) as (client, db):
                if client is None or db is None:
                    logger.error("Failed to connect to MongoDB. Exiting...")
                    return
                
                collection = db["children_movies"]
                tasks = [store_data_mongo_local(session, collection, page, items_per_page) for page in range(1, num_pages + 1)]

                semaphore = asyncio.Semaphore(5)

                async def limited_task(task_id, task):
                    async with semaphore:
                        try:
                            await task
                        except Exception as e:
                            logger.error(f"Error in task {task_id}: {e}")
                            return

                batch_size = 5
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i + batch_size]
                    
                    await asyncio.gather(*[limited_task(i + j + 1, task) for j, task in enumerate(batch)])
                    
                    logger.info(f"Batch {i // batch_size + 1} completed. Sleeping before starting next batch...")
                    await asyncio.sleep(2)
                    
        elapsed_time = time.time() - start_time  # Calculate elapsed time
        logger.info(f"Data storage process completed. Time elapsed: {elapsed_time:.2f} seconds.")

    except Exception as e:
        logger.error(f'Error in main_creation: {e}')


async def main_update():
    
    load_dotenv()
    mongo_uri = os.getenv('mongo_uri', 5)
    
    async with aiohttp.ClientSession() as session:
        async with MongoDBC.MongoDBContext(mongo_uri) as (client, db):
            if client is None or db is None:
                logger.error("Failed to get a valid MongoDB client or database")
                return  # Beende die Funktion, um weitere Fehler zu vermeiden
            else:
                collection = db["children_movies"]
                await database_operation.update_movie_details_in_db(session, collection)
                await client.close()


if __name__ == "__main__":
    try: 
        asyncio.run(main_update())
        #asyncio.run(main_creation())
    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}")