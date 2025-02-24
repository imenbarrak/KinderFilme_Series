def get_title_abstract(film_title):
    replacements = {":": "%3A", ".": "%2E", "'": "%27", ",":"%2C", "?":"%3F", "&":"%26", "!":"", 
                    "?":"","é":"%C3%A9"}  # Mapping characters to their replacements
    film_title_list = film_title.split()
    
    # Replace characters in each word
    for index in range(0,len(film_title_list)):
        for old, new in replacements.items():
            film_title_list[index] = film_title_list[index].replace(old, new)

    return '_'.join(film_title_list)

# 🔹 Tests:

#print(get_title_abstract("Harry Potter and the Goblet of Fire"))  # ➝ 
#print(get_title_abstract("Spider-Man: No Way Home"))  # ➝ "Spider-Man_%3A_Homecoming"
#print(get_title_abstract("Finding Nemo."))  # ➝ "Finding_Nemo."
#print(get_title_abstract("Cars 3"))
#print(get_title_abstract("The Emperor's New Groove"))
#print(get_title_abstract("Miraculous World: Paris, Tales of Shadybug and Claw Noir"))
#print(get_title_abstract("List of Miraculous: Tales of Ladybug & Cat Noir episodes"))
#print(get_title_abstract("The Boss Baby: Christmas Bonus"))
#print(get_title_abstract("Run, Tiger, Run!"))
#print(get_title_abstract("Pokémon 3 the Movie: Spell of the Unown"))
import os
import aiohttp
from dotenv import load_dotenv
import wikipedia
def get_wikipedia_informations(film_titel):
    """Fetch film description from Wikipedia"""
    try:
        summary = wikipedia.summary(film_titel, sentences=3, auto_suggest=True)
        return summary
    except wikipedia.exceptions.DisambiguationError as e:
        print(f"Mehrdeutige Ergebnisse für '{film_titel}': {e.options}")
        return None
    except wikipedia.exceptions.PageError:
        print(f"Keine Wikipedia-Seite für '{film_titel}' gefunden.")
        return None
    
import logger 
import time    
def update_movie_details_in_db(db, imdb_id, movie_data):
    """Update movie details in MongoDB."""
    collection = db["children_movies"]
    if not movie_data:
        logger.warning(f"Keine Daten für IMDb ID {imdb_id}, Update wird übersprungen.")
        return False

    # Speichern in MongoDB
    result = collection.update_one(
        {"imdb_id": imdb_id}, 
        {"$set": {"omdb_details": movie_data}}, 
        upsert=True
    )

    if "wikipedia_Description" in movie_data:
        collection.update_one(
            {"imdb_id": imdb_id},
            {"$set": {"wikipedia_Description": movie_data["wikipedia_Description"],
                      "wiki_page": movie_data["wiki_page"]}},
            upsert=True
        )
        logger.info(f"Wikipedia-Beschreibung für {imdb_id} wurde gespeichert.")
    time.sleep(1)
    logger.info(f"Film mit IMDb ID {imdb_id} wurde aktualisiert: "
                f"Matched: {result.matched_count}, Modified: {result.modified_count}")
    return True


""" async def get_external_ids(session, tmdb_movie_id): #Holt IMDb-ID von TMDb.
    if not tmdb_movie_id:
        logger.warning("Invalid TMDb movie ID: None or empty.")
        return
    
    url = f"https://api.themoviedb.org/3/movie/{tmdb_movie_id}?api_key={TMDB_API_KEY}&append_to_response=external_ids"
    data = await fetch_data(session, url)
    
    logger.debug(f"TMDb Response for {tmdb_movie_id}: {data}")
    
    if data and "external_ids" in data:
        imdb_id = data["external_ids"].get("imdb_id")
        if imdb_id:
            return imdb_id
        else:
            logger.warning(f"IMDb ID not found in external_ids for movie {tmdb_movie_id}")
    else:
        logger.warning(f"No external_ids field in TMDb response for movie {tmdb_movie_id}")
    
    return None
 """
# import sys
# import os

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from logger import logger
# import utils as u

# # Load environment variables
# load_dotenv()
# OMDB_API_KEYS = [os.getenv('OMDB_API_KEY1'), os.getenv('OMDB_API_KEY2')]
# api_key_index = 0  # Start mit dem ersten Key

# def get_next_api_key():
#     global api_key_index
#     key = OMDB_API_KEYS[api_key_index]
#     api_key_index = (api_key_index + 1) % len(OMDB_API_KEYS)  # Wechsle zum nächsten Key
#     return key


