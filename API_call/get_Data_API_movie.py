import json
from dotenv import load_dotenv
import os
import aiohttp
import asyncio
import sys
import pandas as pd
import urllib 

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from logger import logger
import utils as u

# Load environment variables
load_dotenv()
TMDB_API_KEY = os.getenv('TMDB_API_KEY')
OMDB_API_KEY = os.getenv('OMDB_API_KEY2')

# Base TMDb URL for Kinderfilme
BASE_TMDB_DISCOVER_URL = (
    f"https://api.themoviedb.org/3/discover/movie?"
    f"api_key={TMDB_API_KEY}&with_genres=16,10751,12"
    f"&include_adult=false&certification.lte=PG-13,G,PG"
)

async def fetch_data(session, url, retries=3, timeout=10):
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=timeout) as response:
                response.raise_for_status()
                data = await response.json()
                if data is None:  # Falls die API ein `null`-JSON schickt
                    logger.warning(f"Received `None` response from {url}")
                    return {}  # Sicherstellen, dass niemals `None` zurückkommt
                return data  # Erfolgreiche Antwort zurückgeben
        except asyncio.TimeoutError:
            logger.error(f"Timeout error on attempt {attempt + 1}/{retries} for {url}")
        except aiohttp.ClientError as e:
            logger.error(f"Client error: {e} on {url}")

        await asyncio.sleep(2)  # Wait before retrying
    
    logger.error(f"All {retries} attempts failed for {url}. Returning empty dictionary.")
    return {}  # IMMER `{}` zurückgeben, niemals `None`


async def get_more_informations(session, tmdb_movie_id):
    """Holt IMDb-ID von TMDb."""
    try:
        if not tmdb_movie_id:
            logger.warning("Invalid TMDb movie ID: None or empty.")
            return {}
        
        url = f"https://api.themoviedb.org/3/movie/{tmdb_movie_id}?api_key={TMDB_API_KEY}&append_to_response=external_ids"
        data = await fetch_data(session, url)
        
        if not data:  # Falls `fetch_data` ein leeres Dictionary zurückgibt
            logger.warning(f"No valid data received from {url}")
            return {}
        
        # Check if the movie status is "Released"
        if data.get("status", "").lower() != "released" or data.get("media_type", "").lower() != 'movie':
            logger.warning(f"Movie with TMDb ID {tmdb_movie_id} is not released or not a film. Status: {data.get('status')}")
            return {}
        
        logger.debug(f"TMDb Response for {tmdb_movie_id}: {data}")
        return data
    except Exception as e:
        logger.error(f"Problem occurs in get_more_informations {str(e)}")
        return {}  # ✅ Fehlerfall abfangen, damit kein `None` zurückkommt
    
    
async def get_movie_details(session, movie, task_id):
    logger.info(f"[Task {task_id}] Starting to fetch details for movie: {movie.get('title')}")
    tmdb_movie_id = movie.get("id")
    if not tmdb_movie_id:
        logger.warning(f"[Task {task_id}] Skipping movie without TMDb ID: {movie}")
        movie["error"] = "Missing TMDb ID"
        return movie

    data = await get_more_informations(session, tmdb_movie_id)
    if not data:
        logger.warning(f"[Task {task_id}] Skipping movie {tmdb_movie_id} - No Data found.")
        movie["error"] = "No IMDb ID found"
        return movie

    await update_movie_data(session, movie, data)
    movie["omdb_details"] = await fetch_movie_omdb_wiki(session, movie.get('title'), data.get('imdb_id'), data.get('wikidata_id'), task_id)
    logger.debug(f"[Task {task_id}] Final movie details: {movie}")
    return movie


async def get_imdb_from_wikidata(session, wikidata_id):
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
    
    async with session.get(url) as response:
        data = await response.json()
        
        try:
            imdb_id = data["entities"][wikidata_id]["claims"]["P345"][0]["mainsnak"]["datavalue"]["value"]
            logger.info(f"Extracted IMDb ID {imdb_id} from Wikidata ID {wikidata_id}")
            return imdb_id
        except KeyError as e:
            logger.error(f"Problem by extracting imdb using wikidata {e}")
            return None
        
async def update_movie_data(session, movie, data):
    """ Aktualisiert das Movie-Dictionary mit fehlenden Informationen aus der API-Antwort. """
    
    if not isinstance(movie, dict):
        logger.error("update_movie_data received an invalid movie object (None or not a dict).")
        return
    
    if not isinstance(data, dict):
        logger.error(f"update_movie_data received invalid data for movie {movie.get('id', 'Unknown')}")
        return
    
    # Liste von relevanten Schlüsseln, die übernommen werden sollen
    keys_to_update = [
        "genres", "budget", "imdb_id",
        "homepage", "tagline", "status", "origin_country", "revenue",
        "production_companies", "production_countries", "spoken_languages"
    ]
    
    try:
        for key in keys_to_update:
            movie[key] = data.get(key)  # Ergänzen aus API-Daten
        
        # Falls external_ids existiert, IMDb ID extrahieren
        if "external_ids" in data:
            movie['wikidata_id'] = data["external_ids"].get("wikidata_id")
            if not movie.get("imdb_id"):  # Überprüfen, ob imdb_id fehlt
                if  data["external_ids"].get("imdb_id") is not None:
                    movie["imdb_id"] = data["external_ids"].get("imdb_id")
                elif movie.get('wikidata_id'):
                    logger.info(f"Trying to get IMDb from Wikidata: {movie['wikidata_id']}")
                    movie["imdb_id"] = await get_imdb_from_wikidata(session, movie['wikidata_id'])
                    logger.info(f"IMDb ID after Wikidata lookup: {movie['imdb_id']}")
                else:
                    movie["imdb_id"] = await get_missed_imdb_id(movie.get("title"))
        else:
            logger.warning(f"External IDs not found for movie {movie.get('id', 'Unknown')}.")

    except Exception as e:
        logger.error(f"Problem occurred: {str(e)} while updating data for movie {movie.get('id', 'Unknown')}.")

async def get_kinder_movies_parallel(session, page, limit):
    """Holt Kinderfilme von TMDb und verarbeitet sie parallel."""
    
    url = f"{BASE_TMDB_DISCOVER_URL}&limit={limit}&page={page}"
    try:
        data = await fetch_data(session, url)
        if not data:  # Falls `fetch_data` ein leeres Dictionary zurückgibt
            logger.warning(f"No valid data received from {url}")
            return []
        
        logger.debug(f"Raw API response: {data}")
        movies = data.get('results', [])
        if not movies:
            logger.warning(f"No results found for page {page}")
            return []
        
        for movie in movies:
            movie.pop("genre_ids", None)  # Remove if exists
            movie.pop("video", None)  # Remove if exists
            movie.pop("adult", None)

        tasks = [asyncio.create_task(get_movie_details(session, movie, task_id)) for task_id, movie in enumerate(movies)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed with error: {repr(result)}")

        return results
            
    except aiohttp.ClientError as e:
        logger.error(f"Network error: {e}. Retrying...")
        await asyncio.sleep(1)
        return []  # ✅ Rückgabe einer leeren Liste statt None

    except KeyError as e:
        logger.error(f"Data parsing error: {e}.")
        return []  # ✅ Rückgabe einer leeren Liste

    except Exception as e:
        logger.error(f"Unexpected error: {repr(e)}.")
        return []  # ✅ Rückgabe einer leeren Liste

async def search_wikipedia(session, film_title):
    """Sucht den besten Wikipedia-Treffer für einen Filmtitel."""
    base_url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": film_title,
        "format": "json"
    }
    try:
        async with session.get(base_url, params=params, timeout=10) as response:
            response.raise_for_status()  # Falls z. B. 404 oder 500 kommt, wird hier eine Exception geworfen
            data = await response.json()
            if "query" in data and "search" in data["query"] and len(data["query"]["search"]) > 0:
                return data["query"]["search"][0]["title"]
            return ""
    
    except aiohttp.ClientError as e:
        logger.error(f"Wikipedia Search API error: {e}")
        return ""

    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
        logger.error(f"Unexpected data format or parsing error: {repr(e)}")
        return ""

    except Exception as e:
        logger.error(f"Unexpected error in Wikipedia Search: {repr(e)}")
        return ""

async def get_wiki_beschreibung(session, film_titel):
    """Holt die Wikipedia-Beschreibung für einen Filmtitel."""
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{film_titel}"
    try:
        data = await fetch_data(session, url)
        if not data:  # Falls `fetch_data` ein leeres Dictionary zurückgibt
            logger.warning(f"No valid data received from {url}")
            return "", ""

        # Prüfen, ob die erwarteten Schlüssel existieren
        if "extract" in data and "content_urls" in data and "desktop" in data["content_urls"]:
            return data["extract"], data["content_urls"]["desktop"]["page"]

        logger.warning(f"Incomplete Wikipedia response: {data}")
        return "", ""

    except aiohttp.ClientError as e:
        logger.error(f"Wikipedia get Beschreibung API error: {e}")
        return "", ""

    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as e:
        logger.error(f"Unexpected data format or parsing error: {repr(e)}")
        return "", ""

    except Exception as e:
        logger.error(f"Unexpected error in Wikipedia get Beschreibung: {repr(e)}")
        return "", ""

async def fetch_movie_omdb_wiki(session, title, imdb_id, wikidata_id, task_id):
    """Holt Filmdetails von OMDb und Wikipedia parallel."""
    movie_data = {}
    
    if not imdb_id:
        logger.warning(f"[Task {task_id}] No IMDb ID provided, skipping OMDb fetch.")
        return movie_data

    try:
        url_omdb = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&i={imdb_id}"
        omdb_task = fetch_data(session, url_omdb)
        wiki_title_task = search_wikipedia(session, title)  # Wikipedia-Suche parallel starten
        
        wiki_data_task = None
        if wikidata_id:
            url_wiki = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
            wiki_data_task = fetch_data(session, url_wiki)

        # Alle Tasks parallel ausführen
        omdb_result, wiki_title = await asyncio.gather(omdb_task, wiki_title_task)
        
        # Debugging: Logge die Rohdaten
        logger.debug(f"[Task {task_id}] OMDb Response: {omdb_result}")
        
        if isinstance(omdb_result, dict) and omdb_result.get("Response") != "False":
            remove_keys = {"Title", "Released", "Genre", "Plot", "Language", "Country", 
                           "imdbID", "DVD", "Production", "Website", "Response"}
            movie_data = {k: v for k, v in omdb_result.items() if k not in remove_keys}
        else:
            logger.error(f"[Task {task_id}] OMDb API error for IMDb ID {imdb_id}: {omdb_result}")

        # Fallback: Falls `movie_data` nach dem Filtern leer ist
        if not movie_data:
            movie_data["fallback_title"] = title  # Wenigstens den Titel speichern
            logger.warning(f"[Task {task_id}] Filtered OMDb data was empty. Keeping fallback title.")
        
        # Wikipedia-Beschreibung abrufen
        link_page = None
        if isinstance(wiki_title, str):
            film_beschreibung, link_page = await get_wiki_beschreibung(session, u.get_title_abstract(wiki_title))
            movie_data["wikipedia_Description"] = film_beschreibung if film_beschreibung else "No Description"
        
        # Wikidata abrufen, falls verfügbar
        if wiki_data_task:
            wiki_data_result = await wiki_data_task
            if isinstance(wiki_data_result, dict) and wikidata_id in wiki_data_result.get("entities", {}):
                entity_data = wiki_data_result["entities"][wikidata_id].get("sitelinks", {})
                wiki_raw_url = entity_data.get("enwiki", {}).get("url")
                if not wiki_raw_url and entity_data:
                    first_key = next(iter(entity_data))
                    wiki_raw_url = entity_data[first_key]["url"]
                movie_data["wiki_page"] = urllib.parse.quote(wiki_raw_url, safe=":/") if wiki_raw_url else None
            else:
                movie_data["wiki_page"] = link_page or f"https://www.wikidata.org/wiki/{wikidata_id}"
        
        return movie_data
    except Exception as e:
        logger.error(f"[Task {task_id}] Error fetching movie data: {repr(e)}")
        return {}


async def get_missed_imdb_id(title):
    """Lädt IMDb-Daten, sucht parallele IMDb-IDs für fehlende Filme und speichert sie."""
    try:
        file_path = "Data\\title.basics.tsv.gz"  # Update mit dem korrekten Pfad
        df = pd.read_csv(file_path, sep="\t", dtype=str, na_values="\\N")

        matching_movies = df[df["primaryTitle"].str.contains(title, case=False, na=False)]
        if not matching_movies.empty:
            return matching_movies.iloc[0]["tconst"]  # Nimmt den ersten Treffer
        return None

    except Exception as e:
        logger.error(f"Ein Fehler ist aufgetreten: {e} by movie {title}")