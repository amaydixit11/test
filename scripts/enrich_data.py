# scripts/enrich_data.py
import pandas as pd
import requests
import time
import json
import pickle
import os
from urllib.parse import quote
from collections import defaultdict
import logging
from typing import List, Dict, Optional
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/enrichment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LastFMEnricher:
    def __init__(self, api_key: str, rate_limit_delay: float = 0.2, cache_file: str = "cache/lastfm_cache.pkl"):
        """
        Initialize the Last.fm data enricher
        
        Args:
            api_key: Your Last.fm API key
            rate_limit_delay: Delay between API calls to respect rate limits (seconds)
            cache_file: File to store cached API responses
        """
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay
        self.base_url = "http://ws.audioscrobbler.com/2.0/"
        self.session = requests.Session()
        self.cache_file = cache_file
        
        # Ensure cache directory exists
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        
        # Load existing cache
        self.track_cache = {}
        self.artist_cache = {}
        self.album_cache = {}
        self.load_cache()
        
    def load_cache(self):
        """Load cache from file if it exists"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'rb') as f:
                    cache_data = pickle.load(f)
                    self.track_cache = cache_data.get('track_cache', {})
                    self.artist_cache = cache_data.get('artist_cache', {})
                    self.album_cache = cache_data.get('album_cache', {})
                logger.info(f"Loaded cache: {len(self.track_cache)} tracks, {len(self.artist_cache)} artists, {len(self.album_cache)} albums")
            except Exception as e:
                logger.warning(f"Could not load cache: {e}")
                self.track_cache = {}
                self.artist_cache = {}
                self.album_cache = {}
    
    def save_cache(self):
        """Save cache to file"""
        try:
            cache_data = {
                'track_cache': self.track_cache,
                'artist_cache': self.artist_cache,
                'album_cache': self.album_cache
            }
            with open(self.cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            logger.info(f"Saved cache: {len(self.track_cache)} tracks, {len(self.artist_cache)} artists, {len(self.album_cache)} albums")
        except Exception as e:
            logger.error(f"Could not save cache: {e}")
    
    def get_artist_tags(self, artist: str, top_n: int = 5) -> List[str]:
        """Get top tags for an artist (with caching)"""
        cache_key = artist.lower().strip()
        
        if cache_key in self.artist_cache:
            cached_tags = self.artist_cache[cache_key]
            return cached_tags[:top_n]
            
        params = {
            'method': 'artist.gettoptags',
            'artist': artist,
            'api_key': self.api_key,
            'format': 'json',
            'autocorrect': 1
        }
        
        try:
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'toptags' in data and 'tag' in data['toptags']:
                tags = data['toptags']['tag']
                if isinstance(tags, dict):
                    tags = [tags]
                
                tag_names = [tag['name'] for tag in tags]
                self.artist_cache[cache_key] = tag_names
                return tag_names[:top_n]
            
            self.artist_cache[cache_key] = []
            return []
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for artist '{artist}': {e}")
            self.artist_cache[cache_key] = []
            return []
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"Error parsing response for artist '{artist}': {e}")
            self.artist_cache[cache_key] = []
            return []
    
    def get_track_tags(self, artist: str, track: str, top_n: int = 5) -> List[str]:
        """Get top tags for a track (with caching)"""
        cache_key = f"{artist.lower().strip()}|||{track.lower().strip()}"
        
        if cache_key in self.track_cache:
            cached_tags = self.track_cache[cache_key]
            return cached_tags[:top_n]
            
        params = {
            'method': 'track.gettoptags',
            'artist': artist,
            'track': track,
            'api_key': self.api_key,
            'format': 'json',
            'autocorrect': 1
        }
        
        try:
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'toptags' in data and 'tag' in data['toptags']:
                tags = data['toptags']['tag']
                if isinstance(tags, dict):
                    tags = [tags]
                
                tag_names = [tag['name'] for tag in tags]
                self.track_cache[cache_key] = tag_names
                return tag_names[:top_n]
            
            self.track_cache[cache_key] = []
            return []
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for track '{artist} - {track}': {e}")
            self.track_cache[cache_key] = []
            return []
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"Error parsing response for track '{artist} - {track}': {e}")
            self.track_cache[cache_key] = []
            return []
    
    def get_album_tags(self, artist: str, album: str, top_n: int = 5) -> List[str]:
        """Get top tags for an album (with caching)"""
        cache_key = f"{artist.lower().strip()}|||{album.lower().strip()}"
        
        if cache_key in self.album_cache:
            cached_tags = self.album_cache[cache_key]
            return cached_tags[:top_n]
        
        params = {
            'method': 'album.gettoptags',
            'artist': artist,
            'album': album,
            'api_key': self.api_key,
            'format': 'json',
            'autocorrect': 1
        }
        
        try:
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'toptags' in data and 'tag' in data['toptags']:
                tags = data['toptags']['tag']
                if isinstance(tags, dict):
                    tags = [tags]
                
                tag_names = [tag['name'] for tag in tags]
                self.album_cache[cache_key] = tag_names
                return tag_names[:top_n]
            
            self.album_cache[cache_key] = []
            return []
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for album '{artist} - {album}': {e}")
            self.album_cache[cache_key] = []
            return []
        except (KeyError, json.JSONDecodeError) as e:
            logger.error(f"Error parsing response for album '{artist} - {album}': {e}")
            self.album_cache[cache_key] = []
            return []
    
    def analyze_duplicates(self, input_file: str):
        """Analyze duplicate patterns in the data"""
        df = pd.read_csv(input_file)
        
        total_rows = len(df)
        unique_artists = df['artist'].nunique()
        unique_tracks = df[['artist', 'track']].drop_duplicates().shape[0]
        unique_albums = df[['artist', 'album']].drop_duplicates().shape[0]
        
        logger.info(f"Data Analysis:")
        logger.info(f"Total scrobbles: {total_rows:,}")
        logger.info(f"Unique artists: {unique_artists:,}")
        logger.info(f"Unique albums: {unique_albums:,}")
        logger.info(f"Unique tracks: {unique_tracks:,}")
        
        return unique_artists, unique_albums, unique_tracks
    
    def enrich_scrobbles(self, input_file: str, output_file: str, 
                        use_track_tags: bool = True, use_album_tags: bool = True, 
                        use_artist_tags: bool = True, top_n_tags: int = 5):
        """Enrich scrobble data with tags (optimized for duplicates)"""
        
        logger.info("Analyzing data structure...")
        unique_artists, unique_albums, unique_tracks = self.analyze_duplicates(input_file)
        
        logger.info(f"Reading data from {input_file}")
        df = pd.read_csv(input_file)
        
        # Process unique combinations to minimize API calls
        if use_track_tags:
            unique_tracks_df = df[['artist', 'track']].drop_duplicates()
            logger.info(f"Processing {len(unique_tracks_df)} unique tracks...")
            
            api_calls_needed = sum(1 for _, row in unique_tracks_df.iterrows() 
                                 if f"{str(row['artist']).lower().strip()}|||{str(row['track']).lower().strip()}" not in self.track_cache)
            
            logger.info(f"API calls needed for tracks: {api_calls_needed}")
            
            call_count = 0
            for idx, row in unique_tracks_df.iterrows():
                artist = str(row['artist']).strip()
                track = str(row['track']).strip()
                cache_key = f"{artist.lower().strip()}|||{track.lower().strip()}"
                
                if cache_key not in self.track_cache:
                    call_count += 1
                    logger.info(f"Fetching track tags {call_count}/{api_calls_needed}: {artist} - {track}")
                    self.get_track_tags(artist, track, top_n_tags)
                    time.sleep(self.rate_limit_delay)
                    
                    if call_count % 50 == 0:
                        self.save_cache()
        
        if use_album_tags:
            unique_albums_df = df[['artist', 'album']].drop_duplicates()
            unique_albums_df = unique_albums_df[
                (unique_albums_df['album'].notna()) & 
                (unique_albums_df['album'].str.strip() != '')
            ]
            logger.info(f"Processing {len(unique_albums_df)} unique albums...")
            
            api_calls_needed = sum(1 for _, row in unique_albums_df.iterrows() 
                                 if f"{str(row['artist']).lower().strip()}|||{str(row['album']).lower().strip()}" not in self.album_cache)
            
            logger.info(f"API calls needed for albums: {api_calls_needed}")
            
            call_count = 0
            for idx, row in unique_albums_df.iterrows():
                artist = str(row['artist']).strip()
                album = str(row['album']).strip()
                cache_key = f"{artist.lower().strip()}|||{album.lower().strip()}"
                
                if cache_key not in self.album_cache:
                    call_count += 1
                    logger.info(f"Fetching album tags {call_count}/{api_calls_needed}: {artist} - {album}")
                    self.get_album_tags(artist, album, top_n_tags)
                    time.sleep(self.rate_limit_delay)
                    
                    if call_count % 50 == 0:
                        self.save_cache()
        
        if use_artist_tags:
            unique_artists_list = df['artist'].unique()
            logger.info(f"Processing {len(unique_artists_list)} unique artists...")
            
            api_calls_needed = sum(1 for artist in unique_artists_list 
                                 if str(artist).lower().strip() not in self.artist_cache)
            
            logger.info(f"API calls needed for artists: {api_calls_needed}")
            
            call_count = 0
            for artist in unique_artists_list:
                cache_key = str(artist).lower().strip()
                
                if cache_key not in self.artist_cache:
                    call_count += 1
                    logger.info(f"Fetching artist tags {call_count}/{api_calls_needed}: {artist}")
                    self.get_artist_tags(str(artist), top_n_tags)
                    time.sleep(self.rate_limit_delay)
                    
                    if call_count % 50 == 0:
                        self.save_cache()
        
        self.save_cache()
        
        # Apply cached results to all rows
        logger.info("Applying cached results to all scrobbles...")
        df['track_tags'] = ''
        df['album_tags'] = ''
        df['artist_tags'] = ''
        df['combined_tags'] = ''
        
        for idx, row in df.iterrows():
            artist = str(row['artist']).strip()
            track = str(row['track']).strip()
            album = str(row['album']).strip() if pd.notna(row['album']) else ''
            
            # Get cached tags
            track_tags = []
            if use_track_tags:
                cache_key = f"{artist.lower().strip()}|||{track.lower().strip()}"
                track_tags = self.track_cache.get(cache_key, [])[:top_n_tags]
                df.at[idx, 'track_tags'] = '|'.join(track_tags)
            
            album_tags = []
            if use_album_tags and album:
                cache_key = f"{artist.lower().strip()}|||{album.lower().strip()}"
                album_tags = self.album_cache.get(cache_key, [])[:top_n_tags]
                df.at[idx, 'album_tags'] = '|'.join(album_tags)
            
            artist_tags = []
            if use_artist_tags:
                cache_key = artist.lower().strip()
                artist_tags = self.artist_cache.get(cache_key, [])[:top_n_tags]
                df.at[idx, 'artist_tags'] = '|'.join(artist_tags)
            
            # Combine tags with priority: track > album > artist
            combined_tags = track_tags.copy()
            
            for tag in album_tags:
                if tag not in combined_tags:
                    combined_tags.append(tag)
            
            for tag in artist_tags:
                if tag not in combined_tags:
                    combined_tags.append(tag)
            
            df.at[idx, 'combined_tags'] = '|'.join(combined_tags[:top_n_tags])
        
        # Save final results
        logger.info(f"Saving final results to {output_file}")
        df.to_csv(output_file, index=False)
        logger.info("Enrichment complete!")
        
        self.print_stats(df)
    
    def print_stats(self, df: pd.DataFrame):
        """Print statistics about the enriched data"""
        total_rows = len(df)
        rows_with_track_tags = len(df[df['track_tags'] != ''])
        rows_with_album_tags = len(df[df['album_tags'] != ''])
        rows_with_artist_tags = len(df[df['artist_tags'] != ''])
        rows_with_any_tags = len(df[df['combined_tags'] != ''])
        
        logger.info(f"\n--- Enrichment Statistics ---")
        logger.info(f"Total scrobbles: {total_rows}")
        logger.info(f"Scrobbles with track tags: {rows_with_track_tags} ({rows_with_track_tags/total_rows*100:.1f}%)")
        logger.info(f"Scrobbles with album tags: {rows_with_album_tags} ({rows_with_album_tags/total_rows*100:.1f}%)")
        logger.info(f"Scrobbles with artist tags: {rows_with_artist_tags} ({rows_with_artist_tags/total_rows*100:.1f}%)")
        logger.info(f"Scrobbles with any tags: {rows_with_any_tags} ({rows_with_any_tags/total_rows*100:.1f}%)")

def get_latest_filename():
    """Get the latest filename from the collection script"""
    try:
        with open("latest_file.txt", "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        logger.error("latest_file.txt not found - collection script may have failed")
        return None

def main():
    api_key = os.getenv('LASTFM_API_KEY')
    
    if not api_key:
        logger.error("LASTFM_API_KEY environment variable not set")
        return False
    
    # Get the latest filename from collection script
    latest_filename = get_latest_filename()
    if not latest_filename:
        return False
    
    input_file = os.path.join("data/raw", latest_filename)
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"enriched_{latest_filename.replace('.csv', '')}_{timestamp}.csv"
    output_file = os.path.join("data/enriched", output_filename)
    
    # Create enricher instance
    enricher = LastFMEnricher(api_key=api_key, rate_limit_delay=0.2)
    
    # Process the data
    try:
        enricher.enrich_scrobbles(
            input_file=input_file,
            output_file=output_file,
            use_track_tags=True,
            use_album_tags=True,
            use_artist_tags=True,
            top_n_tags=5
        )
        return True
    except Exception as e:
        logger.error(f"Enrichment failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
