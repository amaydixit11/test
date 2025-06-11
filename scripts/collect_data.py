# scripts/collect_data.py
import requests
import os
import time
import json
import logging
from datetime import datetime
from typing import Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LastFMDataCollector:
    def __init__(self, username: str, base_url: str = "https://mainstream.ghan.nl"):
        self.username = username
        self.base_url = base_url
        self.session = requests.Session()
        self.metadata_file = "metadata.json"
        
    def load_metadata(self) -> dict:
        """Load metadata including last timestamp"""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load metadata: {e}")
        
        return {
            "last_timestamp": None,
            "last_filename": None,
            "collection_count": 0
        }
    
    def save_metadata(self, metadata: dict):
        """Save metadata including last timestamp"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            logger.info("Metadata saved successfully")
        except Exception as e:
            logger.error(f"Could not save metadata: {e}")
    
    def collect_scrobbles(self, previous_timestamp: Optional[str] = None) -> Optional[str]:
        """
        Collect scrobbles from the Last.fm export service
        
        Args:
            previous_timestamp: Previous timestamp to use for incremental collection
            
        Returns:
            Filename of the collected data or None if failed
        """
        # Prepare the form data
        form_data = {
            'user': self.username,
            'type': 'scrobbles',
            'format': 'csv'
        }
        
        if previous_timestamp:
            form_data['stamp'] = previous_timestamp
            logger.info(f"Using previous timestamp: {previous_timestamp}")
        
        try:
            # Make the request to the export service
            logger.info(f"Requesting data for user: {self.username}")
            response = self.session.post(
                f"{self.base_url}/export.html",
                data=form_data,
                timeout=300  # 5 minutes timeout
            )
            response.raise_for_status()
            
            # Check if we got CSV data
            if response.headers.get('content-type', '').startswith('text/csv') or \
               response.text.startswith('artist,album,track'):
                
                # Generate filename with timestamp
                current_timestamp = int(time.time())
                filename = f"scrobbles-{self.username}-{current_timestamp}.csv"
                filepath = os.path.join("data/raw", filename)
                
                # Save the CSV data
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                logger.info(f"Data saved to: {filepath}")
                logger.info(f"File size: {len(response.text)} characters")
                
                # Count rows (excluding header)
                row_count = len(response.text.strip().split('\n')) - 1
                logger.info(f"Number of scrobbles collected: {row_count}")
                
                return filename
            else:
                logger.error("Response does not appear to be CSV data")
                logger.error(f"Response content: {response.text[:500]}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def extract_latest_timestamp(self, filename: str) -> Optional[str]:
        """
        Extract the latest timestamp from the collected CSV file
        
        Args:
            filename: Name of the CSV file
            
        Returns:
            Latest timestamp or None if extraction failed
        """
        filepath = os.path.join("data/raw", filename)
        
        try:
            import pandas as pd
            df = pd.read_csv(filepath)
            
            if 'date' in df.columns and len(df) > 0:
                # Convert date to timestamp and get the latest one
                df['timestamp'] = pd.to_datetime(df['date']).astype(int) // 10**9
                latest_timestamp = str(df['timestamp'].max())
                logger.info(f"Latest timestamp extracted: {latest_timestamp}")
                return latest_timestamp
            else:
                logger.warning("No date column found or empty dataset")
                return None
                
        except Exception as e:
            logger.error(f"Could not extract timestamp: {e}")
            return None
    
    def run_collection(self):
        """Run the complete data collection process"""
        logger.info("Starting Last.fm data collection")
        
        # Load existing metadata
        metadata = self.load_metadata()
        
        # Collect new data
        filename = self.collect_scrobbles(metadata.get("last_timestamp"))
        
        if filename:
            # Extract latest timestamp for next run
            latest_timestamp = self.extract_latest_timestamp(filename)
            
            # Update metadata
            metadata.update({
                "last_timestamp": latest_timestamp,
                "last_filename": filename,
                "collection_count": metadata.get("collection_count", 0) + 1,
                "last_collection_time": datetime.now().isoformat()
            })
            
            self.save_metadata(metadata)
            logger.info("Data collection completed successfully")
            
            # Write filename to a file for the next script to read
            with open("latest_file.txt", "w") as f:
                f.write(filename)
            
        else:
            logger.error("Data collection failed")
            return False
        
        return True

def main():
    username = os.getenv('LASTFM_USERNAME')
    
    if not username:
        logger.error("LASTFM_USERNAME environment variable not set")
        return False
    
    collector = LastFMDataCollector(username)
    success = collector.run_collection()
    
    if not success:
        exit(1)

if __name__ == "__main__":
    main()
