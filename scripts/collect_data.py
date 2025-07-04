#!/usr/bin/env python3
"""
Last.fm Data Export Automation Script
Automates the process of exporting scrobbles from Last.fm using the mainstream.ghan.nl export tool
"""

import time
import os
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
import argparse
from datetime import datetime
from pathlib import Path

class LastFmExporter:
    def __init__(self, download_dir=None, headless=True, username=None):
        """
        Initialize the Last.fm exporter
        
        Args:
            download_dir (str): Directory to save downloaded files (defaults to data/raw)
            headless (bool): Run browser in headless mode (default: True)
            username (str): Last.fm username (defaults to environment variable or hardcoded)
        """
        self.url = "https://mainstream.ghan.nl/export.html"
        # Get username from parameter, environment variable, or fallback
        self.username = "amaydixit11"
        
        # Set default download directory to data/raw
        if download_dir is None:
            # Create data directory structure
            base_dir = Path(__file__).parent.parent if Path(__file__).parent.name == 'scripts' else Path.cwd()
            self.download_dir = str(base_dir / 'data' / 'raw')
        else:
            self.download_dir = download_dir
            
        # Ensure download directory exists
        os.makedirs(self.download_dir, exist_ok=True)
        
        self.headless = headless
        self.driver = None
        
    def setup_driver(self):
        """Setup Chrome WebDriver with appropriate options"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        # Set download directory
        prefs = {
            "download.default_directory": os.path.abspath(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Additional options for stability and GitHub Actions compatibility
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")  # Speed up loading
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(10)
            print("Chrome WebDriver initialized successfully")
            print(f"Download directory: {os.path.abspath(self.download_dir)}")
        except WebDriverException as e:
            print(f"Error initializing Chrome WebDriver: {e}")
            print("Make sure ChromeDriver is installed and in your PATH")
            sys.exit(1)
    
    def get_latest_timestamp(self):
        """
        Get the latest timestamp from existing CSV files to avoid duplicate downloads
        """
        try:
            csv_files = [f for f in os.listdir(self.download_dir) if f.endswith('.csv')]
            if not csv_files:
                return None
                
            # Sort files by modification time, get the newest
            csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(self.download_dir, x)), reverse=True)
            latest_file = csv_files[0]
            
            # Try to extract timestamp from filename if it follows a pattern
            # Last.fm exports typically include timestamps in filenames
            print(f"Found existing export: {latest_file}")
            
            # Read the last line of the CSV to get the latest scrobble timestamp
            file_path = os.path.join(self.download_dir, latest_file)
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                if len(lines) > 1:  # Skip header
                    last_line = lines[-1].strip()
                    # This is a simple approach - you might need to adjust based on CSV format
                    print(f"Latest export found, consider using incremental update")
                    
        except Exception as e:
            print(f"Error checking for existing files: {e}")
            return None
    
    def export_scrobbles(self, timestamp=None):
        """
        Export scrobbles data
        
        Args:
            timestamp (str): Optional timestamp to export scrobbles from that point forward
        """
        try:
            print(f"Navigating to {self.url}")
            self.driver.get(self.url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.ID, "user"))
            )
            print("Page loaded successfully")
            
            # Fill in username
            username_field = self.driver.find_element(By.ID, "user")
            username_field.clear()
            username_field.send_keys(self.username)
            print(f"Username '{self.username}' entered")
            
            # Select export type (Scrobbles)
            type_select = Select(self.driver.find_element(By.ID, "type"))
            type_select.select_by_value("scrobbles")
            print("Selected 'Scrobbles' as export type")
            
            # Select format (CSV)
            format_select = Select(self.driver.find_element(By.ID, "format"))
            format_select.select_by_value("csv")
            print("Selected 'CSV' as export format")
            
            # If timestamp is provided, enter it
            if timestamp:
                # Wait for timestamp field to become visible (it's hidden by default)
                timestamp_field = self.driver.find_element(By.ID, "stamp")
                
                # Make the field visible by removing the hidden class
                self.driver.execute_script(
                    "arguments[0].style.display = 'inline-block'; arguments[0].classList.remove('ui-helper-hidden');", 
                    timestamp_field
                )
                
                timestamp_field.clear()
                timestamp_field.send_keys(str(timestamp))
                print(f"Timestamp '{timestamp}' entered")
            
            # Get list of files in download directory before clicking Go
            files_before = set(os.listdir(self.download_dir))
            
            # Click Go button
            go_button = self.driver.find_element(By.ID, "go")
            go_button.click()
            print("Clicked 'Go' button - starting export process")
            
            # Wait for the export to complete and file to be downloaded
            # This can take a while for large datasets
            print("Waiting for export to complete...")
            downloaded_file = self.wait_for_download(files_before, timeout=300)  # 5 minutes timeout
            
            if downloaded_file:
                # Rename file with timestamp for better organization
                self.organize_downloaded_file(downloaded_file)
                return True
            
        except TimeoutException:
            print("Timeout waiting for page elements or download to complete")
            return False
        except Exception as e:
            print(f"An error occurred during export: {e}")
            return False
        
        return False
    
    def organize_downloaded_file(self, file_path):
        """
        Rename the downloaded file with a timestamp for better organization
        """
        try:
            if not os.path.exists(file_path):
                return
                
            # Get current timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Get file info
            file_dir = os.path.dirname(file_path)
            file_name = os.path.basename(file_path)
            name, ext = os.path.splitext(file_name)
            
            # Create new filename with timestamp
            new_name = f"lastfm_scrobbles_{self.username}_{timestamp}{ext}"
            new_path = os.path.join(file_dir, new_name)
            
            # Rename the file
            os.rename(file_path, new_path)
            print(f"📁 File renamed to: {new_name}")
            
            return new_path
            
        except Exception as e:
            print(f"Error organizing file: {e}")
            return file_path
    
    def wait_for_download(self, files_before, timeout=300):
        """
        Wait for file download to complete
        
        Args:
            files_before (set): Set of files that existed before download
            timeout (int): Maximum time to wait in seconds
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            time.sleep(2)
            
            # Check for new files
            files_after = set(os.listdir(self.download_dir))
            new_files = files_after - files_before
            
            # Filter for CSV files and exclude temporary Chrome download files
            csv_files = [f for f in new_files if f.endswith('.csv') and not f.endswith('.crdownload')]
            
            if csv_files:
                downloaded_file = csv_files[0]  # Take the first CSV file found
                file_path = os.path.join(self.download_dir, downloaded_file)
                print(f"✅ Export completed! File saved as: {file_path}")
                
                # Get file size
                file_size = os.path.getsize(file_path)
                print(f"📊 File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
                
                # Count approximate number of lines (rough estimate of scrobbles)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        line_count = sum(1 for _ in f) - 1  # Subtract header
                    print(f"📈 Approximate scrobbles: {line_count:,}")
                except:
                    pass
                
                return file_path
            
            # Check if there are any error messages on the page
            try:
                error_element = self.driver.find_element(By.ID, "error")
                if error_element.is_displayed():
                    error_text = error_element.text
                    print(f"❌ Error on page: {error_text}")
                    return None
            except:
                pass  # Error element not found or not displayed
            
            # Show progress if available
            try:
                progress_element = self.driver.find_element(By.ID, "progress")
                if progress_element.is_displayed():
                    progress_text = progress_element.text
                    if progress_text and "Starting..." not in progress_text:
                        print(f"📈 Progress: {progress_text}")
            except:
                pass  # Progress element not found or not displayed
        
        print("❌ Timeout waiting for download to complete")
        return None
    
    def close(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            print("WebDriver closed")

def main():
    parser = argparse.ArgumentParser(description="Export Last.fm scrobbles data")
    parser.add_argument("--timestamp", "-t", type=str, help="Unix timestamp to export scrobbles from")
    parser.add_argument("--download-dir", "-d", type=str, help="Directory to save downloaded file (default: data/raw)")
    parser.add_argument("--visible", "-v", action="store_true", help="Run browser in visible mode (not headless)")
    parser.add_argument("--username", "-u", type=str, help="Last.fm username (default: from env LASTFM_USERNAME)")
    parser.add_argument("--check-existing", action="store_true", help="Check for existing exports")
    
    args = parser.parse_args()
    
    # Create download directory if it doesn't exist
    download_dir = args.download_dir
    if not download_dir:
        # Default to data/raw structure
        base_dir = Path.cwd()
        download_dir = str(base_dir / 'data' / 'raw')
    
    os.makedirs(download_dir, exist_ok=True)
    
    # Initialize exporter
    exporter = LastFmExporter(
        download_dir=download_dir,
        headless=not args.visible,
        username=args.username
    )
    
    print("🎵 Last.fm Scrobbles Export Tool")
    print("=" * 40)
    print(f"Username: {exporter.username}")
    print(f"Export type: Scrobbles")
    print(f"Format: CSV")
    print(f"Download directory: {download_dir}")
    if args.timestamp:
        print(f"Timestamp: {args.timestamp}")
    print("=" * 40)
    
    # Check for existing exports if requested
    if args.check_existing:
        exporter.get_latest_timestamp()
    
    try:
        exporter.setup_driver()
        success = exporter.export_scrobbles(timestamp=args.timestamp)
        
        if success:
            print("✅ Export process completed successfully!")
        else:
            print("❌ Export process failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Export cancelled by user")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
    finally:
        exporter.close()

if __name__ == "__main__":
    main()()

# # Last.fm Export Automation Setup

# ## Requirements

# Create a `requirements.txt` file:

# ```txt
# selenium>=4.15.0
# ```

# ## Installation

# 1. **Install Python dependencies:**
#    ```bash
#    pip install -r requirements.txt
#    ```

# 2. **Install ChromeDriver:**
   
#    **Option 1: Using webdriver-manager (Recommended)**
#    ```bash
#    pip install webdriver-manager
#    ```
#    Then modify the script to use webdriver-manager by adding this import and changing the driver setup:
#    ```python
#    from webdriver_manager.chrome import ChromeDriverManager
   
#    # In setup_driver method, replace:
#    self.driver = webdriver.Chrome(options=chrome_options)
#    # With:
#    self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
#    ```

#    **Option 2: Manual installation**
#    - Download ChromeDriver from https://chromedriver.chromium.org/
#    - Make sure it matches your Chrome browser version
#    - Add ChromeDriver to your system PATH

# ## Usage

# ### Basic Usage (Export all scrobbles)
# ```bash
# python lastfm_export.py
# ```

# ### Export with timestamp (only scrobbles after specific date)
# ```bash
# python lastfm_export.py --timestamp 1640995200
# ```

# ### Specify custom download directory
# ```bash
# python lastfm_export.py --download-dir "/path/to/your/downloads"
# ```

# ### Run in visible mode (see browser window)
# ```bash
# python lastfm_export.py --visible
# ```

# ### Combined example
# ```bash
# python lastfm_export.py --timestamp 1640995200 --download-dir "./my_exports" --visible
# ```

# ## Command Line Arguments

# - `--timestamp, -t`: Unix timestamp to export scrobbles from that point forward
# - `--download-dir, -d`: Directory to save the downloaded CSV file (default: ./lastfm_exports)
# - `--visible, -v`: Run browser in visible mode instead of headless mode

# ## How to Get Unix Timestamp

# If you have a previous export and want to continue from where you left off:

# 1. **From a date:** Use an online converter like https://www.unixtimestamp.com/
# 2. **From previous export filename:** The timestamp is usually included in the filename
# 3. **From Python:**
#    ```python
#    import datetime
#    # For January 1, 2024
#    timestamp = int(datetime.datetime(2024, 1, 1).timestamp())
#    print(timestamp)  # 1704067200
#    ```

# ## Expected Output

# The script will:
# 1. Open Chrome browser (headless by default)
# 2. Navigate to the Last.fm export site
# 3. Fill in your username automatically
# 4. Select "Scrobbles" and "CSV" format
# 5. Enter timestamp if provided
# 6. Click "Go" and wait for the export to complete
# 7. Download the CSV file to the specified directory
# 8. Display progress and final file information

# ## File Format

# The exported CSV will contain columns like:
# - Artist
# - Album
# - Track
# - Date/Time
# - And other Last.fm metadata

# ## Troubleshooting

# ### Common Issues:

# 1. **ChromeDriver not found:**
#    - Make sure ChromeDriver is installed and in PATH
#    - Or use webdriver-manager as shown above

# 2. **Timeout errors:**
#    - Large datasets (>100k scrobbles) can take several minutes
#    - The script has a 5-minute timeout by default

# 3. **Permission errors:**
#    - Make sure the download directory is writable
#    - Run with appropriate permissions

# 4. **Network issues:**
#    - Check your internet connection
#    - The Last.fm API might be temporarily unavailable

# ### Debug Mode:
# Run with `--visible` flag to see what's happening in the browser window.

# ## Notes

# - The script is hardcoded for username "amaydixit11" as requested
# - Large exports can take several minutes to complete
# - The tool respects Last.fm's rate limits
# - Files are saved with timestamps in the filename for easy identification
