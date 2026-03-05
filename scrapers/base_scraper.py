import requests
import time
import logging
import random
import os

class BaseScraper:
    def __init__(self, store_name):
        self.store_name = store_name
        self.setup_logging()
        # We use standard browser headers so the websites think we are a real human, not a bot.
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }

    def setup_logging(self):
        """
        Sets up structured logging. 
        Saves logs to the scrapers/logs folder.
        """
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        log_filename = os.path.join(log_dir, f"{self.store_name}_scraper.log")
        
        # Configure the logger to include timestamps and severity levels
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.store_name)
        self.logger.info(f"--- Initialized Scraper for {self.store_name} ---")

    def fetch_page(self, url, max_retries=3):
        """
        Fetches a webpage with rate limiting, retry logic, and exponential backoff.
        """
        for attempt in range(max_retries):
            try:
                # Rate Limiting: Random delay between 2 to 5 seconds so we don't overload the server
                delay = random.uniform(2.0, 5.0)
                time.sleep(delay)
                
                self.logger.info(f"Attempting to fetch: {url} (Attempt {attempt + 1}/{max_retries})")
                
                # The actual request
                response = requests.get(url, headers=self.headers, timeout=10)
                
                # Check if the request was successful
                if response.status_code == 200:
                    self.logger.info(f"Successfully fetched: {url}")
                    return response.text
                else:
                    self.logger.warning(f"Failed to fetch {url}. Server returned status code: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Network error while fetching {url}: {e}")
            
            # Exponential Backoff: Wait longer after each failure (e.g., 2s, 4s, 8s) plus a tiny random jitter
            backoff_time = (2 ** attempt) + random.uniform(0, 1)
            self.logger.info(f"Applying exponential backoff. Retrying in {backoff_time:.2f} seconds...")
            time.sleep(backoff_time)
            
        self.logger.error(f"Max retries ({max_retries}) reached for {url}. Giving up.")
        return None