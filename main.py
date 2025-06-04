import requests
from bs4 import BeautifulSoup, SoupStrainer
import json
from datetime import datetime
import pytz
import os
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import lxml  # Much faster parser than html.parser
import cchardet  # For faster encoding detection

class NasdaqScraper:
    def __init__(self, check_interval=60):  # Default check every 60 seconds
        self.url = "https://listingcenter.nasdaq.com/rulebook/nasdaq/rulefilings"
        self.last_id_file = "last_processed_id.txt"
        self.webhook_url = "https://discord.com/api/webhooks/1378087722031386624/T39exvsnkSYfEekML7OyKuhe6KBrZks6Rr8Z1MQVGw4A_m33uEaYBPJaWbFsfBK1sPQC"
        
        # Initialize session with optimized settings
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Enable optimized connection pooling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,  # Increased pool size
            pool_maxsize=20,
            max_retries=3,
            pool_block=False
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Enable session caching
        self.session.cookies.set_policy(None)  # Allow all cookies
        
        self.check_interval = check_interval
        self.is_running = True
        
        # Pre-compile message template once
        self.message_template = (
            "📜 **NASDAQ Rule Filing Update**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📋 **Rule Filing:** %s\n\n"
            "📝 **Description:**\n%s\n\n"
            "📊 **Status:** %s\n"
            "📬 **SEC for Comment:** %s\n"
            "⏰ **Timestamp:** %s\n"
            "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        
        # Cache for last processed ID to reduce file I/O
        self._last_processed_id_cache = None
        
        # Pre-compile the parser and strainer
        self._parser = 'lxml'
        self._strainer = SoupStrainer('div', id='NASDAQ')
        
        # Initialize request retry settings
        self._max_retries = 3
        self._retry_delay = 1  # seconds
        
    def _fetch_with_retry(self, url, timeout=10):
        """Fetch URL with retry logic and optimized settings"""
        last_exception = None
        
        for attempt in range(self._max_retries):
            try:
                if attempt > 0:
                    print(f"[INFO] Retry attempt {attempt + 1}/{self._max_retries}")
                    time.sleep(self._retry_delay)
                
                response = self.session.get(
                    url,
                    timeout=timeout,
                    allow_redirects=True,
                    verify=True,
                    stream=True  # Enable streaming
                )
                response.raise_for_status()
                
                # Stream and decompress content
                content = b''
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        content += chunk
                
                return content
                
            except requests.exceptions.RequestException as e:
                print(f"[WARN] Fetch attempt {attempt + 1} failed: {str(e)}")
                last_exception = e
                continue
        
        if last_exception:
            raise last_exception
        
    def get_last_processed_id(self):
        """Read the last processed ID from cache or file"""
        if self._last_processed_id_cache is not None:
            return self._last_processed_id_cache
            
        try:
            with open(self.last_id_file, 'r') as f:
                last_id = f.read().strip()
                self._last_processed_id_cache = last_id
                print("[INFO] Last processed ID: %s" % last_id)
                return last_id
        except FileNotFoundError:
            print("[INFO] No previous ID found - will process all entries")
            return None
            
    def save_last_processed_id(self, rule_id):
        """Save the most recent processed ID to cache and file"""
        print("[INFO] Saving new last processed ID: %s" % rule_id)
        self._last_processed_id_cache = rule_id
        with open(self.last_id_file, 'w') as f:
            f.write(rule_id)
            
    def extract_row_data(self, tr):
        """Extract data from a table row efficiently"""
        try:
            current_id = tr.get('id')
            if not current_id:
                return None
                
            # Get all td elements at once and extract text directly
            tds = tr.find_all('td', limit=4)
            if len(tds) < 4:
                return None
                
            # Extract text directly without additional stripping
            return {
                'id': current_id,
                'rule_filing': tds[0].get_text(strip=True),
                'description': tds[1].get_text(strip=True),
                'status': tds[2].get_text(strip=True),
                'noticed_by_sec': tds[3].get_text(strip=True)
            }
        except Exception:
            return None
            
    def scrape_nasdaq_filings(self):
        """Scrape NASDAQ rule filings and extract new entries"""
        try:
            scrape_start = time.time()
            print("\n[INFO] Starting NASDAQ scraping at %s" % datetime.now())
            
            # Get the webpage content using optimized fetching
            content = self._fetch_with_retry(self.url)
            
            fetch_time = time.time() - scrape_start
            print("[INFO] Fetched webpage in %.2f seconds" % fetch_time)
            
            # Parse HTML efficiently using lxml and SoupStrainer
            parse_start = time.time()
            soup = BeautifulSoup(content, self._parser, parse_only=self._strainer)
            
            # Find the table directly (since we already filtered for NASDAQ div)
            table = soup.find('table')
            if not table:
                raise Exception("Could not find table")
                
            # Get tbody and validate
            tbody = table.find('tbody')
            if not tbody:
                raise Exception("Could not find table body")
                
            parse_time = time.time() - parse_start
            print("[INFO] Parsed HTML in %.2f seconds" % parse_time)
            
            # Get current timestamp in UTC
            timestamp = datetime.now(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
            
            # Get the last processed ID from cache
            last_processed_id = self.get_last_processed_id()
            
            # Process rows efficiently
            process_start = time.time()
            new_entries = []
            total_rows = 0
            
            # Get all rows at once and process in parallel
            rows = tbody.find_all('tr')[1:]  # Skip header row
            
            # Use ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=10) as executor:
                # Process rows in parallel
                row_data_list = list(executor.map(self.extract_row_data, rows))
            
            # Process results
            for row_data in row_data_list:
                total_rows += 1
                
                if not row_data:
                    continue
                    
                if row_data['id'] == last_processed_id:
                    print("[INFO] Found last processed ID at row %d" % total_rows)
                    break
                    
                # Format message using pre-compiled template
                message = self.message_template % (
                    row_data['rule_filing'],
                    row_data['description'],
                    row_data['status'],
                    row_data['noticed_by_sec'],
                    timestamp
                )
                
                new_entries.append({
                    "Rule Filing": row_data['rule_filing'],
                    "Description": row_data['description'],
                    "Status": row_data['status'],
                    "SEC for Comment": row_data['noticed_by_sec'],
                    "timestamp": timestamp,
                    "message": message
                })
                print("[INFO] Found new entry: %s" % row_data['rule_filing'])
            
            process_time = time.time() - process_start
            total_time = time.time() - scrape_start
            
            print("\n[INFO] Scraping Performance:")
            print("- Fetch time: %.2f seconds" % fetch_time)
            print("- Parse time: %.2f seconds" % parse_time)
            print("- Process time: %.2f seconds" % process_time)
            print("- Total time: %.2f seconds" % total_time)
            print("\n[INFO] Processed %d total rows" % total_rows)
            print("[INFO] Found %d new entries" % len(new_entries))
            
            # Update last processed ID if new entries found
            if new_entries:
                self.save_last_processed_id(new_entries[0]["Rule Filing"])
                
            return new_entries
            
        except Exception as e:
            print("\n[ERROR] Error scraping NASDAQ filings: %s" % str(e))
            return []

    def send_single_entry(self, entry, entry_num, total_entries):
        """Send a single entry to Discord"""
        try:
            print("[INFO] Sending entry %d/%d to Discord..." % (entry_num, total_entries))
            
            # Send message using webhook with session and timeout
            response = self.session.post(
                self.webhook_url, 
                json={'content': entry['message']},
                timeout=5
            )
            if response.status_code == 204:
                print("[SUCCESS] Entry sent successfully")
                return True
            else:
                print("[ERROR] Failed to send entry. Status code: %s" % response.status_code)
                return False
                
        except Exception as e:
            print("[ERROR] Error sending entry to Discord: %s" % str(e))
            return False

    def send_to_discord(self, entries):
        """Send entries to Discord using webhook with parallel processing"""
        if not entries:
            return

        print("\n[INFO] Starting to send entries to Discord...")
        success_count = 0
        error_count = 0
        total_entries = len(entries)

        # Increase worker count for more parallelism
        max_workers = min(5, total_entries)  # Up to 5 workers, but no more than entries
        
        # Create a partial function with fixed arguments
        send_func = partial(self.send_single_entry, total_entries=total_entries)
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks and get futures
            futures = [executor.submit(send_func, entry, i+1) 
                      for i, entry in enumerate(entries)]
            
            # Process results as they complete
            for future in futures:
                if future.result():
                    success_count += 1
                else:
                    error_count += 1
                # Reduced delay between sends
                time.sleep(0.2)  # 200ms delay instead of 500ms
        
        print("\n[SUMMARY] Discord sending complete:")
        print("- Total entries processed: %d" % total_entries)
        print("- Successfully sent: %d" % success_count)
        print("- Failed to send: %d" % error_count)

    def run_continuously(self):
        """Run the scraper continuously with specified interval"""
        print("\n" + "="*50)
        print("NASDAQ Rule Filings Scraper - Continuous Mode")
        print("="*50)
        print("Started at: %s" % datetime.now())
        print("Check interval: %d seconds" % self.check_interval)
        print("Press Ctrl+C to stop")
        print("="*50)

        while self.is_running:
            try:
                # Get current time for this iteration
                current_time = datetime.now()
                print("\n[INFO] Running check at %s" % current_time)

                # Run the scraper
                new_entries = self.scrape_nasdaq_filings()
                
                if new_entries:
                    print("\n[INFO] Starting Discord update process...")
                    self.send_to_discord(new_entries)
                    print("\n[INFO] Updates completed successfully")
                else:
                    print("\n[INFO] No new entries found")

                # Wait for next check
                print("\n[INFO] Next check in %d seconds..." % self.check_interval)
                time.sleep(self.check_interval)

            except KeyboardInterrupt:
                print("\n[INFO] Received stop signal. Shutting down gracefully...")
                self.is_running = False
                break
            except Exception as e:
                print("\n[ERROR] An error occurred: %s" % str(e))
                print("[INFO] Will retry in %d seconds..." % self.check_interval)
                time.sleep(self.check_interval)

        print("\nScraper stopped at: %s" % datetime.now())
        print("="*50)

def main():
    # Create scraper with 60 second check interval
    scraper = NasdaqScraper(check_interval=60)
    
    try:
        # Run continuously
        scraper.run_continuously()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        scraper.is_running = False

if __name__ == "__main__":
    main()