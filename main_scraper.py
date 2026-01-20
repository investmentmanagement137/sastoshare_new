import os
import time
import csv
import re
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import boto3
from botocore.client import Config
from playwright.sync_api import sync_playwright
import cloudscraper

def sanitize_filename(name):
    return re.sub(r'[<>:"/\\|?*]', '', name).strip()

def sanitize_s3_key(name):
    """Replace all special characters with - for S3 keys, keeping only alphanumeric, dots, and hyphens."""
    return re.sub(r'[^a-zA-Z0-9.\-]', '-', name)

def upload_to_supabase(file_path):
    access_key = os.environ.get("SUPABASE_ACCESS_KEY_ID")
    secret_key = os.environ.get("SUPABASE_SECRET_ACCESS_KEY")
    endpoint_url = "https://unbgkfatcaztstordiyt.storage.supabase.co/storage/v1/s3"
    region_name = "ap-southeast-1"
    bucket_name = "investment_management"
    
    if not access_key or not secret_key:
        print("Skipping S3 upload: Credentials not found in environment.")
        return

    try:
        s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
            config=Config(signature_version='s3v4')
        )
        
        file_name = os.path.basename(file_path)
        s3_key = sanitize_s3_key(file_name)
        print(f"Uploading {file_name} as '{s3_key}' to bucket '{bucket_name}'...")
        s3.upload_file(file_path, bucket_name, s3_key)
        print("Upload successful.")
    except Exception as e:
        print(f"Failed to upload {file_path}: {e}")

def scrape_main_sections():
    url = "https://nepsealpha.com/mutual-fund-navs"
    print(f"Navigating to {url}...")
    
    stock_holdings_file = None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            page.goto(url)
            page.wait_for_selector("#home", timeout=60000)
            
            # Sections configuration
            sections = [
                {"name": "NAV", "tab_href": "#home", "table_id": "DataTables_Table_0", "select_name": "DataTables_Table_0_length"},
                {"name": "Stock_Holdings_Fund_PE_Ratio", "tab_href": "#stkHolding", "table_id": "DataTables_Table_1", "select_name": "DataTables_Table_1_length"},
                {"name": "MF_Assets_Allocation", "tab_href": "#assetsAllocation", "table_id": "DataTables_Table_2", "select_name": "DataTables_Table_2_length"},
                {"name": "Distributable_Dividend", "tab_href": "#distributableDividend", "table_id": "DataTables_Table_3", "select_name": "DataTables_Table_3_length"}
            ]
            
            today_str = datetime.now().strftime("%d-%m-%Y")
            
            for section in sections:
                print(f"Scraping {section['name']}...")
                try:
                    # Click tab
                    page.click(f"a[href='{section['tab_href']}']")
                    time.sleep(2) # Short wait for transitions
                    
                    # Show all entries
                    page.select_option(f"select[name='{section['select_name']}']", "100")
                    time.sleep(2) # Wait for table redraw
                    
                    # Parse
                    soup = BeautifulSoup(page.content(), 'html.parser')
                    table = soup.find('table', {'id': section['table_id']})
                    
                    if table:
                        # Clean up header text
                        headers = [th.text.strip() for th in table.find('thead').find_all('th')]
                        rows = []
                        tbody = table.find('tbody')
                        if tbody:
                            for tr in tbody.find_all('tr'):
                                 # Use separator for multiline cells if needed, or just space
                                cells = [td.text.strip() for td in tr.find_all('td')]
                                if len(cells) == len(headers):
                                    rows.append(cells)
                        
                        if rows:
                            filename = f"{section['name']}-{today_str}.csv"
                            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                                writer = csv.writer(f)
                                writer.writerow(headers)
                                writer.writerows(rows)
                            print(f"Saved {filename}")
                            print(f"Debug: Headers found for {section['name']}: {headers}")
                            upload_to_supabase(filename)
                            
                            if section['name'] == "Stock_Holdings_Fund_PE_Ratio":
                                stock_holdings_file = filename
                    else:
                        print(f"Table not found for {section['name']}")
                        
                except Exception as e:
                    print(f"Error scraping {section['name']}: {e}")
            
            browser.close()
            
        return stock_holdings_file

    except Exception as e:
        print(f"Critical error in main loop: {e}")
        return None

def scrape_detailed_holdings(stock_holdings_file):
    if not stock_holdings_file or not os.path.exists(stock_holdings_file):
        print("Stock Holdings file not found. Skipping detailed scraping.")
        return

    print("Reading symbols for detailed scraping...")
    funds = []
    # Read with utf-8-sig to handle BOM if present
    with open(stock_holdings_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
             print(f"Debug: Headers in {stock_holdings_file}: {reader.fieldnames}")
        for row in reader:
             # Ensure we have Symbol and Name
            if 'Symbol' in row and 'Name' in row:
                funds.append({'Symbol': row['Symbol'], 'Name': row['Name']})
    
    print(f"Found {len(funds)} funds.")
    
    today_str = datetime.now().strftime("%d-%m-%Y")
    failed_funds = []
    
    start_time = time.time()
    
    from collections import deque
    import random
    from io import StringIO
    
    # Initialize queue with (fund_dict, attempt_count)
    queue = deque((fund, 0) for fund in funds)
    total_to_process = len(funds)
    processed_count = 0
    consecutive_failures = 0
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            while queue:
                # Check execution time (limit to 25 mins)
                if time.time() - start_time > 1500:
                    print("\n[WARNING] Time limit approaching (25 mins). Stopping detailed scraping gracefully.")
                    failed_funds.append("BATCH STOPPED: Time Limit Exceeded")
                    break

                fund, attempt = queue.popleft()
                symbol = fund['Symbol']
                name = fund['Name']
                
                print(f"[{processed_count + 1}/{total_to_process}] Processing {symbol} (Attempt {attempt + 1})...")

                url = f"https://nepsealpha.com/mutual-fund-navs/{symbol}?fsk=fs"
                success = False
                rate_limited = False
                
                try:
                    response = page.goto(url, wait_until="networkidle", timeout=30000)
                    
                    if response and response.status == 200:
                        # Wait a bit for any dynamic content
                        time.sleep(2)
                        html = page.content()
                        
                        # Check for Cloudflare challenge and wait for it to complete
                        cloudflare_failed = False
                        if "Just a moment" in html or "Checking your browser" in html:
                            print(f"  Cloudflare challenge detected for {symbol}. Waiting for auto-solve...")
                            try:
                                # Wait up to 15 seconds for a table to appear (real content)
                                page.wait_for_selector("table", timeout=15000)
                                time.sleep(2)  # Extra wait for full page load
                                html = page.content()
                                print(f"  Cloudflare challenge solved for {symbol}.")
                            except:
                                print(f"  Cloudflare challenge NOT solved for {symbol}.")
                                cloudflare_failed = True
                                rate_limited = True
                                consecutive_failures += 1
                        
                        # Parse tables if we have real content
                        if not cloudflare_failed:
                            dfs = pd.read_html(StringIO(html))
                            if dfs:
                                df = dfs[0]
                                safe_name = sanitize_filename(name)
                                filename = f"assets-{symbol}-{safe_name}-{today_str}.csv"
                                df.to_csv(filename, index=False, encoding='utf-8-sig')
                                print(f"Saved {filename}")
                                upload_to_supabase(filename)
                                success = True
                                processed_count += 1
                                consecutive_failures = 0
                            else:
                                print(f"  No tables found for {symbol}.")
                                consecutive_failures += 1
                    elif response and response.status == 403:
                        print(f"  Rate limited (403) for {symbol}.")
                        rate_limited = True
                        consecutive_failures += 1
                    else:
                        status = response.status if response else "No response"
                        print(f"  Failed {symbol}: HTTP {status}")
                        consecutive_failures += 1
                        
                except Exception as e:
                    print(f"  Error {symbol}: {e}")
                    consecutive_failures += 1

                if not success:
                    if attempt < 2:  # Allow up to 3 attempts
                        print(f"  -> Re-queueing {symbol} for retry later...")
                        queue.append((fund, attempt + 1))
                    else:
                        print(f"  -> Giving up on {symbol} after {attempt + 1} attempts.")
                        failed_funds.append(f"{symbol}: Failed after {attempt + 1} attempts")
                        processed_count += 1
                
                # Cooldown if multiple consecutive failures
                if consecutive_failures >= 3:
                    print(f"\n[COOLDOWN] {consecutive_failures} consecutive failures. Waiting 60s...")
                    time.sleep(60)
                    consecutive_failures = 0
                elif rate_limited:
                    cooldown = random.uniform(20, 40)
                    print(f"  Waiting {cooldown:.0f}s after rate limit...")
                    time.sleep(cooldown)
                
                # Random delay between requests
                delay = random.uniform(8, 15)
                time.sleep(delay)
            
            browser.close()
    
    except Exception as e:
        print(f"Critical error in detailed scraping: {e}")

    # Report failures
    print(f"\nScraping Summary: {len(funds) - len(failed_funds)} succeeded, {len(failed_funds)} failed.")
    if failed_funds:
        print("Writing failures to 'scraping_errors.log'...")
        with open('scraping_errors.log', 'w', encoding='utf-8-sig') as f:
            for fail in failed_funds:
                f.write(fail + '\n')
            f.write(f"\nTotal Funds Attempted: {len(funds)}\n")
        print("Check 'scraping_errors.log' for details.")

def scrape_debentures():
    url = "https://nepsealpha.com/debenture"
    print(f"Navigating to {url}...")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            page.goto(url)
            # Wait for table
            page.wait_for_selector("#DataTables_Table_0", timeout=60000)
            
            # Select 'Show 100 entries'
            try:
                page.select_option("select[name='DataTables_Table_0_length']", "100")
                time.sleep(2) # Wait for redraw
            except Exception as e:
                print(f"Could not select length: {e}")

            # Parse with BS4
            soup = BeautifulSoup(page.content(), 'html.parser')
            table = soup.find('table', {'id': 'DataTables_Table_0'})
            
            if table:
                headers = [th.text.strip() for th in table.find('thead').find_all('th')]
                rows = []
                tbody = table.find('tbody')
                if tbody:
                    for tr in tbody.find_all('tr'):
                        cells = [td.text.strip() for td in tr.find_all('td')]
                        # Basic validation
                        if len(cells) == len(headers):
                            rows.append(cells)
                
                if rows:
                    today_str = datetime.now().strftime("%d-%m-%Y")
                    filename = f"debenture-sastoshare-{today_str}.csv"
                    with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                        writer = csv.writer(f)
                        writer.writerow(headers)
                        writer.writerows(rows)
                    print(f"Saved {filename}")
                    upload_to_supabase(filename)
                else:
                    print("No rows found for Debentures.")
            else:
                print("Debenture table not found.")
            
            browser.close()
            
    except Exception as e:
        print(f"Error scraping debentures: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='NepseAlpha Mutual Fund Scraper')
    parser.add_argument('--task', type=str, default='all', choices=['daily', 'detailed', 'all'], help='Task to run: "daily", "detailed", or "all" (default)')
    args = parser.parse_args()

    print(f"Starting Scraper with task: {args.task}")

    if args.task == 'daily':
        scrape_main_sections()
        scrape_debentures()
    
    elif args.task == 'detailed':
        # Detailed scraping needs the stock list from main sections
        stock_csv = scrape_main_sections()
        if stock_csv:
            scrape_detailed_holdings(stock_csv)

    elif args.task == 'all':
        print("Running ALL scraping tasks...")
        stock_csv = scrape_main_sections()
        scrape_debentures()
        if stock_csv:
            scrape_detailed_holdings(stock_csv)
    
    print("All tasks completed.")
