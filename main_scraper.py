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
        print(f"Uploading {file_name} to bucket '{bucket_name}'...")
        s3.upload_file(file_path, bucket_name, file_name)
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
                {"name": "Assets_Allocation", "tab_href": "#assetsAllocation", "table_id": "DataTables_Table_2", "select_name": "DataTables_Table_2_length"},
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
    with open(stock_holdings_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
             # Ensure we have Symbol and Name
            if 'Symbol' in row and 'Name' in row:
                funds.append({'Symbol': row['Symbol'], 'Name': row['Name']})
    
    print(f"Found {len(funds)} funds.")
    
    scraper = cloudscraper.create_scraper()
    today_str = datetime.now().strftime("%d-%m-%Y")
    failed_funds = []
    
    start_time = time.time()
    
    for idx, fund in enumerate(funds):
        # Check execution time (limit to 9 mins 30 sec to allow graceful exit)
        if time.time() - start_time > 570:
            print("\n[WARNING] Time limit approaching (9.5 mins). Stopping detailed scraping gracefully.")
            failed_funds.append("BATCH STOPPED: Time Limit Exceeded")
            break

        symbol = fund['Symbol']
        name = fund['Name']
        url = f"https://nepsealpha.com/mutual-fund-navs/{symbol}?fsk=fs"
        
        print(f"[{idx+1}/{len(funds)}] Process {symbol}...")
        try:
            resp = scraper.get(url, timeout=30)
            if resp.status_code == 200:
                from io import StringIO
                dfs = pd.read_html(StringIO(resp.text))
                if dfs:
                    df = dfs[0]
                    safe_name = sanitize_filename(name)
                    filename = f"assets-{symbol}-{safe_name}-{today_str}.csv"
                    df.to_csv(filename, index=False, encoding='utf-8-sig')
                    print(f"Saved {filename}")
                    upload_to_supabase(filename)
            else:
                print(f"Failed {symbol}: {resp.status_code}")
                failed_funds.append(f"{symbol}: HTTP {resp.status_code}")
            time.sleep(0.5) 
        except Exception as e:
             # Just log simple error to avoid clutter
            print(f"Error {symbol}: {e}")
            failed_funds.append(f"{symbol}: Error - {e}")

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
    print("Starting Main Scraper...")
    stock_csv = scrape_main_sections()
    scrape_debentures()
    if stock_csv:
        scrape_detailed_holdings(stock_csv)
    print("All tasks completed.")
