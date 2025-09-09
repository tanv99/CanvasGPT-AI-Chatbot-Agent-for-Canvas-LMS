import boto3
import requests
import time
import os
import sys
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import logging
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from datetime import datetime
import urllib.parse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('springer_scraper.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def setup_webdriver():
    """Sets up Chrome webdriver with appropriate options."""
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")

    # Add preferences for handling PDFs
    prefs = {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "download.default_directory": os.path.abspath("temp_downloads")
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(30)
    return driver

def download_pdf(pdf_url, headers):
    """Download PDF using requests."""
    for attempt in range(3):  # Retry logic
        try:
            response = requests.get(pdf_url, headers=headers, timeout=30, stream=True)
            if response.status_code == 200:
                return response.content
            logger.error(f"Failed to download PDF. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading PDF (Attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
    return None

def process_book(driver, s3, bucket, title, book_url):
    """Process a single book."""
    try:
        logger.info(f"Processing book: {title}")
        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
        metadata = {
            'title': title,
            'url': book_url,
            'timestamp': datetime.now().isoformat(),
            'has_pdf': False
        }

        driver.get(book_url)
        time.sleep(2)

        try:
            pdf_link = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.c-pdf-download.u-clear-both a"))
            )
            pdf_path = pdf_link.get_attribute('href')
            pdf_url = urllib.parse.urljoin("https://link.springer.com", pdf_path)

            logger.info(f"Attempting to download PDF: {pdf_url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': book_url
            }

            pdf_content = download_pdf(pdf_url, headers)
            if pdf_content:
                if upload_to_s3(s3, bucket, f"springer_books/{safe_title}/book.pdf", pdf_content, 'application/pdf'):
                    metadata['has_pdf'] = True
        except TimeoutException:
            logger.error(f"No PDF download link found for {title}")

        # Upload metadata to S3
        upload_to_s3(s3, bucket, f"springer_books/{safe_title}/metadata.json",
                     json.dumps(metadata, indent=2).encode('utf-8'), 'application/json')
        return True
    except Exception as e:
        logger.error(f"Error processing book {title}: {e}")
        return False

def upload_to_s3(s3, bucket, key_name, content, content_type):
    """Upload content to S3 with error handling."""
    for attempt in range(3):
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key_name,
                Body=content,
                ContentType=content_type
            )
            logger.info(f"Successfully uploaded {key_name} to S3")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {key_name} (Attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
    return False

def scrape_springer_books(s3, bucket):
    """Main function to scrape Springer books across multiple pages."""
    base_url = "https://link.springer.com/search?facet-content-type=%22Book%22&package=openaccess&facet-language=%22En%22&facet-sub-discipline=%22Artificial+Intelligence%22&facet-discipline=%22Computer+Science%22"
    driver = setup_webdriver()
    books_processed = 0

    try:
        for page in range(1, 3):  # Scrape the first 5 pages
            page_url = f"{base_url}&page={page}"
            logger.info(f"Accessing page {page}: {page_url}")
            
            driver.get(page_url)
            time.sleep(5)

            # Find book elements on the current page
            try:
                book_elements = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.title"))
                )
                logger.info(f"Found {len(book_elements)} books on page {page}")

                books = []
                for element in book_elements:
                    title = element.text.strip()
                    href = element.get_attribute('href')
                    if title and href:
                        full_url = urllib.parse.urljoin("https://link.springer.com", href)
                        books.append({'title': title, 'url': full_url})

                # Process each book
                for book in books:
                    if process_book(driver, s3, bucket, book['title'], book['url']):
                        books_processed += 1
                        time.sleep(1)
            except TimeoutException:
                logger.error(f"No books found on page {page}")
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
    finally:
        driver.quit()
        logger.info(f"Scraping complete. Total books processed: {books_processed}")

def main():
    """Entry point of the script."""
    load_dotenv()
    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION', 'AWS_BUCKET_NAME']
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Missing required environment variable: {var}")

    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )
    bucket_name = os.getenv('AWS_BUCKET_NAME')

    scrape_springer_books(s3, bucket_name)

if __name__ == "__main__":
    main()
