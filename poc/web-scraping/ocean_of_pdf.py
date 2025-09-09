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
from selenium.common.exceptions import TimeoutException
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("oceanpdf_scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

def setup_webdriver():
    """Sets up Chrome webdriver with appropriate options."""
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--enable-unsafe-swiftshader")
    chrome_options.add_argument("--disable-software-rasterizer")
    prefs = {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "download.default_directory": os.path.abspath("temp_downloads"),
    }
    chrome_options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(30)
    return driver

def extract_books_from_page(driver, page_url):
    """Extracts book titles and URLs from the given page."""
    driver.get(page_url)
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    book_links = soup.select("h2.entry-title a.entry-title-link")
    books = [(link.text.strip(), link.get("href")) for link in book_links]
    return books

def download_pdf_with_selenium(driver, book_url):
    """Download the PDF from the book page using Selenium."""
    try:
        driver.get(book_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='image'][alt='Submit']"))
        )
        download_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='image'][alt='Submit']"))
        )
        download_button.click()
        time.sleep(5)  # Allow time for download
        
        # Check the page source or logs for a successful download trigger
        soup = BeautifulSoup(driver.page_source, "html.parser")
        filename_input = soup.find("input", {"name": "filename"})
        if filename_input:
            filename = filename_input.get("value")
            logger.info(f"Successfully initiated download for: {filename}")
            return filename
        else:
            logger.error("Download initiation failed: No filename found.")
            return None
    except TimeoutException as e:
        logger.error(f"Timeout while waiting for elements on page: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during PDF download: {e}")
        return None


def upload_to_s3(s3, bucket, key_name, content, content_type):
    """Upload content to S3."""
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key_name,
            Body=content,
            ContentType=content_type,
        )
        logger.info(f"Uploaded {key_name} to S3 successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {key_name} to S3: {e}")
        return False

def process_book(driver, s3, bucket, title, book_url):
    """Process a single book page."""
    logger.info(f"Processing book: {title}")
    safe_title = "".join(c for c in title if c.isalnum() or c in (" ", "-", "_")).rstrip()
    metadata = {
        "title": title,
        "url": book_url,
        "timestamp": datetime.now().isoformat(),
        "has_pdf": False,
    }

    pdf_filename = download_pdf_with_selenium(driver, book_url)
    if pdf_filename:
        # Upload metadata and placeholder PDF (dummy content)
        metadata["has_pdf"] = True
        pdf_key = f"ocean_pdf_books/{safe_title}/{pdf_filename}"
        upload_to_s3(s3, bucket, pdf_key, b"PDF CONTENT PLACEHOLDER", "application/pdf")
        metadata_key = f"ocean_pdf_books/{safe_title}/metadata.json"
        upload_to_s3(s3, bucket, metadata_key, json.dumps(metadata).encode("utf-8"), "application/json")
        return True
    return False

def scrape_ocean_pdf(driver, s3, bucket, base_url, max_pages):
    """Main function to scrape Ocean of PDF books."""
    books_processed = 0

    for page in range(1, max_pages + 1):
        page_url = f"{base_url}/page/{page}/?s=data" if page > 1 else base_url
        logger.info(f"Scraping page {page}: {page_url}")
        books = extract_books_from_page(driver, page_url)
        for title, book_url in books:
            if process_book(driver, s3, bucket, title, book_url):
                books_processed += 1

    logger.info(f"Scraping complete. Total books processed: {books_processed}")

def main():
    """Entry point of the script."""
    load_dotenv()
    required_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "AWS_BUCKET_NAME"]
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Missing required environment variable: {var}")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    base_url = "https://oceanofpdf.com/?s=data"

    driver = setup_webdriver()
    try:
        scrape_ocean_pdf(driver, s3, bucket_name, base_url, max_pages=3)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()