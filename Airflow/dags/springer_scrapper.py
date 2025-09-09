import os
import time
from datetime import datetime, timedelta
import requests
import json
import logging
import urllib.parse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from contextlib import contextmanager
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
import subprocess
from selenium.webdriver.chrome.service import Service as ChromiumService
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import environment variables from env_var.py
from env_var import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    AWS_BUCKET_NAME
)

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_s3_client():
    """Create S3 client with retry configuration"""
    config = Config(
        connect_timeout=30,
        read_timeout=30,
        retries={'max_attempts': 3, 'mode': 'standard'}
    )
    
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=config
    )

def setup_webdriver():
    """Setup Chrome WebDriver with appropriate options"""
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('window-size=1920,1080')
    
    try:
        # service = Service(
        #     executable_path='/usr/bin/chromedriver',  # Use system-installed ChromeDriver
        #     log_path="/opt/airflow/logs/chromedriver.log"
        # )
        
        return webdriver.Chrome(
            # service=service,
            options=chrome_options
        )
    except Exception as e:
        logger.error(f"Failed to initialize webdriver: {str(e)}")
        raise

def download_pdf(pdf_url, headers):
    """Download PDF with retry logic"""
    for attempt in range(3):
        try:
            response = requests.get(pdf_url, headers=headers, timeout=30, stream=True)
            if response.status_code == 200:
                return response.content
            logger.error(f"Failed to download PDF. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading PDF (Attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
    return None

def scrape_metadata(**context):
    """Scrape book metadata from Springer"""
    driver = setup_webdriver()

    books = []
    
    try:
        base_url = "https://link.springer.com/search?facet-content-type=%22Book%22&package=openaccess&facet-language=%22En%22&facet-sub-discipline=%22Artificial+Intelligence%22&facet-discipline=%22Computer+Science%22"
        
        for page in range(1, 3):  # Scrape first 2 pages
            page_url = f"{base_url}&page={page}"
            logger.info(f"Processing page {page}")
            
            driver.get(page_url)
            time.sleep(2)
            
            try:
                book_elements = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.title"))
                )
                
                for element in book_elements:
                    title = element.text.strip()
                    href = element.get_attribute('href')
                    if title and href:
                        full_url = urllib.parse.urljoin("https://link.springer.com", href)
                        books.append({
                            'title': title,
                            'url': full_url,
                            'timestamp': datetime.now().isoformat()
                        })
                
                logger.info(f"Found {len(book_elements)} books on page {page}")
                time.sleep(1)
                
            except TimeoutException:
                logger.error(f"Timeout on page {page}")
                continue
                
    except Exception as e:
        logger.error(f"Scraping error: {e}")
    finally:
        if driver:
            driver.quit()
    
    # Push to XCom
    context['task_instance'].xcom_push(key='books', value=books)
    return f"Scraped {len(books)} books"

def process_books(**context):
    """Process books and download PDFs"""
    books = context['task_instance'].xcom_pull(key='books', task_ids='scrape_metadata')
    s3_client = create_s3_client()
    # driver = webdriver.Chrome()
    driver = setup_webdriver()
    processed_books = []
    
    try:
        for book in books:
            try:
                logger.info(f"Processing book: {book['title']}")
                safe_title = "".join(c for c in book['title'] if c.isalnum() or c in (' ', '-', '_')).rstrip()
                
                driver.get(book['url'])
                time.sleep(2)
                
                try:
                    pdf_link = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.c-pdf-download.u-clear-both a"))
                    )
                    pdf_url = pdf_link.get_attribute('href')
                    
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Referer': book['url']
                    }
                    
                    pdf_content = download_pdf(pdf_url, headers)
                    if pdf_content:
                        # Upload PDF
                        s3_client.put_object(
                            Bucket=AWS_BUCKET_NAME,
                            Key=f"springer_books/{safe_title}/book.pdf",
                            Body=pdf_content,
                            ContentType='application/pdf'
                        )
                        
                        # Get chapters if available
                        chapters = []
                        chapter_elements = driver.find_elements(By.CSS_SELECTOR, "li.c-card__section-item a")
                        for chapter in chapter_elements:
                            chapters.append({
                                'title': chapter.text.strip(),
                                'url': chapter.get_attribute('href')
                            })
                        
                        # Prepare metadata
                        book_metadata = {
                            'title': book['title'],
                            'url': book['url'],
                            'pdf_url': pdf_url,
                            'chapters': chapters,
                            'timestamp': datetime.now().isoformat(),
                            'has_pdf': True
                        }
                        
                        # Upload metadata
                        s3_client.put_object(
                            Bucket=AWS_BUCKET_NAME,
                            Key=f"springer_books/{safe_title}/metadata.json",
                            Body=json.dumps(book_metadata, indent=2),
                            ContentType='application/json'
                        )
                        
                        processed_books.append(book_metadata)
                        logger.info(f"Successfully processed: {book['title']}")
                        
                except TimeoutException:
                    logger.error(f"No PDF link found for: {book['title']}")
                    continue
                    
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing book {book['title']}: {e}")
                continue
                
    finally:
        driver.quit()
    
    # Push to XCom
    context['task_instance'].xcom_push(key='processed_books', value=processed_books)
    return f"Processed {len(processed_books)} books"

def generate_report(**context):
    """Generate final report"""
    books = context['task_instance'].xcom_pull(key='books', task_ids='scrape_metadata')
    processed_books = context['task_instance'].xcom_pull(key='processed_books', task_ids='process_books')
    
    report = {
        'total_books_scraped': len(books),
        'total_books_processed': len(processed_books),
        'success_rate': f"{(len(processed_books) / len(books) * 100):.2f}%" if books else "0%",
        'timestamp': datetime.now().isoformat()
    }
    
    s3_client = create_s3_client()
    
    try:
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=f"springer_books/reports/scrape_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            Body=json.dumps(report, indent=2),
            ContentType='application/json'
        )
    except ClientError as e:
        logger.error(f"Failed to upload report: {e}")
        
    return f"Report generated. Success rate: {report['success_rate']}"

# DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'books_scraper',
    default_args=default_args,
    description='Scrape Springer Books with PDFs',
    schedule_interval='@weekly',
    catchup=False
) as dag:
    
    scrape_task = PythonOperator(
        task_id='scrape_metadata',
        python_callable=scrape_metadata,
        provide_context=True
    )
    
    process_task = PythonOperator(
        task_id='process_books',
        python_callable=process_books,
        provide_context=True
    )
    
    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True
    )
    
    # Set task dependencies
    scrape_task >> process_task >> report_task