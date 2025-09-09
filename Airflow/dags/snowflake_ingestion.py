import os
import logging
import json
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional
from dataclasses import dataclass
import time

import snowflake.connector
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

# Import environment variables
from env_var import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    AWS_BUCKET_NAME,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_ROLE
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('book_loader.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class SnowflakeConfig:
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    schema: str
    role: str

@dataclass
class AWSConfig:
    access_key_id: str
    secret_access_key: str
    region: str
    bucket: str

def setup_s3_client(aws_config: AWSConfig) -> boto3.client:
    """Set up S3 client with retry logic."""
    config = Config(
        retries={'max_attempts': 3, 'mode': 'adaptive'},
        connect_timeout=5,
        read_timeout=10
    )

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_config.access_key_id,
            aws_secret_access_key=aws_config.secret_access_key,
            region_name=aws_config.region,
            config=config
        )
        
        # Test connection
        s3_client.list_buckets()
        logger.info(f"Successfully connected to S3 bucket: {aws_config.bucket}")
        return s3_client

    except Exception as e:
        logger.error(f"Error setting up S3 client: {e}")
        raise
@task
def process_s3_metadata():
    """Task to process metadata and files from S3 with correct folder paths."""
    aws_config = AWSConfig(
        access_key_id=AWS_ACCESS_KEY_ID,
        secret_access_key=AWS_SECRET_ACCESS_KEY,
        region=AWS_REGION,
        bucket=AWS_BUCKET_NAME
    )
    
    s3_client = setup_s3_client(aws_config)
    processed_data = []
    
    try:
        # List all folders in springer_books directory
        response = s3_client.list_objects_v2(
            Bucket=aws_config.bucket,
            Prefix='springer_books/',
            Delimiter='/'
        )
        
        book_folders = [
            prefix.get('Prefix') 
            for prefix in response.get('CommonPrefixes', [])
            if not prefix.get('Prefix').endswith('reports/')  # Skip reports folder
        ]
        
        logger.info(f"Found {len(book_folders)} book folders")
        
        for folder_path in book_folders:
            try:
                book_name = folder_path.rstrip('/').split('/')[-1]
                logger.info(f"Processing book: {book_name}")
                
                # Get metadata.json from book folder
                try:
                    metadata_path = f"{folder_path}metadata.json"
                    logger.info(f"Looking for metadata at: {metadata_path}")
                    metadata_obj = s3_client.get_object(
                        Bucket=aws_config.bucket,
                        Key=metadata_path
                    )
                    metadata = json.loads(metadata_obj['Body'].read().decode('utf-8'))
                    title = metadata.get('title', book_name)
                    url = metadata.get('url', '')
                    logger.info(f"Found metadata for {book_name}")
                except Exception as e:
                    logger.warning(f"Could not read metadata for {book_name}: {str(e)}")
                    continue

                # Get book.pdf from book folder
                pdf_url = None
                try:
                    pdf_key = f"{folder_path}book.pdf"
                    logger.info(f"Looking for PDF at: {pdf_key}")
                    s3_client.head_object(
                        Bucket=aws_config.bucket,
                        Key=pdf_key
                    )
                    pdf_url = f"https://{aws_config.bucket}.s3.amazonaws.com/{pdf_key}"
                    logger.info(f"Found PDF file for {book_name}")
                except Exception as e:
                    logger.warning(f"No PDF file found for {book_name}: {str(e)}")

                # Get book.txt from output folder
                txt_url = None
                try:
                    txt_key = f"{folder_path}output/book.txt"
                    logger.info(f"Looking for TXT at: {txt_key}")
                    s3_client.head_object(
                        Bucket=aws_config.bucket,
                        Key=txt_key
                    )
                    txt_url = f"https://{aws_config.bucket}.s3.amazonaws.com/{txt_key}"
                    logger.info(f"Found TXT file for {book_name}")
                except Exception as e:
                    logger.warning(f"No TXT file found for {book_name}: {str(e)}")

                # Get all images from output/images folder
                image_urls = []
                try:
                    images_prefix = f"{folder_path}output/images/"
                    logger.info(f"Looking for images in: {images_prefix}")
                    paginator = s3_client.get_paginator('list_objects_v2')
                    
                    for page in paginator.paginate(
                        Bucket=aws_config.bucket,
                        Prefix=images_prefix
                    ):
                        for obj in page.get('Contents', []):
                            key = obj['Key']
                            if any(key.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png']):
                                image_url = f"https://{aws_config.bucket}.s3.amazonaws.com/{key}"
                                image_urls.append(image_url)
                                logger.info(f"Found image: {key}")
                    
                    if image_urls:
                        logger.info(f"Found {len(image_urls)} images for {book_name}")
                    else:
                        logger.warning(f"No images found for {book_name}")
                        
                except Exception as e:
                    logger.warning(f"Error getting images for {book_name}: {str(e)}")

                # Process book data
                if metadata or pdf_url or txt_url or image_urls:
                    book_data = {
                        'TITLE': title,
                        'PDF_URL': pdf_url if pdf_url else '',
                        'TXT_URL': txt_url if txt_url else '',
                        'IMAGE_URLS': sorted(image_urls),
                        'URL': url,
                        'S3_BUCKET': aws_config.bucket
                    }
                    processed_data.append(book_data)
                    logger.info(f"Successfully processed {book_name} with {len(image_urls)} images")
                
            except Exception as e:
                logger.error(f"Error processing book folder {folder_path}: {str(e)}")
                continue
        
        logger.info(f"Total books processed: {len(processed_data)}")
        return processed_data
        
    except Exception as e:
        logger.error(f"Error in process_s3_metadata: {str(e)}")
        raise AirflowException(f"Failed to process S3 metadata: {str(e)}")
    
@task
def create_snowflake_table():
    """Task to create Snowflake table with schema for individual image URLs."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        
        with conn.cursor() as cursor:
            # Resume warehouse
            cursor.execute(f"ALTER WAREHOUSE {SNOWFLAKE_WAREHOUSE} RESUME IF SUSPENDED")
            cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            
            # Drop existing table
            cursor.execute("""
            DROP TABLE IF EXISTS BOOK_DATA
            """)
            
            # Create new table with single IMAGE_URL columns
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS BOOK_DATA (
                ID INTEGER AUTOINCREMENT START 1 INCREMENT 1,
                TITLE VARCHAR(1000),
                PDF_URL VARCHAR(1000),
                TXT_URL VARCHAR(1000),
                IMAGE_URL VARCHAR(1000),  -- Changed from ARRAY to single URL
                URL VARCHAR(1000),
                S3_BUCKET VARCHAR(100),
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (ID)
            )
            """)
            
            conn.commit()
            logger.info("Successfully created Snowflake table")
            return True
            
    except Exception as e:
        logger.error(f"Error creating Snowflake table: {str(e)}")
        raise AirflowException(f"Failed to create Snowflake table: {str(e)}")
    finally:
        if conn:
            conn.close()

@task
def load_to_snowflake(processed_data: list):
    """Task to load processed book data into Snowflake with individual rows for images."""
    if not processed_data:
        logger.warning("No data received for loading into Snowflake")
        return {"records_loaded": 0}
        
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        
        with conn.cursor() as cursor:
            # Set up context
            cursor.execute(f"ALTER WAREHOUSE {SNOWFLAKE_WAREHOUSE} RESUME IF SUSPENDED")
            cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            
            successful_count = 0
            
            for book_data in processed_data:
                try:
                    # Create one row for each image URL
                    for image_url in book_data['IMAGE_URLS']:
                        insert_sql = """
                        INSERT INTO BOOK_DATA (
                            TITLE, 
                            PDF_URL, 
                            TXT_URL, 
                            IMAGE_URL,  -- Single URL
                            URL, 
                            S3_BUCKET
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor.execute(insert_sql, (
                            book_data['TITLE'],
                            book_data['PDF_URL'],
                            book_data['TXT_URL'],
                            image_url,  # Individual image URL
                            book_data['URL'],
                            book_data['S3_BUCKET']
                        ))
                        successful_count += 1
                        
                    logger.info(f"Inserted {len(book_data['IMAGE_URLS'])} records for book: {book_data['TITLE']}")
                    
                except Exception as e:
                    logger.error(f"Error inserting book {book_data['TITLE']}: {str(e)}")
                    continue
            
            conn.commit()
            
            # Verify data
            cursor.execute("SELECT COUNT(*) FROM BOOK_DATA")
            total_count = cursor.fetchone()[0]
            
            # Log results
            logger.info(f"Successfully loaded {successful_count} image records to Snowflake")
            logger.info(f"Total records in table: {total_count}")
            
            return {
                "records_loaded": successful_count,
                "total_records": total_count
            }
            
    except Exception as e:
        logger.error(f"Error in load_to_snowflake: {str(e)}")
        raise AirflowException(f"Failed to load data to Snowflake: {str(e)}")
    finally:
        if conn:
            conn.close()


# DAG definition
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': True
}

with DAG(
    'book_data_to_snowflake',
    default_args=default_args,
    description='Load Springer book data from S3 to Snowflake',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['snowflake', 's3', 'books']
) as dag:
    
    create_table_task = create_snowflake_table()
    process_metadata_task = process_s3_metadata()
    load_data_task = load_to_snowflake(process_metadata_task)
    
    create_table_task >> process_metadata_task >> load_data_task