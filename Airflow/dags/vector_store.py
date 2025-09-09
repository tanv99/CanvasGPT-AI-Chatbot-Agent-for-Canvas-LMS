from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import logging
from pathlib import Path
from env_var import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    AWS_BUCKET_NAME,
    PINECONE_API_KEY,
    PINECONE_ENVIRONMENT,
    NVIDIA_API_KEY
)
import boto3
from pinecone import Pinecone, PodSpec
from langchain.text_splitter import RecursiveCharacterTextSplitter
from tqdm import tqdm
import time
from openai import OpenAI
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NVIDIAEmbeddings:
    def __init__(self):
        self.embeddings_client = OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=NVIDIA_API_KEY
        )

    def embed_text(self, text, input_type='passage'):
        response = self.embeddings_client.embeddings.create(
            input=[text],
            model="nvidia/nv-embedqa-e5-v5",
            encoding_format="float",
            extra_body={
                "input_type": input_type,
                "truncate": "NONE"
            }
        )
        return response.data[0].embedding

def download_book_files_from_s3(**context):
    """Task to download first 3 book.txt files from S3 bucket output folders."""
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    local_dir = Path("/tmp/books")
    local_dir.mkdir(parents=True, exist_ok=True)
    
    # First, list book folders under springer_books/
    book_files = []
    
    try:
        # List contents under springer_books prefix
        prefix = "springer_books/"
        response = s3_client.list_objects_v2(
            Bucket=AWS_BUCKET_NAME,
            Prefix=prefix,
            Delimiter='/'
        )
        
        # Process only first 3 book folders
        count = 0
        for prefix_obj in response.get('CommonPrefixes', []):
            if count >= 3:  # Limit to first 3 books
                break
                
            book_folder = prefix_obj.get('Prefix')
            logger.info(f"Processing book folder: {book_folder}")
            
            # Check for book.txt in output folder
            output_path = f"{book_folder}output/book.txt"
            try:
                # Check if file exists
                s3_client.head_object(Bucket=AWS_BUCKET_NAME, Key=output_path)
                
                # Extract book name from folder path
                book_name = book_folder.rstrip('/').split('/')[-1]
                local_file_path = local_dir / f"{book_name}_book.txt"
                
                # Download file
                s3_client.download_file(
                    AWS_BUCKET_NAME,
                    output_path,
                    str(local_file_path)
                )
                
                book_files.append({
                    'local_path': str(local_file_path),
                    'book_name': book_name
                })
                logger.info(f"Successfully downloaded: {output_path} to {local_file_path}")
                count += 1
                
            except Exception as e:
                logger.warning(f"Could not download {output_path}: {str(e)}")
                continue
    
    except Exception as e:
        logger.error(f"Error listing S3 bucket contents: {str(e)}")
        raise
    
    if not book_files:
        raise ValueError("No book.txt files found in output folders")
    
    logger.info(f"Successfully downloaded {len(book_files)} books")
    context['task_instance'].xcom_push(key='downloaded_books', value=book_files)
    return book_files

# Process files function remains mostly the same, just updating the log message
def process_book_files(**context):
    """Task to split books into chunks with unique IDs."""
    downloaded_books = context['task_instance'].xcom_pull(task_ids='download_book_files_from_s3', key='downloaded_books')
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        separators=["\n\n", "\n", ".", " ", ""]
    )
    
    processed_chunks = []
    
    for book_info in downloaded_books:
        try:
            with open(book_info['local_path'], "r", encoding="utf-8") as f:
                text = f.read()
            
            chunks = text_splitter.split_text(text)
            book_chunks = []
            
            for i, chunk in enumerate(chunks[:50]):  # Process up to 50 chunks per book
                chunk_id = f"{book_info['book_name']}_id{i:04d}"
                book_chunks.append({
                    'chunk_id': chunk_id,
                    'text': chunk,
                    'source': book_info['book_name'],
                    'chunk_index': i
                })
            
            processed_chunks.extend(book_chunks)
            logger.info(f"Processed {len(book_chunks)} chunks from {book_info['book_name']}")
            
        except Exception as e:
            logger.error(f"Failed to process file {book_info['local_path']}: {e}")
    
    temp_chunks_file = "/tmp/processed_chunks.json"
    with open(temp_chunks_file, 'w') as f:
        json.dump(processed_chunks, f)
    
    context['task_instance'].xcom_push(key='processed_chunks_file', value=temp_chunks_file)
    return temp_chunks_file

# Generate embeddings function remains the same
def generate_embeddings(**context):
    chunks_file = context['task_instance'].xcom_pull(task_ids='process_book_files', key='processed_chunks_file')
    
    with open(chunks_file, 'r') as f:
        chunks = json.load(f)
    
    embeddings_client = NVIDIAEmbeddings()
    chunks_with_embeddings = []
    
    for chunk in chunks:
        try:
            embedding = embeddings_client.embed_text(chunk['text'])
            chunk['embedding'] = embedding
            chunks_with_embeddings.append(chunk)
            logger.info(f"Generated embedding for chunk {chunk['chunk_id']}")
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            logger.error(f"Failed to generate embedding for chunk {chunk['chunk_id']}: {e}")
    
    temp_embeddings_file = "/tmp/chunks_with_embeddings.json"
    with open(temp_embeddings_file, 'w') as f:
        json.dump(chunks_with_embeddings, f)
    
    context['task_instance'].xcom_push(key='embeddings_file', value=temp_embeddings_file)
    return temp_embeddings_file

def store_vectors(**context):
    """Task to store vectors in Pinecone."""
    embeddings_file = context['task_instance'].xcom_pull(task_ids='generate_embeddings', key='embeddings_file')
    
    with open(embeddings_file, 'r') as f:
        chunks_with_embeddings = json.load(f)
    
    try:
        # Initialize Pinecone with correct environment
        pc = Pinecone(api_key=PINECONE_API_KEY, environment='gcp-starter')
        index_name = "finalbigdata"  # Updated index name
        
        # Get index
        index = pc.Index(index_name)
        
        # Process in batches with exponential backoff
        batch_size = 5
        current_batch = []
        total_vectors = len(chunks_with_embeddings)
        vectors_uploaded = 0
        max_retries = 3
        base_delay = 2
        
        for chunk in chunks_with_embeddings:
            vector_data = (
                chunk['chunk_id'],
                chunk['embedding'],
                {
                    "text": chunk['text'][:1000],
                    "source": chunk['source'],
                    "chunk_index": chunk['chunk_index']
                }
            )
            current_batch.append(vector_data)
            
            if len(current_batch) >= batch_size:
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        index.upsert(vectors=current_batch)
                        vectors_uploaded += len(current_batch)
                        logger.info(f"Progress: {vectors_uploaded}/{total_vectors} vectors uploaded")
                        current_batch = []
                        time.sleep(1)
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count == max_retries:
                            logger.error(f"Failed to upload batch after {max_retries} retries")
                            for single_vector in current_batch:
                                try:
                                    index.upsert(vectors=[single_vector])
                                    vectors_uploaded += 1
                                    time.sleep(1)
                                except Exception as single_e:
                                    logger.error(f"Failed to upload vector {single_vector[0]}: {str(single_e)}")
                        else:
                            wait_time = base_delay * (2 ** retry_count)
                            logger.warning(f"Retry {retry_count + 1}/{max_retries} after {wait_time}s")
                            time.sleep(wait_time)
                current_batch = []
        
        # Upload remaining vectors
        if current_batch:
            retry_count = 0
            while retry_count < max_retries:
                try:
                    index.upsert(vectors=current_batch)
                    vectors_uploaded += len(current_batch)
                    logger.info(f"Final batch uploaded. Total vectors: {vectors_uploaded}")
                    break
                except Exception as e:
                    retry_count += 1
                    if retry_count == max_retries:
                        logger.error("Failed to upload final batch, trying one by one")
                        for single_vector in current_batch:
                            try:
                                index.upsert(vectors=[single_vector])
                                vectors_uploaded += 1
                                time.sleep(1)
                            except Exception as single_e:
                                logger.error(f"Failed to upload vector {single_vector[0]}: {str(single_e)}")
                    else:
                        wait_time = base_delay * (2 ** retry_count)
                        logger.warning(f"Retry {retry_count + 1}/{max_retries} after {wait_time}s")
                        time.sleep(wait_time)
        
        logger.info(f"Vector upload completed. Total vectors uploaded: {vectors_uploaded}")
        
        # Verify final state
        try:
            stats = index.describe_index_stats()
            logger.info(f"Final index stats: {stats}")
            return f"Successfully uploaded {vectors_uploaded} vectors to Pinecone"
        except Exception as e:
            logger.error(f"Failed to get final index stats: {str(e)}")
            return f"Upload completed with {vectors_uploaded} vectors, but failed to verify final state"
            
    except Exception as e:
        logger.error(f"Fatal error in store_vectors: {str(e)}")
        raise e

# DAG definition
default_args = {
    "owner": "tanvi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "s3_to_pinecone_book_processing",
    default_args=default_args,
    description="Process first 3 books from S3 and store in Pinecone",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 15),
    catchup=False,
)

# Define tasks
download_task = PythonOperator(
    task_id="download_book_files_from_s3",
    python_callable=download_book_files_from_s3,
    provide_context=True,
    dag=dag,
)

process_text_task = PythonOperator(
    task_id="process_book_files",
    python_callable=process_book_files,
    provide_context=True,
    dag=dag,
)

generate_embeddings_task = PythonOperator(
    task_id="generate_embeddings",
    python_callable=generate_embeddings,
    provide_context=True,
    dag=dag,
)

store_vectors_task = PythonOperator(
    task_id="store_vectors",
    python_callable=store_vectors,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
download_task >> process_text_task >> generate_embeddings_task >> store_vectors_task