from dotenv import load_dotenv
import os
from pathlib import Path
import logging
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import tempfile
import fitz  # PyMuPDF
import tabula
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import json

# Load environment variables from .env file
load_dotenv()

# Get environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PDFProcessor:
    def __init__(self):
        config = Config(retries=dict(max_attempts=3))
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
            config=config
        )
        self.bucket_name = AWS_BUCKET_NAME

    def format_table(self, df):
        """Format DataFrame as inline text with delimiters"""
        if df.empty:
            return ""
        
        # Convert DataFrame to string with minimal formatting
        table_str = "\n[TABLE_START]\n"
        headers = " | ".join(str(col) for col in df.columns)
        table_str += headers + "\n"
        table_str += "-" * len(headers) + "\n"
        
        for _, row in df.iterrows():
            row_str = " | ".join(str(val) for val in row.values)
            table_str += row_str + "\n"
        
        table_str += "[TABLE_END]\n"
        return table_str

    def extract_tables_from_page(self, pdf_path, page_number):
        """Extract tables from a specific page"""
        try:
            # Extract tables from the page
            tables = tabula.read_pdf(
                pdf_path,
                pages=page_number + 1,  # tabula uses 1-based page numbers
                multiple_tables=True,
                guess=True,
                silent=True,
                lattice=True,  # for tables with lines
                stream=True    # for tables without lines
            )
            
            return tables
        except Exception as e:
            logger.error(f"Error extracting tables from page {page_number}: {str(e)}")
            return []

    def process_pdf(self, s3_input_key):
        try:
            logger.info(f"Processing file: {s3_input_key}")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir_path = Path(temp_dir)
                
                # Get path structure and filename
                path_parts = Path(s3_input_key).parts
                book_folder = path_parts[-2]
                original_filename = path_parts[-1]
                filename_stem = Path(original_filename).stem
                
                # Download PDF
                input_file = temp_dir_path / original_filename
                self.download_from_s3(s3_input_key, str(input_file))
                
                # Setup directories
                output_base_dir = temp_dir_path / book_folder / "output"
                output_dir = output_base_dir
                images_dir = output_base_dir / "images"
                output_dir.mkdir(parents=True, exist_ok=True)
                images_dir.mkdir(parents=True, exist_ok=True)
                
                # Create output files
                text_file_path = output_dir / f"{filename_stem}.txt"
                image_list_path = output_dir / f"{filename_stem}_image_list.txt"
                
                # Open PDF
                pdf_document = fitz.open(str(input_file))
                image_counter = 0
                table_counter = 0
                
                with open(text_file_path, 'w', encoding='utf-8') as text_file, \
                     open(image_list_path, 'w', encoding='utf-8') as image_list_file:
                    
                    # Write book title
                    text_file.write(f"{filename_stem}\n{'='*len(filename_stem)}\n\n")
                    
                    for page_num in range(len(pdf_document)):
                        page = pdf_document[page_num]
                        text_file.write(f"\nPage {page_num + 1}\n{'-'*10}\n")
                        
                        # Extract text
                        text = page.get_text()
                        if text.strip():
                            text_file.write(text.strip() + "\n")
                        
                        # Extract and format tables
                        tables = self.extract_tables_from_page(str(input_file), page_num)
                        for table in tables:
                            if not isinstance(table, pd.DataFrame) or table.empty:
                                continue
                            table_counter += 1
                            table_text = self.format_table(table)
                            text_file.write(f"\nTable {table_counter}:{table_text}")
                        
                        # Extract and save images
                        images = page.get_images()
                        for img_index, img in enumerate(images):
                            try:
                                image_counter += 1
                                xref = img[0]
                                base_image = pdf_document.extract_image(xref)
                                image_bytes = base_image["image"]
                                image_ext = base_image["ext"]
                                
                                image_filename = f"{filename_stem}_image{image_counter}.{image_ext}"
                                image_path = images_dir / image_filename
                                
                                # Save image locally
                                with open(image_path, "wb") as image_file:
                                    image_file.write(image_bytes)
                                
                                # Upload to S3
                                s3_image_key = f"{book_folder}/output/images/{image_filename}"
                                self.upload_to_s3(str(image_path), s3_image_key)
                                
                                # Log image in list and main text
                                image_list_file.write(f"Page {page_num + 1}: {s3_image_key}\n")
                                text_file.write(f"\n[Image {image_counter}: {image_filename}]\n")
                                
                                # Clean up local image
                                image_path.unlink()
                                
                            except Exception as e:
                                logger.error(f"Failed to process image {image_counter}: {str(e)}")
                                continue
                        
                        text_file.write("\n" + "-"*40 + "\n")  # Page separator
                    
                    pdf_document.close()
                
                # Upload files to S3
                text_s3_key = f"{book_folder}/output/{filename_stem}.txt"
                image_list_s3_key = f"{book_folder}/output/{filename_stem}_image_list.txt"
                
                self.upload_to_s3(str(text_file_path), text_s3_key)
                self.upload_to_s3(str(image_list_path), image_list_s3_key)
                
                logger.info(f"Successfully processed {original_filename}")
                return {
                    'text_file': text_s3_key,
                    'image_list': image_list_s3_key,
                    'image_count': image_counter,
                    'table_count': table_counter
                }
                
        except Exception as e:
            logger.error(f"Error processing document {s3_input_key}: {str(e)}")
            logger.exception("Full error traceback:")
            raise

    def download_from_s3(self, s3_key, local_path):
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Downloaded {s3_key} from S3")
        except ClientError as e:
            logger.error(f"Error downloading from S3: {str(e)}")
            raise

    def upload_to_s3(self, local_path, s3_key):
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            logger.info(f"Uploaded {local_path} to S3 as {s3_key}")
        except ClientError as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            raise

    def list_pdf_files(self):
        try:
            all_pdf_files = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix='springer_books/'
            )
            
            for page in pages:
                for obj in page.get('Contents', []):
                    if obj['Key'].lower().endswith('.pdf'):
                        all_pdf_files.append(obj['Key'])
            
            if not all_pdf_files:
                logger.warning("No PDF files found in springer_books/")
            else:
                all_pdf_files.sort()
                all_pdf_files = all_pdf_files[:5]
            
            return all_pdf_files
                
        except ClientError as e:
            logger.error(f"Error listing PDF files: {str(e)}")
            raise

def main():
    try:
        processor = PDFProcessor()
        
        # List PDFs
        pdf_files = processor.list_pdf_files()
        if not pdf_files:
            logger.error("No PDF files found")
            return
        
        logger.info(f"Found {len(pdf_files)} PDF files to process")
        
        # Process each PDF
        results = []
        for pdf_file in pdf_files:
            try:
                result = processor.process_pdf(pdf_file)
                results.append({
                    'input_file': pdf_file,
                    'output_files': result,
                    'status': 'success'
                })
            except Exception as e:
                results.append({
                    'input_file': pdf_file,
                    'error': str(e),
                    'status': 'failed'
                })
        
        # Save results
        with open('processing_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        # Print summary
        success_count = sum(1 for r in results if r['status'] == 'success')
        logger.info(f"\nProcessing Summary:")
        logger.info(f"Successfully processed: {success_count} files")
        logger.info(f"Failed to process: {len(results) - success_count} files")
        
        for result in results:
            if result['status'] == 'success':
                logger.info(f"✓ Processed {result['input_file']}")
                logger.info(f"  - Text file: {result['output_files']['text_file']}")
                logger.info(f"  - Images found: {result['output_files']['image_count']}")
                logger.info(f"  - Tables found: {result['output_files']['table_count']}")
            else:
                logger.error(f"✗ Failed to process {result['input_file']}: {result['error']}")
                
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()