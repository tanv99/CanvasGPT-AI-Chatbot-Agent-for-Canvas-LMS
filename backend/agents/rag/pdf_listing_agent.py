import boto3
from botocore.exceptions import ClientError
import logging
from typing import List, Dict
from pydantic import BaseModel
from openai import OpenAI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BookFolder(BaseModel):
    """Model for book folder metadata"""
    name: str
    path: str
    last_modified: str

class PDFListingAgent:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, bucket_name: str, 
                 books_folder: str, region_name: str = 'us-east-1', openai_api_key: str = None):
        self.bucket_name = bucket_name
        self.books_folder = books_folder
        self.client = OpenAI(api_key=openai_api_key) if openai_api_key else None
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
            logger.info(f"Successfully initialized S3 client for bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise

    def _extract_folder_names(self, objects: List[Dict]) -> List[str]:
        """Extract unique folder names from object list"""
        folders = set()
        for obj in objects:
            key = obj['Key']
            if key.startswith(f'{self.books_folder}/'):
                parts = key[len(f'{self.books_folder}/'):].split('/')
                if parts[0]:
                    folders.add(parts[0])
        return sorted(list(folders))

    async def format_response(self, folders: List[BookFolder]) -> str:
        """Format folder information using GPT for better chatbot display"""
        if not folders:
            return "No PDF folders found."

        if not self.client:
            # Fallback formatting if no OpenAI client
            folder_info = []
            for folder in folders:
                folder_info.append(f"📁 {folder.name}\n   Path: {folder.path}\n   Last Modified: {folder.last_modified}")
            return f"Found {len(folders)} PDF folders:\n\n" + "\n\n".join(folder_info)

        try:
            folder_info = []
            for folder in folders:
                folder_info.append(f"Name: {folder.name}\nPath: {folder.path}\nLast Modified: {folder.last_modified}")
            
            folders_text = "\n\n".join(folder_info)
            
            messages = [
                {"role": "system", "content": """You are a file system display assistant. Format the folder information 
                in a clear, structured way suitable for chatbot display. Use emojis and markdown formatting for better readability."""},
                {"role": "user", "content": f"""
Please format the following folder information in a clear, structured way:

Total Folders: {len(folders)}

Folder Details:
{folders_text}

Format it with:
1. A clear header showing total count
2. Each folder clearly separated
3. Use emojis (📁 for folders, 🕒 for time)
4. Use markdown formatting for structure
"""}
            ]

            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=messages,
                temperature=0.7
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"Error formatting response: {e}")
            # Fallback to basic formatting
            return f"Found {len(folders)} PDF folders:\n\n" + "\n\n".join(folder_info)

    async def list_book_folders(self) -> Dict[str, any]:
        """List all book folders in the S3 bucket using pagination"""
        try:
            all_contents = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            # Use pagination to get all objects
            for page in paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=f'{self.books_folder}/'
            ):
                if 'Contents' in page:
                    all_contents.extend(page['Contents'])

            if not all_contents:
                return {
                    "success": True,
                    "formatted_output": "# No PDF folders found",
                    "total_folders": 0
                }
            
            # Extract and organize folders
            folder_info = {}
            for obj in all_contents:
                key = obj['Key']
                if key.startswith(f'{self.books_folder}/'):
                    parts = key[len(f'{self.books_folder}/'):].split('/')
                    if parts[0]:  # Ensure it's not empty
                        folder_name = parts[0]
                        if folder_name not in folder_info:
                            folder_info[folder_name] = {
                                'last_modified': obj['LastModified']
                            }
                        else:
                            # Update last modified if this file is newer
                            if obj['LastModified'] > folder_info[folder_name]['last_modified']:
                                folder_info[folder_name]['last_modified'] = obj['LastModified']

            # Format the output
            output_lines = ["# Available PDF Folders\n"]
            output_lines.append("## Uncategorized\n")
            
            # Sort folders alphabetically
            sorted_folders = sorted(folder_info.items(), key=lambda x: x[0].lower())
            
            for folder_name, info in sorted_folders:
                # Add folder name
                output_lines.append(f"- {folder_name}")
                # Add last modified date with proper indentation
                last_modified = info['last_modified'].strftime('%Y-%m-%d %H:%M:%S')
                output_lines.append(f"  - Last modified: {last_modified}\n")
            
            output_lines.append(f"\nTotal Folders: {len(folder_info)}")
            
            formatted_output = "\n".join(output_lines)
            
            return {
                "success": True,
                "formatted_output": formatted_output,
                "total_folders": len(folder_info)
            }
            
        except ClientError as e:
            error_message = f"Error listing book folders: {str(e)}"
            logger.error(error_message)
            return {
                "success": False,
                "error": error_message,
                "formatted_output": "# Error retrieving PDF folders",
                "total_folders": 0
            }

    async def close(self):
        """Cleanup method"""
        if hasattr(self, 's3_client'):
            self.s3_client.close()
        logger.info("PDF listing agent closed")