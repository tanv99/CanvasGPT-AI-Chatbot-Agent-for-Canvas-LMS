import logging
from typing import Dict, Any, BinaryIO
import os
from dotenv import load_dotenv
from pathlib import Path
import tempfile
import nest_asyncio

# Apply nest_asyncio to handle nested async operations
nest_asyncio.apply()

from llama_parse import LlamaParse

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class DocumentHandlerAgent:
    """Agent for handling uploaded files and extracting their content"""
    
    def __init__(self):
        self.supported_extensions = [
            '.pdf', '.docx', '.jpg', '.jpeg', 
            '.png', '.csv', '.xlsx'
        ]
        self.llamaparse_api_key = os.getenv('LLAMAPARSE_API_KEY')
        if not self.llamaparse_api_key:
            logger.warning("LLAMAPARSE_API_KEY not found in environment variables")
        else:
            self.parser = LlamaParse(
                api_key=self.llamaparse_api_key,
                result_type="markdown",
                parsing_instruction="Extract all text content and maintain document structure",
                show_progress=True,
            )
            
    def _format_extracted_content(self, parsed_content: list, file_type: str) -> Dict[str, Any]:
        """Format extracted content based on file type"""
        if isinstance(parsed_content, list) and len(parsed_content) > 0:
            # Extract text from Document object
            main_text = parsed_content[0].text
            metadata = parsed_content[0].metadata
            
            if file_type in ['.pdf', '.docx']:
                return {
                    "full_text": main_text,
                    "metadata": metadata,
                    "content_type": "document"
                }
                
            elif file_type in ['.jpg', '.jpeg', '.png']:
                return {
                    "text": main_text,
                    "metadata": metadata,
                    "content_type": "image"
                }
                
            elif file_type in ['.csv', '.xlsx']:
                return {
                    "text": main_text,
                    "metadata": metadata,
                    "content_type": "spreadsheet"
                }
                
        # Fallback case if parsed_content is not a list or empty
        return {"text": str(parsed_content), "content_type": "unknown"}

    async def extract_content_with_llamaparse(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """Extract content from file using LlamaParse"""
        try:
            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(filename).suffix) as tmp_file:
                tmp_file.write(file_content)
                tmp_path = tmp_file.name

            try:
                # Parse the document using async method
                parsed_content = await self.parser.aload_data(tmp_path)
                
                # Format the content based on file type
                file_type = Path(filename).suffix.lower()
                formatted_content = self._format_extracted_content(parsed_content, file_type)
                
                return {
                    "success": True,
                    "content": formatted_content,
                }
                
            finally:
                # Clean up temporary file
                os.unlink(tmp_path)
                
        except Exception as e:
            logger.error(f"Error in content extraction: {str(e)}")
            return {
                "success": False,
                "error": f"Content extraction error: {str(e)}",
                "content": None
            }

    async def process_file(self, file: BinaryIO, filename: str, extract_mode: bool = False) -> Dict[str, Any]:
        """
        Process uploaded file and optionally extract its content
        
        Args:
            file: The file object
            filename: Name of the file
            extract_mode: Whether to use LlamaParse for extraction
        """
        try:
            # Read the file content
            file_content = file.read()
            file_extension = Path(filename).suffix.lower()
            
            if file_extension not in self.supported_extensions:
                return {
                    "success": False,
                    "error": f"Unsupported file type: {file_extension}",
                    "content": None
                }
            
            # Only use LlamaParse if in extract mode
            if extract_mode and self.llamaparse_api_key:
                logger.info("Extracting content using LlamaParse")
                extraction_result = await self.extract_content_with_llamaparse(file_content, filename)
                if extraction_result["success"]:
                    formatted_content = extraction_result["content"]
                    return {
                        "success": True,
                        "content": formatted_content.get("full_text") or formatted_content.get("text", ""),
                        "metadata": formatted_content.get("metadata", {}),
                        "filename": filename,
                        "file_type": file_extension,
                        "extracted": True
                    }
                else:
                    logger.error(f"Content extraction failed: {extraction_result.get('error')}")
            
            # Return raw content for non-extract mode or if extraction fails
            logger.info("Returning raw file content without extraction")
            return {
                "success": True,
                "content": file_content,
                "filename": filename,
                "file_type": file_extension,
                "extracted": False
            }
                
        except Exception as e:
            logger.error(f"Error handling file: {str(e)}")
            return {
                "success": False,
                "error": f"Error handling file: {str(e)}",
                "content": None
            }

    async def close(self):
        """Cleanup method"""
        pass