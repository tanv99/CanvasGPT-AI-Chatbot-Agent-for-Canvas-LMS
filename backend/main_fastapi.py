from fastapi import FastAPI, UploadFile, Form, File, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import os
from dotenv import load_dotenv
from agents.supervisor import CanvasGPTSupervisor
from pydantic import BaseModel
from io import BytesIO  

# Load environment variables
load_dotenv()

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic model for JSON payload
class QueryRequest(BaseModel):
    query: str

# Initialize supervisor with Canvas credentials
supervisor = CanvasGPTSupervisor(
    openai_api_key=os.getenv("OPENAI_API_KEY"),
    canvas_api_key=os.getenv("CANVAS_API_KEY"),
    canvas_base_url=os.getenv("CANVAS_BASE_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    s3_bucket_name=os.getenv("S3_BUCKET_NAME"),
    s3_books_folder=os.getenv("S3_BOOKS_FOLDER"),
    nvidia_api_key=os.getenv("NVIDIA_API_KEY"),
    nvidia_api_url=os.getenv("NVIDIA_API_URL"),
    pinecone_index_name=os.getenv("PINECONE_INDEX_NAME"),
    pinecone_api_key=os.getenv("PINECONE_API_KEY"),
)
@app.post("/agent-workflow")
async def process_message(
    request: QueryRequest = Body(...),  # For JSON payload
):
    try:
        # Process message through supervisor
        result = await supervisor.process_message(
            message=request.query,
            file_content=None
        )

        return result

    except Exception as e:
        return {"error": f"Error processing request: {str(e)}"}
@app.get("/test-pdf-listing")
async def test_pdf_listing():
    """Test S3 PDF listing configuration"""
    try:
        if not supervisor.pdf_listing_agent:
            return {
                "success": False,
                "error": "PDF listing agent not configured",
                "details": {
                    "aws_key_set": bool(os.getenv("AWS_ACCESS_KEY_ID")),
                    "aws_secret_set": bool(os.getenv("AWS_SECRET_ACCESS_KEY")),
                    "bucket_name_set": bool(os.getenv("S3_BUCKET_NAME"))
                }
            }
        
        result = await supervisor.pdf_listing_agent.list_pdfs()
        return {
            "success": True,
            "pdf_count": result["total_pdfs"],
            "bucket_name": supervisor.pdf_listing_agent.bucket_name
        }
        
    except Exception as e:
        logger.error(f"Error testing PDF listing: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


@app.post("/agent-workflow/form")
async def process_message_form(
    message: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None)
):
    try:
        # Handle file if present
        file_content = None
        if file:
            content = await file.read()
            # Create a file-like object from bytes
            file_io = BytesIO(content)
            file_io.name = file.filename  # Add filename attribute
            
            file_content = {
                "file": file_io,        # File-like object
                "filename": file.filename,
                "content_type": file.content_type
            }

        # Process message through supervisor
        result = await supervisor.process_message(
            message=message or "",
            file_content=file_content
        )

        return result

    except Exception as e:
        logger.error(f"Error in process_message_form: {str(e)}")
        return {"error": f"Error processing request: {str(e)}"}

@app.get("/supervisor-state")
async def get_supervisor_state():
    """Endpoint to check current supervisor state"""
    return await supervisor.get_state()

@app.post("/reset-supervisor")
async def reset_supervisor():
    """Endpoint to reset supervisor state"""
    await supervisor.reset_state()
    return {"status": "success", "message": "Supervisor state reset"}

@app.get("/courses")
async def list_courses():
    """Endpoint to list all available courses"""
    try:
        courses = await supervisor.get_available_courses()
        return {
            "success": True,
            "courses": courses
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@app.get("/canvas-test")
async def test_canvas_connection():
    """Test Canvas API connection and configuration"""
    try:
        if not supervisor.canvas_agent:
            return {
                "success": False,
                "error": "Canvas agent not configured",
                "details": {
                    "api_key_set": bool(os.getenv("CANVAS_API_KEY")),
                    "base_url_set": bool(os.getenv("CANVAS_BASE_URL"))
                }
            }
            
        # Test the connection
        await supervisor.canvas_agent._ensure_session()
        async with supervisor.canvas_agent.session.get(
            f"{supervisor.canvas_agent.base_url}/api/v1/courses",
            headers=supervisor.canvas_agent.headers
        ) as response:
            return {
                "success": True,
                "status": response.status,
                "headers": dict(response.headers),
                "api_base_url": supervisor.canvas_agent.base_url,
                "api_key_present": bool(supervisor.canvas_agent.api_key)
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }