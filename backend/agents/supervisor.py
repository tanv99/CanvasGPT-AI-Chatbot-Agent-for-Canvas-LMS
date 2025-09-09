from typing import Dict, Any, Optional, List, Union 
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
import logging
from .web_agent import WebSearchAgent
from .canvas.post_agent import CanvasPostAgent
import re
from datetime import datetime, timezone, timedelta  
import json  
from .document_handler import DocumentHandlerAgent
from .rag.pdf_listing_agent import PDFListingAgent
from .rag.rag_agent import RAGQueryAgent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Message(BaseModel):
    """Message model for communication between agents"""
    content: str
    type: str = "text"
    role: str  # 'user' or 'assistant'
    metadata: Dict[str, Any] = Field(default_factory=dict)

class SupervisorState(BaseModel):
    """State management for the supervisor"""
    messages: List[Message] = Field(default_factory=list)
    context: Dict[str, Any] = Field(default_factory=dict)
    current_agent: Optional[str] = None

class CanvasGPTSupervisor:
    """Main supervisor class for orchestrating agent interactions"""
    
    def __init__(self, openai_api_key: str, canvas_api_key: str = None, canvas_base_url: str = None,
             aws_access_key_id: str = None, aws_secret_access_key: str = None, 
                s3_bucket_name: str = None, s3_books_folder: str = None,
                nvidia_api_key: str = None, nvidia_api_url: str = None,
                pinecone_api_key: str = None,pinecone_index_name: str = None):
        
        self.llm = ChatOpenAI(api_key=openai_api_key)
        self.web_agent = WebSearchAgent()
        self.canvas_agent = CanvasPostAgent(canvas_api_key, canvas_base_url) if canvas_api_key else None
        self.state = SupervisorState()
        self.document_handler = DocumentHandlerAgent()
        
        # Initialize PDF listing agent
        if aws_access_key_id and aws_secret_access_key and s3_bucket_name and s3_books_folder:
            try:
                self.pdf_listing_agent = PDFListingAgent(
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    bucket_name=s3_bucket_name,
                    books_folder=s3_books_folder
                )
                logger.info("PDF listing agent initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize PDF listing agent: {str(e)}")
                self.pdf_listing_agent = None
        else:
            self.pdf_listing_agent = None
            logger.warning("PDF listing agent not initialized - missing AWS credentials")

        # Initialize RAG query agent with NVIDIA credentials
        try:
            if all([nvidia_api_key, nvidia_api_url, pinecone_api_key, pinecone_index_name,openai_api_key]):
                self.rag_agent = RAGQueryAgent(
                    api_key=nvidia_api_key,
                    api_url=nvidia_api_url,
                    pinecone_api_key=pinecone_api_key,
                    pinecone_index_name=pinecone_index_name,
                    openai_api_key=openai_api_key
                )
                logger.info("RAG query agent initialized successfully")
            else:
                logger.warning("RAG query agent not initialized - missing NVIDIA credentials")
                self.rag_agent = None
        except Exception as e:
            logger.error(f"Failed to initialize RAG query agent: {str(e)}")
            self.rag_agent = None

        self.pending_quiz = None
        self.pending_announcement = None
        self.pending_assignment = None
        self.pending_page = None
        logger.info("CanvasGPT Supervisor initialized")

    def _get_conversation_context(self, current_message: str) -> str:
        """Get recent conversation context"""
        if not self.state.messages:
            return ""
            
        recent_messages = self.state.messages[-5:]  # Get last 5 messages
        conversation_parts = []
        
        for msg in recent_messages:
            role = "User" if msg.role == "user" else "Assistant"
            conversation_parts.append(f"{role}: {msg.content}")
        
        return "\n".join(conversation_parts)
            
    async def _clean_content_with_llm(self, message: str, content_type: str) -> str:
        """Use LLM to extract clean content for any Canvas LMS content type"""

        # Get conversation context
        context = self._get_conversation_context(message)
        
        prompt = f"""You are a content extractor for Canvas LMS. Your task is to extract the actual content that should be posted as a {content_type}.

        Important Guidelines:
        1. For simple posts with Text: markers, just extract that text
        2. For links, just return the URL
        3. For content referencing "above" or "previous", find and extract that content
        4. For assignments, quizzes, or pages, extract the full structured content
        5. Ignore all command language and metadata
        6. Remove course references like [course_name]
        7. Keep the content's original formatting when important (like quiz questions)

        Current message:
        {message}

        Recent conversation:
        {context}
        
        Extract and return ONLY the content that should be posted to Canvas as a {content_type}. If the message is asking to reuse previous content, find and extract that content."""

        clean_content = await self.llm.apredict(prompt)
        return clean_content.strip()





    async def _route_message(self, message: str) -> str:
        """Determine which agent should handle the message"""
        if message.lower().strip() == "show pdfs":
            return "pdf_listing"
        
        # NEW: Check for extraction requests
        message_lower = message.lower()
        if any(keyword in message_lower for keyword in ["extract", "extract data", "extract content", "analyze content"]):
            logger.info("Content extraction request detected")
            if "[" not in message:
                return "document_extraction"
        
        if message_lower.startswith("query the pdf["):
            logger.info("Routing to RAG query agent")
            return "rag_query"
        
        # First check explicitly for file upload before using GPT
        if "with the file uploaded" in message.lower():
            logger.info("File upload detected, routing to appropriate handler")
            message_lower = message.lower()
            
            # Updated routing logic for file uploads
            if ('create an assignment' in message_lower or 
                'post assignment' in message_lower or 
                'assignment where' in message_lower):
                self.state.context['post_type'] = 'assignment'
                logger.info("Assignment with file upload detected")
            elif 'as a page' in message_lower:
                self.state.context['post_type'] = 'page'
            elif 'as a quiz' in message_lower:
                self.state.context['post_type'] = 'quiz'
            else:
                self.state.context['post_type'] = 'announcement'
            
            # Extract text content if present
            if "Assignment:" in message:
                assignment_match = re.search(r'Assignment:(.*?)(?=$)', message, re.DOTALL)
                if assignment_match:
                    self.state.context["assignment_text"] = assignment_match.group(1).strip()
                    
            logger.info(f"Document handler route detected. Post type: {self.state.context.get('post_type')}")
            return self.state.context.get('post_type')

        # Rest of the routing logic remains the same...
        routing_prompt = f"""
        Given the following message, determine if it requires:
        1. rag_query - If it mentions querying PDFs or searching through documents
        2. canvas_page - If it contains 'as a page', 'create page', or any reference to pages
        3. canvas_assignment - If it mentions creating or generating an assignment
        4. canvas_quiz - If it mentions creating or generating a quiz
        5. canvas_list - If it asks about available courses or course listing
        6. web_search - If it contains a URL or asks for web content
        7. canvas_post - If it mentions posting to Canvas or course announcements
        8. general - For general queries

        Message: {message}
        
            Reply with either 'rag_query', 'canvas_page', 'canvas_assignment', 'canvas_quiz', 'canvas_list', 'web_search', 'canvas_post', or 'general' only.
            Consider these in order:
            1. If the message contains 'as a page' or mentions pages -> 'canvas_page'
            2. If the message contains 'create an assignment' -> 'canvas_assignment'
            3. If the message mentions creating a quiz -> 'canvas_quiz'
            4. If the message asks about listing courses -> 'canvas_list'
            5. If the message contains a URL -> 'web_search'
            6. If the message mentions posting to Canvas -> 'canvas_post'
            7. Otherwise -> 'general'

            or else use this:
            If the message contains any reference to searching documents, PDFs, or querying content, choose 'rag_query'.
            """

        response = await self.llm.apredict(routing_prompt)
        return response.strip().lower()   



    def _extract_title(self, message: str) -> Optional[str]:
        """Extract title from message if specified"""
        if "title:" in message.lower():
            title_match = re.search(r'title:\s*([^\n]+)', message, re.IGNORECASE)
            if title_match:
                return title_match.group(1).strip()
        return None

    async def get_available_courses(self) -> List[Dict[str, Any]]:
        """Get list of available courses"""
        if not self.canvas_agent:
            return []
        return await self.canvas_agent.list_courses()



    async def _process_extracted_content_with_llm(self, content: Dict[str, Any], file_type: str) -> str:
        """Process extracted content with LLM based on file type"""
        if file_type == '.pdf':
            # Handle PDF content
            if isinstance(content, dict) and 'full_text' in content:
                text_content = content['full_text']
                # Add section information if available
                if content.get('sections'):
                    text_content += "\n\nDocument Sections:\n"
                    for section in content['sections']:
                        if section.get('heading'):
                            text_content += f"\n{section['heading']}:\n"
                        text_content += f"{section.get('text', '')}\n"
                
                return text_content
            
        elif file_type in ['.jpg', '.jpeg', '.png']:
            # Handle image content
            if isinstance(content, dict):
                return content.get('text', 'No text extracted from image')
                
        elif file_type in ['.csv', '.xlsx']:
            # Handle spreadsheet content
            if isinstance(content, dict):
                return json.dumps(content.get('structured_data', {}), indent=2)
                
        # Fallback for unhandled content types
        return str(content)

    async def process_message(self, message: str, file_content: Optional[Dict] = None) -> Dict[str, str]:
        """Process incoming messages and route to appropriate agents"""
        try:
            # Add user message to state
            self.state.messages.append(Message(
                content=message,
                type="text",
                role="user",
                metadata={"has_file": bool(file_content)}
            ))

            # Handle confirmations first
            lower_message = message.lower()
            if lower_message in ['yes', 'post it', 'post', 'yes post it']:
                if self.pending_quiz:
                    return await self._handle_quiz_confirmation()
                elif self.pending_announcement:
                    return await self._handle_announcement_confirmation()
                elif self.pending_assignment:
                    return await self._handle_assignment_confirmation()
                elif self.pending_page:
                    return await self._handle_page_confirmation()

            # Handle cancellations
            elif lower_message in ['no', 'cancel', 'dont post', "don't post"]:
                return self._handle_cancellation()

            # NEW: Handle extraction request
            if "extract" in message.lower() and file_content and "[" not in message:
                logger.info("Processing extraction request")
                file_result = await self.document_handler.process_file(
                    file_content["file"],
                    file_content["filename"],
                    extract_mode=True
                )
                
                if not file_result["success"]:
                    return {
                        "response": f"Error processing file: {file_result.get('error', 'Unknown error')}",
                        "agent": "document_handler",
                        "conversation_id": id(self.state)
                    }
                
                # Get the extracted content
                extracted_content = file_result["content"]
                
                # If the content is a Document object list (from LlamaParse)
                if isinstance(extracted_content, list) and len(extracted_content) > 0:
                    # Extract text from the first Document object
                    content_text = extracted_content[0].text
                else:
                    # Fallback for other content types
                    content_text = str(extracted_content)
                
                # Store the formatted content for later use
                self.state.context["extracted_content"] = content_text
                
                # Add extraction result to messages
                self.state.messages.append(Message(
                    content=f"Here's what I extracted from {file_result['filename']}:\n\n{content_text}",
                    type="text",
                    role="assistant",
                    metadata={
                        "agent": "document_handler",
                        "file_type": file_result["file_type"],
                        "extracted": True,
                        "filename": file_result["filename"]
                    }
                ))
                
                return {
                    "response": f"Here's what I extracted from {file_result['filename']}:\n\n{content_text}",
                    "agent": "document_handler",
                    "conversation_id": id(self.state)
                }


            # Process file if present
            file_result = None
            if file_content:
                file_result = await self.document_handler.process_file(
                    file_content["file"],
                    file_content["filename"]
                )
                
                if not file_result["success"]:
                    return {
                        "response": f"Error processing file: {file_result.get('error', 'Unknown error')}",
                        "agent": "document_handler",
                        "conversation_id": id(self.state)
                    }
                
                # Add file content to message metadata
                self.state.messages[-1].metadata.update({
                    "file_content": file_result["content"],
                    "file_type": file_result["file_type"],
                    "filename": file_result["filename"]
                })

            # Route the message
            route = await self._route_message(message)
            logger.info(f"Message routed to: {route}")

            try:
                # Initialize context
                context = self._get_conversation_context(message)

                # Handle different routes based on message type and content
                if route == "pdf_listing":
                    if not self.pdf_listing_agent:
                        response = {
                            "response": "PDF listing is not configured. Please provide AWS credentials.",
                            "agent": "pdf_listing",
                            "conversation_id": id(self.state)
                        }
                    else:
                        result = await self.pdf_listing_agent.list_book_folders()
                        if result["success"]:
                            formatted_text = result["formatted_output"]
                            # Ensure proper line breaks are preserved
                            formatted_text = formatted_text.replace('\n', '\n')  # Force line break preservation
                            response = {
                                "response": f"```markdown\n{formatted_text}\n```",  # Wrap in markdown code block
                                "agent": "pdf_listing",
                                "conversation_id": id(self.state),
                                "success": True
                            }
                        else:
                            response = {
                                "response": f"Error listing PDFs: {result.get('error', 'Unknown error')}",
                                "agent": "pdf_listing",
                                "conversation_id": id(self.state),
                                "success": False
                            }

                elif route == "rag_query":
                    logger.info(f"RAG Agent exists: {self.rag_agent is not None}")
                    if not self.rag_agent:
                        logger.error("RAG query agent is None - Check NVIDIA API key")
                        response = {
                            "response": "RAG query agent is not configured properly.",
                            "agent": "rag_query",
                            "conversation_id": id(self.state),
                            "success": False
                        }
                    else:
                        try:
                            result = await self.rag_agent.process_query(message)
                            
                            # Format the response with matches if available
                            if result["success"] and "matches" in result:
                                response_text = result["response"] + "\n\n"
                                response_text += "Top matching chunks:\n"
                                for i, match in enumerate(result["matches"], 1):
                                    response_text += f"\n{i}. Similarity: {match['score']:.3f}\n"
                                    response_text += f"Preview: {match['text']}\n"
                            else:
                                response_text = result["response"]
                                
                            response = {
                                "response": response_text,
                                "agent": "rag_query",
                                "conversation_id": id(self.state)
                            }
                        except Exception as e:
                            logger.error(f"Error in RAG query processing: {e}")
                            response = {
                                "response": f"Error processing query: {str(e)}",
                                "agent": "rag_query",
                                "conversation_id": id(self.state)
                            }

                # Handle file upload cases
                elif file_content:
                    if route == "assignment":
                        logger.info("Processing assignment with file upload")
                        return await self._handle_assignment_request(
                            message,
                            {
                                "text": self.state.context.get("assignment_text", ""),
                                "file_content": file_result["content"],
                                "filename": file_result["filename"],
                                "file_type": file_result["file_type"]
                            }
                        )
                    elif route == "page":
                        logger.info("Processing page with file upload")
                        return await self._handle_page_request(
                            message,
                            {
                                "text": self.state.context.get("page_text", ""),
                                "file_content": file_result["content"],
                                "filename": file_result["filename"],
                                "file_type": file_result["file_type"]
                            }
                        )
                    elif route == "quiz":
                        logger.info("Processing quiz with file upload")
                        return await self._handle_quiz_request(
                            message,
                            {
                                "text": self.state.context.get("quiz_text", ""),
                                "file_content": file_result["content"],
                                "filename": file_result["filename"],
                                "file_type": file_result["file_type"]
                            }
                        )
                    elif route == "document_extraction":
                        logger.info("Processing simple extraction request")
                        file_result = await self.document_handler.process_file(
                            file_content["file"],
                            file_content["filename"],
                            extract_mode=False 
                        )
                        
                        if not file_result["success"]:
                            return {
                                "response": f"Error processing file: {file_result.get('error', 'Unknown error')}",
                                "agent": "document_handler",
                                "conversation_id": id(self.state)
                            }

                        if file_result.get("extracted"):
                            content = file_result["content"]
                            processed_content = await self._process_extracted_content_with_llm(
                                content, 
                                file_result["file_type"]
                            )
                            
                            logger.info("Extracted content:")
                            logger.info(processed_content)
                            
                            return {
                                "response": f"Here's what I extracted from the file:\n\n{processed_content}",
                                "agent": "document_handler",
                                "conversation_id": id(self.state)
                            }
                    else:  # Default to announcement
                        logger.info("Processing announcement with file upload")
                        return await self._handle_post_request(
                            message,
                            {
                                "text": self.state.context.get("announcement_text", "File uploaded"),
                                "file_content": file_result["content"],
                                "filename": file_result["filename"],
                                "file_type": file_result["file_type"]
                            }
                        )
                    

                # Handle non-file cases
                elif route == "canvas_quiz":
                    response = await self._handle_quiz_request(message, context)
                elif route == "canvas_post":
                    response = await self._handle_post_request(message, context)
                elif route == "canvas_list":
                    response = await self._handle_list_request()
                elif route == "canvas_assignment":
                    response = await self._handle_assignment_request(message, context)
                elif route == "canvas_page":
                    response = await self._handle_page_request(message, context)
                elif route == "web_search":
                    response = await self._handle_web_search(message, context)
                else:
                    response = await self._handle_general_request(message, context)

                # Store assistant response
                self.state.messages.append(Message(
                    content=response["response"],
                    type="text",
                    role="assistant",
                    metadata={"agent": route}
                ))
                
                return response

            except Exception as e:
                logger.error(f"Error in route handling: {str(e)}")
                raise

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            return {
                "error": f"Error processing message: {str(e)}",
                "response": f"An error occurred while processing your message: {str(e)}",
                "agent": "error",
                "conversation_id": id(self.state)
            }
   
   
    async def _handle_page_confirmation(self) -> Dict[str, str]:
        """Handle confirmation for page creation"""
        if not self.canvas_agent:
            return {
                "response": "Canvas is not configured. Please provide Canvas API credentials.",
                "agent": "canvas_page",
                "conversation_id": id(self.state)
            }

        try:
            result = await self.canvas_agent.process(
                self.pending_page['content'],
                f"page for [{self.pending_page['course_name']}] title: {self.pending_page['title']}"
            )
            
            if result.get('success', False):
                response = f"Successfully created page in {self.pending_page['course_name']}!"
            else:
                response = f"Failed to create page: {result.get('message', 'Unknown error')}"
            
            self.pending_page = None  # Clear pending page
            
            return {
                "response": response,
                "agent": "canvas_page",
                "conversation_id": id(self.state)
            }
            
        except Exception as e:
            logger.error(f"Error creating page: {str(e)}")
            return {
                "response": f"Error creating page: {str(e)}",
                "agent": "canvas_page",
                "conversation_id": id(self.state)
            }



    async def _handle_page_request(self, message: str, content: Union[str, Dict]) -> Dict[str, str]:
        """Handle page creation requests with URL support"""
        if not self.canvas_agent:
            return {
                "response": "Canvas is not configured. Please provide Canvas API credentials.",
                "agent": "canvas_page",
                "conversation_id": id(self.state)
            }

        course_match = re.search(r'\[(.*?)\]', message)
        if not course_match:
            return {
                "response": "Please specify a course name in square brackets, e.g. [Course Name]",
                "agent": "canvas_page",
                "conversation_id": id(self.state)
            }
        
        course_name = course_match.group(1)
        title = self._extract_title(message)
        
        # Extract URL if present
        url_match = re.search(r'link:(https?://[^\s]+)', message)
        if url_match:
            url = url_match.group(1)
            # Extract content from URL
            url_content = await self.web_agent.extract_url_content(url)
            if url_content["success"]:
                cleaned_content = url_content["content"]
                # Add source reference
                cleaned_content += f"\n\nSource: {url_content['original_url']}"
            else:
                return {
                    "response": f"Failed to extract content from URL: {url_content.get('error', 'Unknown error')}",
                    "agent": "canvas_page",
                    "conversation_id": id(self.state)
                }
        else:
            # Handle regular content
            cleaned_content = await self._clean_content_with_llm(message, "page")
        
        if isinstance(content, dict) and content.get("file_content"):
            self.pending_page = {
                "course_name": course_name,
                "content": cleaned_content,
                "title": title,
                "file_content": content["file_content"],
                "file_name": content.get("filename")
            }
        else:
            self.pending_page = {
                "course_name": course_name,
                "content": cleaned_content,
                "title": title
            }
        
        response = (
            f"I've prepared the page for {course_name}:\n"
            f"Title: {title}\n"
            f"Content:\n{cleaned_content}\n\n"
        )
        
        if "file_content" in self.pending_page:
            response += f"File to be attached: {self.pending_page['file_name']}\n\n"
        
        response += "Would you like me to create this page? (Reply with 'yes' to create or 'no' to cancel)"
        
        return {
            "response": response,
            "agent": "canvas_page",
            "conversation_id": id(self.state)
        }

    async def _handle_post_request(self, message: str, content: Union[str, Dict]) -> Dict[str, str]:
        """Handle announcement posting requests"""
        if not self.canvas_agent:
            return {
                "response": "Canvas is not configured. Please provide Canvas API credentials.",
                "agent": "canvas_post",
                "conversation_id": id(self.state)
            }

        course_match = re.search(r'\[(.*?)\]', message)
        if not course_match:
            return {
                "response": "Please specify a course name in square brackets, e.g. [Course Name]",
                "agent": "canvas_post",
                "conversation_id": id(self.state)
            }
        
        course_name = course_match.group(1)
        title = self._extract_title(message)
        
        if not title:
            title = await self.canvas_agent.announcement_agent.generate_title(message)
        
        # Clean content using LLM
        cleaned_content = await self._clean_content_with_llm(message, "announcement")
        
            # Handle file upload case
        if isinstance(content, dict) and content.get("file_content"):
            self.pending_announcement = {
                "course_name": course_name,
                "content": cleaned_content,
                "title": title,
                "file_content": content["file_content"],
                "filename": content.get("filename")
            }
        else:
            self.pending_announcement = {
                "course_name": course_name,
                "content": cleaned_content,
                "title": title
            }
        
        response = (
            f"Here's the announcement for {course_name}:\n"
            f"Title: {title}\n"
            f"{self.pending_announcement['content']}\n\n"
        )
        
        if "file_content" in self.pending_announcement:
            response += f"File to be uploaded: {self.pending_announcement['filename']}\n\n"
        
        response += "Would you like me to post this announcement? (Reply with 'yes' to post or 'no' to cancel)"
        
        return {
            "response": response,
            "agent": "canvas_post",
            "conversation_id": id(self.state)
        }
    async def _handle_quiz_confirmation(self) -> Dict[str, str]:
        """Handle confirmation for quiz creation"""
        if not self.canvas_agent:
            return {
                "response": "Canvas is not configured. Please provide Canvas API credentials.",
                "agent": "canvas_quiz",
                "conversation_id": id(self.state)
            }

        try:
            course_name = self.pending_quiz['course_name']
            content = self.pending_quiz['content']
            title = self.pending_quiz['title']
            
            quiz_result = await self.canvas_agent.process(
                content,
                f"title: {title}\nquiz for [{course_name}]"
            )
            
            if quiz_result.get('success', False):
                response = f"Successfully created quiz in {course_name}!"
            else:
                response = f"Failed to create quiz: {quiz_result.get('message', 'Unknown error')}"
                
            self.pending_quiz = None
                
        except Exception as e:
            response = f"Error creating quiz: {str(e)}"
        
        return {
            "response": response,
            "agent": "canvas_quiz",
            "conversation_id": id(self.state)
        }


    async def _handle_announcement_confirmation(self) -> Dict[str, str]:
        """Handle confirmation for announcement posting"""
        if not self.canvas_agent:
            return {
                "response": "Canvas is not configured. Please provide Canvas API credentials.",
                "agent": "canvas_post",
                "conversation_id": id(self.state)
            }

        try:
            course_name = self.pending_announcement['course_name']
            content = self.pending_announcement['content']
            title = self.pending_announcement['title']
            
            # Get file content if present
            file_content = self.pending_announcement.get('file_content')
            file_name = self.pending_announcement.get('filename')  # This needs to be changed to 'file_name'
            
            if file_content and file_name:
                # Create dictionary format for content
                content = {
                    'text': content,
                    'file_content': file_content,
                    'filename': file_name  # Match the parameter name expected by process method
                }

            # Process the announcement
            result = await self.canvas_agent.process(
                content=content,
                message=f"title: {title}\nannouncement for [{course_name}]"
            )
            
            if result.get('success', False):
                response = "Successfully posted announcement"
                if file_content:
                    response += " with file attachment"
                response += f" to {course_name}!"
            else:
                response = f"Failed to post announcement: {result.get('message', 'Unknown error')}"
                
            self.pending_announcement = None
            
            return {
                "response": response,
                "agent": "canvas_post",
                "conversation_id": id(self.state)
            }
                
        except Exception as e:
            logger.error(f"Error posting announcement: {str(e)}")
            return {
                "response": f"Error posting announcement: {str(e)}",
                "agent": "canvas_post",
                "conversation_id": id(self.state)
            }



    async def _handle_assignment_confirmation(self) -> Dict[str, str]:
        """Handle confirmation for assignment creation with file upload support"""
        if not self.canvas_agent:
            return {
                "response": "Canvas is not configured. Please provide Canvas API credentials.",
                "agent": "canvas_assignment",
                "conversation_id": id(self.state)
            }

        try:
            # Get course ID
            course_id = await self.canvas_agent.get_course_id(self.pending_assignment['course_name'])
            if "extracted_content" in self.state.context:
                self.pending_assignment["content"] = self.state.context["extracted_content"]
                logger.info("Using extracted content for assignment creation")
                logger.info(f"Content being used: {self.pending_assignment['content']}")
            if not course_id:
                return {
                    "response": f"Could not find course: {self.pending_assignment['course_name']}",
                    "agent": "canvas_assignment",
                    "conversation_id": id(self.state)
                }
            
            # If we have a file, use the new process_file_and_create_assignment method
            if "file_content" in self.pending_assignment:
                result = await self.canvas_agent.assignment_agent.process_file_and_create_assignment(
                    course_id=course_id,
                    file_content=self.pending_assignment['file_content'],
                    file_name=self.pending_assignment['file_name'],
                    title=self.pending_assignment['title'],
                    description=self.pending_assignment['content'],
                    points=self.pending_assignment['points'],
                    submission_types=self.pending_assignment['submission_types']
                )
            else:
                # Regular assignment creation without file
                result = await self.canvas_agent.assignment_agent.create_assignment(
                    course_id=course_id,
                    name=self.pending_assignment['title'],
                    description=self.pending_assignment['content'],
                    points=self.pending_assignment['points'],
                    submission_types=self.pending_assignment['submission_types']
                )
            
            if result.get("error"):
                response = f"Failed to create assignment: {result['error']}"
            else:
                response = f"Successfully created assignment in {self.pending_assignment['course_name']}!"
                if "file_url" in result:
                    response += f"\nFile uploaded and attached to the assignment."
            
            self.pending_assignment = None
            
            return {
                "response": response,
                "agent": "canvas_assignment",
                "conversation_id": id(self.state),
                "success": "error" not in result
            }
            
        except Exception as e:
            logger.error(f"Error creating assignment: {str(e)}")
            return {
                "response": f"Error creating assignment: {str(e)}",
                "agent": "canvas_assignment",
                "conversation_id": id(self.state),
                "success": False
            }        
        

    def _handle_cancellation(self) -> Dict[str, str]:
        """Handle cancellation of pending operations"""
        if self.pending_quiz:
            self.pending_quiz = None
            return {
                "response": "Quiz creation cancelled.",
                "agent": "canvas_quiz",
                "conversation_id": id(self.state)
            }
        elif self.pending_announcement:
            self.pending_announcement = None
            return {
                "response": "Announcement cancelled.",
                "agent": "canvas_post",
                "conversation_id": id(self.state)
            }
        elif self.pending_assignment:
            self.pending_assignment = None
            return {
                "response": "Assignment creation cancelled.",
                "agent": "canvas_assignment",
                "conversation_id": id(self.state)
            }
        elif self.pending_page:  # Add this block
            self.pending_page = None
            return {
                "response": "Page creation cancelled.",
                "agent": "canvas_page",
                "conversation_id": id(self.state)
            }
        return {
            "response": "Nothing to cancel.",
            "agent": "general",
            "conversation_id": id(self.state)
        }

    async def _handle_quiz_request(self, message: str, content: str) -> Dict[str, str]:
        """Handle quiz creation requests"""
        if not self.canvas_agent:
            return {
                "response": "Canvas is not configured. Please provide Canvas API credentials.",
                "agent": "canvas_quiz",
                "conversation_id": id(self.state)
            }

        course_match = re.search(r'\[(.*?)\]', message)
        if not course_match:
            return {
                "response": "Please specify a course name in square brackets, e.g. [Course Name]",
                "agent": "canvas_quiz",
                "conversation_id": id(self.state)
            }
        
        course_name = course_match.group(1)
        title = self._extract_title(message) or "Quiz"
        
        # Clean content using LLM
        cleaned_content = await self._clean_content_with_llm(message, "quiz")
        
        self.pending_quiz = {
            "course_name": course_name,
            "content": cleaned_content,
            "title": title
        }
        
        response = (
            f"I've prepared the quiz for {course_name}:\n"
            f"Title: {title}\n\n"
            f"Content:\n{cleaned_content}\n\n"
            "Would you like me to create this quiz? (Reply with 'yes' to create or 'no' to cancel)"
        )
        
        return {
            "response": response,
            "agent": "canvas_quiz",
            "conversation_id": id(self.state)
        }


    async def _handle_list_request(self) -> Dict[str, str]:
        """Handle course listing requests"""
        if not self.canvas_agent:
            return {
                "response": "Canvas is not configured. Please provide Canvas API credentials.",
                "agent": "canvas_list",
                "conversation_id": id(self.state)
            }

        courses = await self.get_available_courses()
        if courses:
            course_list = "\n".join([
                f"• {course['name']} (Code: {course['code']})"
                + (f" - {course['students']} students" if course.get('students') else "")
                for course in courses
            ])
            response = f"Available courses:\n{course_list}"
        else:
            response = "No courses found or error retrieving courses."
        
        return {
            "response": response,
            "agent": "canvas_list",
            "conversation_id": id(self.state)
        }

    async def _handle_assignment_request(self, message: str, content: Union[str, Dict]) -> Dict[str, str]:
        """Handle assignment creation requests with file upload support"""
        if not self.canvas_agent:
            return {
                "response": "Canvas is not configured. Please provide Canvas API credentials.",
                "agent": "canvas_assignment",
                "conversation_id": id(self.state)
            }

        # Extract course name
        course_match = re.search(r'\[(.*?)\]', message)
        if not course_match:
            return {
                "response": "Please specify a course name in square brackets, e.g. [Course Name]",
                "agent": "canvas_assignment",
                "conversation_id": id(self.state)
            }
        
        course_name = course_match.group(1)
        title = self._extract_title(message)
        points_match = re.search(r'points\s*should\s*be\s*(\d+)', message)
        points = int(points_match.group(1)) if points_match else 100
        
        # Clean content using LLM
        cleaned_content = await self._clean_content_with_llm(message, "assignment")

        if isinstance(content, dict) and content.get("file_content"):
            self.pending_assignment = {
                "course_name": course_name,
                "content": cleaned_content,
                "title": title,
                "points": points,
                "submission_types": self.canvas_agent.parse_submission_types(message),
                "file_content": content["file_content"],
                "file_name": content.get("filename")
            }
        else:
            # Store as pending assignment without file
            self.pending_assignment = {
                "course_name": course_name,
                "content": cleaned_content,
                "title": title,
                "points": points,
                "submission_types": self.canvas_agent.parse_submission_types(message)
            }
        
        response = (
            f"I've prepared the assignment for {course_name}:\n"
            f"Title: {title}\n"
            f"Points: {points}\n"
            f"Content:\n{cleaned_content}\n\n"
        )
        
        if "file_content" in self.pending_assignment:
            response += f"File to be attached: {self.pending_assignment['file_name']}\n\n"
        
        response += "Would you like me to create this assignment? (Reply with 'yes' to create or 'no' to cancel)"
        
        return {
            "response": response,
            "agent": "canvas_assignment",
            "conversation_id": id(self.state)
        }

    async def _handle_web_search(self, message: str, context: str) -> Dict[str, str]:
        """Handle web search requests"""
        response = await self.web_agent.process(
            message,
            conversation_context=context if context else None
        )
        
        return {
            "response": response,
            "agent": "web_search",
            "conversation_id": id(self.state)
        }

    async def _handle_general_request(self, message: str, context: str) -> Dict[str, str]:
        """Handle general requests using LLM"""
        if context:
            llm_prompt = (
                f"{context}\n"
                "Please provide a response considering the conversation history above."
            )
        else:
            llm_prompt = message
            
        response = await self.llm.apredict(llm_prompt)
        
        return {
            "response": response,
            "agent": "general",
            "conversation_id": id(self.state)
        }

    async def get_state(self) -> Dict[str, Any]:
        """Return current supervisor state"""
        return self.state.dict()

    def get_conversation_history(self) -> List[Dict[str, str]]:
        """Get formatted conversation history"""
        return [
            {
                "role": msg.role,
                "content": msg.content,
                "metadata": msg.metadata
            }
            for msg in self.state.messages
        ]

    async def reset_state(self):
        """Reset supervisor state"""
        self.state = SupervisorState()
        self.web_agent = WebSearchAgent()  # Create new web agent instance
        self.pending_announcement = None  # Clear any pending announcements
        self.pending_quiz = None  # Clear any pending quizzes
        self.pending_assignment = None  # Clear any pending assignments
        self.pending_page = None  # Clear any pending pages - Add this line
        if self.canvas_agent:
            await self.canvas_agent.close()  # Close any existing Canvas sessions
        logger.info("Supervisor state fully reset")

    async def close(self):
        """Cleanup method for closing all agent sessions"""
        if hasattr(self, 'web_agent'):
            await self.web_agent.close()
        if hasattr(self, 'canvas_agent'):
            await self.canvas_agent.close()
        if hasattr(self, 'document_handler'):
            await self.document_handler.close()
        if hasattr(self, 'pdf_listing_agent'):
            await self.pdf_listing_agent.close()
        logger.info("All agent sessions closed")