from typing import Dict, Any, Optional
import aiohttp
import logging
import re
from .base import CanvasBaseAgent
from langchain_openai import ChatOpenAI
import html

logger = logging.getLogger(__name__)

class PagesAgent(CanvasBaseAgent):
    """Agent for handling Canvas page operations with LLM processing"""
    
    def __init__(self, api_key: str, base_url: str):
        super().__init__(api_key, base_url)
        self.llm = ChatOpenAI()

    async def _format_content(self, content: str) -> str:
        """Format content into HTML with LLM assistance"""
        format_prompt = f"""Format this text into clean HTML for a Canvas page.
        Use semantic HTML elements (p, ul, ol, etc.) as appropriate.
        Keep the formatting simple and clean.
        If the content appears to be plain text, wrap it in paragraph tags.

        Original Text: {content}

        Return only the formatted HTML without any explanation or original text."""

        try:
            formatted_content = await self.llm.apredict(format_prompt)
            # Ensure content has HTML wrapper
            if not formatted_content.strip().startswith('<'):
                formatted_content = f"<p>{formatted_content}</p>"
            logger.info(f"Formatted content: {formatted_content}")
            return formatted_content
        except Exception as e:
            logger.error(f"Error formatting content with LLM: {str(e)}")
            # Fallback to basic HTML formatting
            return f"<p>{html.escape(content)}</p>"

    def _clean_content(self, content: str) -> str:
        """Clean and prepare content for formatting"""
        try:
            # Remove Text: marker if present
            content = re.sub(r'^Text:\s*', '', content, flags=re.IGNORECASE)
            # Remove surrounding quotes if present
            content = content.strip('"\'')
            # Clean up whitespace
            content = content.strip()
            logger.info(f"Cleaned content: {content}")
            return content
        except Exception as e:
            logger.error(f"Error cleaning content: {str(e)}")
            return content

    async def create_page(
        self,
        course_id: str,
        title: str,
        body: str,
        published: bool = True,
        editing_roles: str = "teachers"
    ) -> Dict[str, Any]:
        """Create a new page in a Canvas course"""
        try:
            await self._ensure_session()
            
            if not title or not title.strip():
                return {
                    'success': False,
                    'message': "Title cannot be blank"
                }
            
            # Clean and format the content
            cleaned_body = self._clean_content(body)
            formatted_body = await self._format_content(cleaned_body)
            
            endpoint = f"{self.base_url}/api/v1/courses/{course_id}/pages"
            
            page_data = {
                "wiki_page": {
                    "title": title.strip(),
                    "body": formatted_body,
                    "editing_roles": editing_roles,
                    "published": published,
                    "notify_of_update": False
                }
            }
            
            logger.info(f"Creating page with data: {page_data}")
            
            async with self.session.post(
                endpoint,
                headers=self.headers,
                json=page_data
            ) as response:
                response_text = await response.text()
                logger.info(f"Create page response status: {response.status}")
                logger.info(f"Create page response: {response_text}")
                
                if response.status not in [200, 201]:
                    return {
                        'success': False,
                        'message': f"Failed to create page: {response_text}"
                    }
                
                result = await response.json()
                return {
                    'success': True,
                    'page_id': result.get('page_id'),
                    'title': result.get('title'),
                    'url': result.get('html_url'),
                    'created_at': result.get('created_at'),
                    'published': result.get('published', False)
                }
                    
        except Exception as e:
            error_msg = f"Error in create_page: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg
            }

    async def process_page_request(self, content: str, message: str) -> Dict[str, Any]:
        """Process a page creation request"""
        try:
            # Extract course name
            course_match = re.search(r'\[(.*?)\]', message)
            if not course_match:
                return {
                    'success': False,
                    'message': "Please specify a course name in square brackets, e.g. [Course Name]"
                }
            
            course_name = course_match.group(1)
            course_id = await self.get_course_id(course_name)
            
            if not course_id:
                return {
                    'success': False,
                    'message': f"Could not find course: {course_name}"
                }
            
            # Extract title
            title = None
            title_match = re.search(r'title:\s*([^\n]+)', message, re.IGNORECASE)
            if title_match:
                title = title_match.group(1).strip()
            
            # Extract text content if present
            text_match = re.search(r'Text:\s*"([^"]+)"', message) or \
                        re.search(r'Text:\s*\'([^\']+)\'', message) or \
                        re.search(r'Text:\s*([^\n]+)', message)
            
            if text_match:
                content = text_match.group(1)
            
            if not content:
                return {
                    'success': False,
                    'message': "No content provided for the page"
                }
            
            if not title:
                title = "New Page"
            
            logger.info(f"Processing page request:")
            logger.info(f"Course: {course_name}")
            logger.info(f"Title: {title}")
            logger.info(f"Content: {content}")
            
            # Create the page
            result = await self.create_page(
                course_id=course_id,
                title=title,
                body=content,
                published=True
            )
            
            if result.get('success'):
                message = f"Successfully created page '{title}' in {course_name}"
                if result.get('url'):
                    message += f"\nURL: {result['url']}"
            else:
                message = result.get('message', 'Unknown error occurred')
            
            return {
                'success': result.get('success', False),
                'message': message,
                'details': result
            }
                
        except Exception as e:
            error_msg = f"Error processing page request: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg
            }

    async def get_course_id(self, course_name: str) -> Optional[str]:
        """Get Canvas course ID from course name"""
        try:
            endpoint = f"{self.base_url}/api/v1/courses"
            params = {
                'enrollment_type': 'teacher',
                'state[]': ['available', 'completed', 'created']
            }
            
            async with self.session.get(
                endpoint,
                headers=self.headers,
                params=params
            ) as response:
                if response.status == 200:
                    courses = await response.json()
                    for course in courses:
                        if course_name.lower() in course['name'].lower():
                            return str(course['id'])
                return None
                
        except Exception as e:
            logger.error(f"Error getting course ID: {str(e)}")
            return None