from .base import CanvasBaseAgent
from typing import Dict, Any, List, Optional, Union
import logging
import re
from datetime import datetime, timezone
import json
from datetime import datetime, timezone, timedelta  
import aiohttp

logger = logging.getLogger(__name__)

class AssignmentAgent(CanvasBaseAgent):
    """Enhanced agent for managing Canvas assignments with improved parsing"""
    
    SUBMISSION_TYPES = {
        "no submission": ["none"],
        "text entry": ["online_text_entry"],
        "website url": ["online_url"],
        "file uploads": ["online_upload"],
        "media recording": ["media_recording"],
        "student annotation": ["student_annotation"],
        "external tool": ["external_tool"],
        "on paper": ["on_paper"],
        "online": ["online_text_entry", "online_url", "online_upload", "media_recording"]
    }
    
    def parse_questions(self, content: str) -> str:
        """Parse questions into HTML format for Canvas"""
        html_content = "<div class='assignment-questions'>"
        
        # Split content into questions
        questions = re.split(r'\d+\.', content)[1:]  # Skip empty first split
        
        for i, question in enumerate(questions, 1):
            html_content += f"<div class='question'><p><strong>Question {i}.</strong> "
            
            # Split into question text and options
            parts = question.strip().split('Options:', 1)
            if len(parts) == 2:
                question_text, options = parts
                html_content += f"{question_text.strip()}</p>"
                
                # Parse options
                html_content += "<ul class='options'>"
                options_list = options.strip().split('\n')
                for option in options_list:
                    if option.strip().startswith(('A.', 'B.', 'C.', 'D.')):
                        html_content += f"<li>{option.strip()}</li>"
                html_content += "</ul>"
                
                # Extract correct answer if present
                correct_match = re.search(r'\(Correct Answer:\s*([A-D])\)', question)
                if correct_match:
                    correct_answer = correct_match.group(1)
                    html_content += f"<p class='correct-answer'><em>Correct Answer: {correct_answer}</em></p>"
            else:
                html_content += f"{question.strip()}</p>"
            
            html_content += "</div>"
        
        html_content += "</div>"
        return html_content

    def format_assignment_content(self, content: str) -> str:
        """Format assignment content with proper HTML structure"""
        try:
            formatted_content = "<div class='assignment-content'>"
            
            # Split content into lines and process
            lines = content.strip().split('\n')
            current_section = None
            section_content = []
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Check for question pattern (q1:, q2:, etc.)
                q_match = re.match(r'q(\d+):\s*(.*)', line, re.IGNORECASE)
                
                if q_match:
                    # If we have a previous section, add it
                    if current_section:
                        formatted_content += self._format_section(current_section, section_content)
                        section_content = []
                    
                    # Start new question section
                    q_num = q_match.group(1)
                    q_text = q_match.group(2)
                    formatted_content += f"<div class='question-block'>"
                    formatted_content += f"<h3>Question {q_num}</h3>"
                    formatted_content += f"<p class='question-text'>{q_text}</p>"
                    formatted_content += "</div>"
                else:
                    # Regular content
                    formatted_content += f"<p>{line}</p>"

            # Add any remaining section
            if current_section and section_content:
                formatted_content += self._format_section(current_section, section_content)
            
            formatted_content += "</div>"
            
            # Add CSS styling
            style = """
            <style>
                .assignment-content {
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    margin: 20px 0;
                }
                .question-block {
                    margin: 20px 0;
                    padding: 15px;
                    border-left: 3px solid #2196F3;
                    background-color: #f8f9fa;
                }
                .question-block h3 {
                    color: #2196F3;
                    margin: 0 0 10px 0;
                }
                .question-text {
                    margin: 0;
                }
            </style>
            """
            
            return style + formatted_content
            
        except Exception as e:
            logger.error(f"Error formatting assignment content: {str(e)}")
            return content

    def _format_section(self, section_type: str, content: List[str]) -> str:
        """Helper method to format content sections"""
        if not content:
            return ""
            
        html = f"<div class='{section_type}-section'>"
        if section_type == "steps":
            html += "<ol>"
            for item in content:
                html += f"<li>{item}</li>"
            html += "</ol>"
        else:
            for item in content:
                html += f"<p>{item}</p>"
        html += "</div>"
        return html

    def parse_submission_types(self, query: str) -> List[str]:
        """Extract submission types from query"""
        submission_types = []
        query_lower = query.lower()
        
        # Check for specific submission type mentions
        for key, values in self.SUBMISSION_TYPES.items():
            if key in query_lower:
                submission_types.extend(values)
                
        # Default to online text entry if no specific type mentioned
        if not submission_types:
            submission_types = ["online_text_entry"]
            
        return list(set(submission_types))

    def parse_points(self, query: str) -> int:
        """Extract points from query"""
        points_match = re.search(r'points?\s*(?:should\s*be\s*)?(\d+)', query.lower())
        return int(points_match.group(1)) if points_match else 100

    def parse_due_date(self, query: str) -> Optional[str]:
        """Extract due date from query and convert to Canvas-compatible ISO 8601 format"""
        try:
            # First look for the date pattern
            date_patterns = [
                # Match format like "12/7/2024 10:00 PM"
                r'due\s*(?:date|on|by)?\s*(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s*(?:AM|PM|am|pm))',
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    date_str = match.group(1).strip()
                    try:
                        # Parse the datetime
                        dt = datetime.strptime(date_str, "%m/%d/%Y %I:%M %p")
                        
                        # Convert to user's timezone (assuming UTC for now)
                        local_tz = timezone.utc
                        dt = dt.replace(tzinfo=local_tz)
                        
                        # Format in Canvas's expected format (ISO 8601 with timezone)
                        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                        
                    except ValueError as e:
                        logger.error(f"Error parsing date '{date_str}': {str(e)}")
                        return None
            
            logger.info("No valid date pattern found in query")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing date: {str(e)}")
            return None

    async def process_file_and_create_assignment(self, course_id: str, file_content: bytes, file_name: str, 
                                               title: str, description: str, points: int = 100, 
                                               submission_types: List[str] = None) -> Dict[str, Any]:
            """First upload file to Canvas and then create assignment with file URL"""
            try:
                # Ensure session is initialized
                await self._ensure_session()

                # Step 1: Upload file to course files
                logger.info(f"Uploading file {file_name} to course {course_id}")
                file_url = await self.upload_file(course_id, file_content, file_name)
                
                if not file_url:
                    return {
                        "error": "Failed to upload file",
                        "success": False
                    }

                # Step 2: Format description with file link
                formatted_description = f"""
                {description}
                
                <p>Reference Material: <a href="{file_url}" target="_blank">{file_name}</a></p>
                """

                # Step 3: Create assignment with file reference
                assignment_result = await self.create_assignment(
                    course_id=course_id,
                    name=title,
                    description=formatted_description,
                    points=points,
                    submission_types=submission_types or ["online_text_entry"]
                )

                if "error" in assignment_result:
                    return {
                        "error": assignment_result["error"],
                        "success": False
                    }

                return {
                    "success": True,
                    "assignment": assignment_result,
                    "file_url": file_url
                }

            except Exception as e:
                logger.error(f"Error in process_file_and_create_assignment: {str(e)}")
                return {
                    "error": str(e),
                    "success": False
                }

    async def upload_file(self, course_id: str, file_content: bytes, file_name: str) -> Optional[str]:
        """Upload a file to Canvas course files and return the file URL"""
        try:
            # Step 1: Request file upload URL
            pre_upload_response = await self.session.post(
                f"{self.base_url}/api/v1/courses/{course_id}/files",
                headers=self.headers,
                json={
                    'name': file_name,
                    'size': len(file_content),
                    'content_type': 'application/octet-stream',
                    'parent_folder_path': 'assignment_files'  # Store in a specific folder
                }
            )
            
            if pre_upload_response.status != 200:
                logger.error(f"Failed to get upload URL: {await pre_upload_response.text()}")
                return None
                
            upload_data = await pre_upload_response.json()
            upload_url = upload_data.get('upload_url')
            
            if not upload_url:
                return None
            
            # Step 2: Upload file content
            form = aiohttp.FormData()
            form.add_field('file', 
                        file_content,
                        filename=file_name,
                        content_type='application/octet-stream')
            
            async with self.session.post(
                upload_url,
                headers={'Authorization': self.headers['Authorization']},
                data=form
            ) as upload_response:
                if upload_response.status in [200, 201]:
                    file_data = await upload_response.json()
                    file_id = file_data.get('id')
                    
                    # Step 3: Get file URL
                    async with self.session.get(
                        f"{self.base_url}/api/v1/files/{file_id}",
                        headers=self.headers
                    ) as file_info_response:
                        if file_info_response.status == 200:
                            file_info = await file_info_response.json()
                            return file_info.get('url')
            
            return None
            
        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            return None


    async def _format_content_with_llm(self, content: str) -> str:
        """Use LLM to detect content type and format appropriately"""
        try:
            llm = ChatOpenAI()
            
            prompt = f"""Format the following content for a Canvas LMS Assignment creation, paying special attention to tables and typography.

Rules for formatting:
1. Tables:
   - Must be enclosed in proper <table> tags
   - Each row must use <tr> tags
   - Headers must use <th> tags
   - Data cells must use <td> tags
   - Add borders and padding for readability
   - Tables must be responsive

2. Typography:
   - Base font size should be 14px
   - Headers should use relative sizes:
     * h1: 20px
     * h2: 16px
     * h3: 12px
   - Line height should be 1.5
   - Use Arial or sans-serif fonts

3. Structure:
   - Each section should be clearly separated
   - Numbered lists should use <ol> tags
   - Add appropriate spacing between elements
   - Preserve document hierarchy

Here's the content to format:
{content}

Required HTML structure:
<div style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.5; color: #333;">
  [Main content here with all headers, tables, and lists properly formatted]
</div>

Tables should follow this structure:
<div style="overflow-x: auto;">
  <table style="border-collapse: collapse; width: 100%; margin: 15px 0;">
    <thead>
      <tr>
        <th style="border: 1px solid #ddd; padding: 8px; background-color: #f5f6fa;">[header]</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td style="border: 1px solid #ddd; padding: 8px;">[data]</td>
      </tr>
    </tbody>
  </table>
</div>

Return ONLY the formatted HTML, no explanations. Ensure all tables are properly formatted with the exact structure shown above."""

            formatted_content = await llm.apredict(prompt)
            
            # Ensure we have wrapping div with basic styles
            if not formatted_content.strip().startswith('<div'):
                formatted_content = f'''
                    <div style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333;">
                        {formatted_content}
                    </div>
                '''
            
            return formatted_content.strip()
            
        except Exception as e:
            logger.error(f"Error formatting content with LLM: {str(e)}")
            # Fallback to basic formatting
            return self._format_basic_content(content)

    def _format_basic_content(self, content: str) -> str:
        """Basic formatting for simple text content"""
        try:
            formatted_content = []
            current_section = None
            current_lines = []
            
            for line in content.split('\n'):
                line = line.strip()
                if not line:
                    if current_lines:
                        if current_section:
                            formatted_content.append(
                                f'<h3 style="color: #2c3e50; margin: 15px 0 10px 0;">{current_section}</h3>'
                            )
                        formatted_content.append(
                            f'<p style="margin: 8px 0; line-height: 1.6;">{" ".join(current_lines)}</p>'
                        )
                        current_lines = []
                    continue
                
                # Check if this is a section header (ends with ':')
                if line.endswith(':') and len(line.split()) <= 3:
                    if current_lines:
                        formatted_content.append(
                            f'<p style="margin: 8px 0; line-height: 1.6;">{" ".join(current_lines)}</p>'
                        )
                        current_lines = []
                    current_section = line[:-1]  # Remove the colon
                    continue
                
                current_lines.append(line)
            
            # Handle any remaining lines
            if current_lines:
                if current_section:
                    formatted_content.append(
                        f'<h3 style="color: #2c3e50; margin: 15px 0 10px 0;">{current_section}</h3>'
                    )
                formatted_content.append(
                    f'<p style="margin: 8px 0; line-height: 1.6;">{" ".join(current_lines)}</p>'
                )
            
            return f'''
                <div style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">
                    {" ".join(formatted_content)}
                </div>
            '''
            
        except Exception as e:
            logger.error(f"Error in basic formatting: {str(e)}")
            return f'<div style="font-family: Arial, sans-serif;">{content}</div>'


    async def create_assignment(self, course_id: str, name: str, description: str,
                            points: int = 100, due_date: Optional[str] = None, 
                            submission_types: Optional[List[str]] = None,
                            file_content: bytes = None, file_name: str = None) -> Dict[str, Any]:
        """Create a course assignment with enhanced parsing and file support"""
        try:
            await self._ensure_session()
            
            # Format the content based on type
            formatted_description = await self._format_content_with_llm(description)

            
            # Handle file upload if provided
            file_url = None
            if file_content and file_name:
                file_url = await self.upload_file(course_id, file_content, file_name)
                if file_url:
                    # Append file link to description with consistent styling
                    file_html = f'''
                        <div style="margin-top: 20px; padding: 12px; 
                                border: 1px solid #e0e0e0; border-radius: 4px; 
                                background-color: #f8f9fa;">
                            <p style="margin: 0;">
                                Attached file: <a href="{file_url}" 
                                            target="_blank" 
                                            style="color: #2196F3; text-decoration: none;">
                                            {file_name}
                                            </a>
                            </p>
                        </div>
                    '''
                    formatted_description = formatted_description + file_html

            # Create the assignment payload
            payload = {
                'assignment': {
                    'name': name,
                    'description': formatted_description,
                    'points_possible': points,
                    'submission_types': submission_types or ["online_text_entry"],
                    'published': True
                }
            }

            # Add due date if provided
            if due_date:
                payload['assignment']['due_at'] = due_date

            # Create a safe payload for logging (without file content)
            log_payload = {
                'assignment': {
                    'name': name,
                    'points_possible': points,
                    'submission_types': submission_types or ["online_text_entry"],
                    'has_file_attachment': bool(file_url),
                    'file_name': file_name if file_url else None,
                    'due_date': due_date if due_date else None
                }
            }
            
            logger.info(f"Creating assignment with payload: {json.dumps(log_payload, indent=2)}")
            
            # Make the API request to create the assignment
            async with self.session.post(
                f"{self.base_url}/api/v1/courses/{course_id}/assignments",
                headers=self.headers,
                json=payload
            ) as response:
                if response.status not in (200, 201):
                    error_text = await response.text()
                    logger.error(f"Error creating assignment: {error_text}")
                    return {"error": f"API Error: {error_text}"}
                
                # Process the successful response
                result = await response.json()
                if file_url:
                    result['file_url'] = file_url
                
                logger.info(f"Successfully created assignment: {result.get('id')}")
                return result

        except Exception as e:
            logger.error(f"Error creating assignment: {str(e)}")
            return {"error": str(e)}     
            
    async def process_assignment_query(self, query: str, course_id: str, 
                                file_content: bytes = None, file_name: str = None) -> Dict[str, Any]:
        """Process an assignment creation query with enhanced formatting"""
        try:
            # Extract title/name
            name = "Assignment"  # default name
            title_match = re.search(r'title:\s*([^\n]+)', query)
            if title_match:
                name = title_match.group(1).strip()
            
            # Extract text content if specified
            text_match = re.search(r'Text:\s*"([^"]+)"', query)
            description = text_match.group(1) if text_match else "Assignment Content"
            
            # Parse other parameters
            points = self.parse_points(query)
            submission_types = self.parse_submission_types(query)
            
            # Create log-safe data structure
            log_data = {
                "query_info": {
                    "title": name,
                    "has_file": bool(file_content),
                    "file_name": file_name if file_name else None,
                    "points": points,
                    "submission_types": submission_types
                }
            }
            
            logger.info(f"Processing assignment query: {json.dumps(log_data, indent=2)}")
            
            # Create the assignment
            return await self.create_assignment(
                course_id=course_id,
                name=name,
                description=description,
                points=points,
                submission_types=submission_types,
                file_content=file_content,
                file_name=file_name
            )
                
        except Exception as e:
            logger.error(f"Error processing assignment query: {str(e)}")
            return {"error": str(e)}