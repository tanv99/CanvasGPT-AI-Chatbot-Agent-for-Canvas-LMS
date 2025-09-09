from .base import CanvasBaseAgent
from .announcement import AnnouncementAgent
from .assignment import AssignmentAgent
from .quiz import QuizAgent
from typing import Dict, Any, Optional, List
import logging
import aiohttp
import re
from langchain_openai import ChatOpenAI
from .Pages import PagesAgent

logger = logging.getLogger(__name__)

class CanvasPostAgent:
    """Main agent for Canvas operations with improved direct posting capabilities"""
    
    def __init__(self, canvas_api_key: str, canvas_base_url: str):
        self.api_key = canvas_api_key
        self.base_url = canvas_base_url.rstrip('/')
        self.session = None
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
        # Initialize sub-agents
        self.announcement_agent = AnnouncementAgent(self.api_key, self.base_url)
        self.assignment_agent = AssignmentAgent(self.api_key, self.base_url)
        self.quiz_agent = QuizAgent(self.api_key, self.base_url)
        self.pages_agent = PagesAgent(self.api_key, self.base_url)
        self.llm = ChatOpenAI()  # For title generation if needed

    def parse_structured_quiz(self, content: str) -> List[Dict[str, Any]]:
        """Parse structured quiz content"""
        try:
            questions = []
            # Split into questions (now handling repeated question numbers)
            question_blocks = re.split(r'\d+\.\s+', content)
            question_blocks = [q.strip() for q in question_blocks if q.strip()]

            for block in question_blocks:
                try:
                    # Extract question text
                    question_parts = block.split('A.')
                    if not question_parts:
                        continue
                    question_text = question_parts[0].strip()
                    
                    # Extract options
                    options_text = 'A.' + 'A.'.join(question_parts[1:])
                    options = []
                        
                        # Extract each option
                    for letter in ['A', 'B', 'C', 'D']:
                        pattern = fr'{letter}\.\s*([^A-D\(]+)'
                        match = re.search(pattern, options_text)
                        if match:
                                option_text = match.group(1).strip()
                                options.append((letter, option_text))

                        # Extract correct answer
                        correct_match = re.search(r'\(Correct Answer:\s*([A-D])\)', block)
                        if not correct_match:
                            continue
                        
                        correct_letter = correct_match.group(1)

                        # Format for Canvas
                    canvas_answers = []
                    for letter, text in options:
                        canvas_answers.append({
                                "text": text,
                                "weight": 100 if letter == correct_letter else 0
                        })

                    # Create question dictionary
                    question_dict = {
                        "question_name": question_text[:50],  # Canvas title length limit
                         "question_text": question_text,
                         "question_type": "multiple_choice_question",
                        "points_possible": 1,
                        "answers": canvas_answers
                    }

                    questions.append(question_dict)

                except Exception as e:
                    logger.error(f"Error parsing question block: {str(e)}")
                    continue

            return questions

        except Exception as e:
            logger.error(f"Error parsing quiz content: {str(e)}")
            return []

    async def handle_structured_quiz(self, course_id: str, title: str, content: str) -> Dict[str, Any]:
        """Handle creation of structured quiz"""
        try:
            # Parse questions
            questions = self.parse_structured_quiz(content)
            
            if not questions:
                return {
                    "success": False,
                    "message": "No valid questions could be parsed from the content"
                }

            # Create quiz
            quiz = await self.quiz_agent.create_quiz(
                course_id=course_id,
                title=title,
                description="Quiz with provided questions",
                quiz_type='assignment',
                time_limit=30,
                points_possible=len(questions)
            )
            
            if 'error' in quiz:
                return {
                    "success": False,
                    "message": f"Error creating quiz: {quiz['error']}"
                }
            
            # Add questions
            quiz_id = quiz['id']
            for question in questions:
                result = await self.quiz_agent.add_question(course_id, quiz_id, question)
                if 'error' in result:
                    logger.error(f"Error adding question: {result['error']}")
                    # Continue with other questions even if one fails
            
            return {
                "success": True,
                "message": f"Successfully created quiz with {len(questions)} questions",
                "quiz_id": quiz_id,
                "question_count": len(questions)
            }

        except Exception as e:
            logger.error(f"Error handling structured quiz: {str(e)}")
            return {
                "success": False,
                "message": f"Error creating structured quiz: {str(e)}"
            }


    async def _ensure_session(self):
        """Ensure aiohttp session is created"""
        if not self.session:
            self.session = aiohttp.ClientSession()

    def _extract_title(self, message: str) -> Optional[str]:
        """Extract title from message if specified with 'title:' prefix"""
        try:
            if "title:" in message.lower():
                title_start = message.lower().index("title:") + 6
                title_end = message.find("\n", title_start)
                if title_end == -1:
                    title_end = len(message)
                return message[title_start:title_end].strip()
            return None
        except Exception as e:
            logger.error(f"Error extracting title: {str(e)}")
            return None
        
    def parse_submission_types(self, message: str) -> List[str]:
        """Parse submission types from message"""
        submission_types = []
        message_lower = message.lower()

        # Map of keywords to Canvas submission types
        type_mapping = {
            "text entry": ["online_text_entry"],
            "website url": ["online_url"],
            "file upload": ["online_upload"],
            "media recording": ["media_recording"],
            "student annotation": ["student_annotation"],
            "external tool": ["external_tool"],
            "no submission": ["none"],
            "on paper": ["on_paper"],
            "online": ["online_text_entry", "online_url", "online_upload", "media_recording"]
        }

        # Check for each submission type in the message
        for keyword, types in type_mapping.items():
            if keyword in message_lower:
                submission_types.extend(types)

        # Default to online text entry if no type specified
        if not submission_types:
            submission_types = ["online_text_entry"]

        return list(set(submission_types))

    def _extract_link(self, message: str) -> Optional[str]:
        """Extract link from message if specified with 'link:' prefix or contains URL"""
        try:
            # First try to find explicit link: prefix
            if "link:" in message.lower():
                link_start = message.lower().index("link:") + 5
                link_end = message.find(" ", link_start)
                if link_end == -1:
                    link_end = len(message)
                return message[link_start:link_end].strip()
            
            # If no explicit link:, try to find URL pattern
            url_pattern = r'https?://[^\s<>"\']+|www\.[^\s<>"\']+'
            urls = re.findall(url_pattern, message)
            if urls:
                return urls[0]
                
            return None
        except Exception as e:
            logger.error(f"Error extracting link: {str(e)}")
            return None

    async def _generate_title(self, content: str) -> str:
        """Generate a title from content using LLM"""
        try:
            prompt = f"""
            Generate a brief, descriptive title (maximum 5 words) for the following content:
            {content[:500]}
            """
            title = await self.llm.apredict(prompt)
            return title.strip()
        except Exception as e:
            logger.error(f"Error generating title: {str(e)}")
            return "Generated Content"


    async def process(self, content: str, message: str, file_content: bytes = None, file_name: str = None) -> Dict[str, Any]:
        """Process Canvas operations based on message content"""
        try:
            await self._ensure_session()
            
            # Extract course name
            course_match = re.search(r'\[(.*?)\]', message)
            if not course_match:
                return {
                    "success": False,
                    "message": "Please specify a course name in square brackets, e.g. [Course Name]"
                }

            course_name = course_match.group(1)
            course_id = await self.get_course_id(course_name)

            if not course_id:
                return {
                    "success": False,
                    "message": f"Could not find course: {course_name}"
                }

            # Extract title
            title = self._extract_title(message)
            if not title:
                title = "Quiz"  # Default title if none provided

            logger.info(f"Processing {course_name} with title: {title}")

            # Handle structured quiz format
            if "Questions" in content and "(Correct Answer:" in content:
                logger.info(f"Creating structured quiz in course {course_name}")
                return await self.handle_structured_quiz(course_id, title, content)


            # Extract title and link if present
            title = self._extract_title(message)
            link = self._extract_link(message)
            
            # Handle direct link posts
            if link:
                content = f'<p><a href="{link}" target="_blank">{link}</a></p>'
                if not title:
                    title = "Shared Link"
            elif not isinstance(content, dict) and not content:
                return {
                    "success": False,
                    "message": "No content or link provided"
                }

            # Get title if still not set
            if not title:
                title = await self._generate_title(content if isinstance(content, str) else "File Upload")

            logger.info(f"Processing {course_name} with title: {title}")

            # Determine content type and process accordingly
            result = None
            try:
                
                
                
                
                
                
                
                # Check for specific content types and handle accordingly
                message_lower = message.lower()
                if "quiz" in message.lower() and "**Questions" in content:
                    logger.info(f"Creating structured quiz in course {course_name}")
                    return await self.handle_structured_quiz(course_id, title, content)
                elif "quiz" in message.lower():
                    logger.info(f"Creating quiz in course {course_name}")
                    quiz_questions = await self._generate_quiz_questions(content)
                    quiz = await self.quiz_agent.create_quiz(
                        course_id=course_id,
                        title=title,
                        description="Quiz generated based on provided content",
                        quiz_type='assignment',
                        time_limit=30
                    )
                    
                    if 'error' in quiz:
                        return {
                            "success": False,
                            "message": f"Error creating quiz: {quiz['error']}"
                        }
                    
                    quiz_id = quiz['id']
                    for question in quiz_questions:
                        await self.quiz_agent.add_question(course_id, quiz_id, question)
                    
                    result = {
                        "quiz_id": quiz_id,
                        "question_count": len(quiz_questions),
                        "quiz_data": quiz
                    }
                    
                # Handle page creation first to prevent fallback to announcements
                if "page" in message_lower or "as a page" in message_lower:
                    logger.info(f"Creating page in course {course_name}")
                    if hasattr(self, 'pages_agent'):
                        # Extract text content if present
                        text_match = re.search(r'Text:\s*"([^"]+)"', message) or \
                                    re.search(r'Text:\s*\'([^\']+)\'', message) or \
                                    re.search(r'Text:\s*([^\n]+)', message)
                        
                        body = text_match.group(1).strip() if text_match else content
                        logger.info(f"Extracted content for page: {body}")
                        
                        return await self.pages_agent.create_page(
                            course_id=course_id,
                            title=title,
                            body=body,
                            published=True
                        )
                    else:
                        return {
                            "success": False,
                            "message": "Page creation functionality not available"
                        }
                        
                # Handle assignment creation
                elif "assignment" in message_lower:
                    logger.info(f"Creating assignment in course {course_name}")
                    # ... (rest of your assignment handling code remains the same)
                    
                # Handle quiz creation
                elif "quiz" in message_lower:
                    logger.info(f"Creating quiz in course {course_name}")
                    # ... (rest of your quiz handling code remains the same)

                # Handle announcements (default fallback)
                else:
                    logger.info(f"Creating announcement in course {course_name}")
                    
                    # If we have file_content passed directly
                    if file_content and file_name:
                        # Create announcement with file
                        result = await self.announcement_agent.create_announcement(
                            course_id=course_id,
                            title=title,
                            message=content if isinstance(content, str) else "File uploaded",
                            is_published=True,
                            file_content=file_content,
                            file_name=file_name
                        )
                    # If we have content as a dictionary with file information
                    elif isinstance(content, dict) and content.get('file_content'):
                        # Create announcement with file from content dictionary
                        result = await self.announcement_agent.create_announcement(
                            course_id=course_id,
                            title=title,
                            message=content.get('text', 'File uploaded'),
                            is_published=True,
                            file_content=content['file_content'],
                            file_name=content.get('filename', 'uploaded_file')
                        )
                    else:
                        # Create regular announcement
                        result = await self.announcement_agent.create_announcement(
                            course_id=course_id,
                            title=title,
                            message=content
                        )

                    if isinstance(result, dict) and "error" in result:
                        return {
                            "success": False,
                            "message": str(result["error"])
                        }

                    return {
                        "success": True,
                        "message": f"Successfully posted to {course_name}",
                        "details": result
                    }

            except Exception as e:
                logger.error(f"Error in content creation: {str(e)}")
                return {
                    "success": False,
                    "message": f"Error in content creation: {str(e)}"
                }

        except Exception as e:
            logger.error(f"Error processing Canvas post: {str(e)}")
            return {
                "success": False,
                "message": f"Error processing request: {str(e)}"
            }

    async def _generate_quiz_questions(self, content: str) -> List[Dict[str, Any]]:
        """Generate quiz questions from content"""
        try:
            prompt = f"""
            Based on the following content, generate 5 multiple-choice questions.
            Each question should:
            - Test key concepts from the content
            - Have 4 options with one correct answer
            - Include a brief explanation for the correct answer
            
            Format each question as a dictionary with:
            - question_text: The question
            - answers: List of 4 dictionaries, each with 'text' and 'correct' (boolean)
            - explanation: Brief explanation of the correct answer
            
            Content:
            {content[:4000]}
            """
            
            response = await self.llm.apredict(prompt)
            questions = []
            for q in eval(response):
                formatted_q = {
                    "question_name": q['question_text'][:50],
                    "question_text": q['question_text'],
                    "question_type": "multiple_choice_question",
                    "points_possible": 1,
                    "answers": [
                        {
                            "text": ans['text'],
                            "weight": 100 if ans['correct'] else 0
                        }
                        for ans in q['answers']
                    ],
                    "correct_comments": q.get('explanation', 'Correct!'),
                    "incorrect_comments": "Please review the material and try again."
                }
                questions.append(formatted_q)
            
            return questions
            
        except Exception as e:
            logger.error(f"Error generating quiz questions: {str(e)}")
            return self._get_fallback_questions()

    def _get_fallback_questions(self) -> List[Dict[str, Any]]:
        """Generate basic fallback questions if main generation fails"""
        return [{
            "question_name": "Basic Understanding",
            "question_text": "What is the main topic discussed in the content?",
            "question_type": "multiple_choice_question",
            "points_possible": 1,
            "answers": [
                {"text": "Main topic", "weight": 100},
                {"text": "Alternative 1", "weight": 0},
                {"text": "Alternative 2", "weight": 0},
                {"text": "Alternative 3", "weight": 0}
            ]
        }]

    async def list_courses(self) -> List[Dict[str, Any]]:
        """Get list of all available courses"""
        try:
            await self._ensure_session()
            async with self.session.get(
                f"{self.base_url}/api/v1/courses",
                headers=self.headers,
                params={
                    'enrollment_type': 'teacher',
                    'state[]': ['available', 'completed', 'created'],
                    'include[]': ['term', 'total_students']
                }
            ) as response:
                if response.status == 200:
                    courses = await response.json()
                    return [{
                        'id': course.get('id'),
                        'name': course.get('name'),
                        'code': course.get('course_code'),
                        'term': course.get('term', {}).get('name'),
                        'students': course.get('total_students', 0)
                    } for course in courses]
                logger.error(f"Error listing courses: Status {response.status}")
                return []
        except Exception as e:
            logger.error(f"Error listing courses: {str(e)}")
            return []

    async def get_course_id(self, course_name: str) -> Optional[str]:
        """Get Canvas course ID from course name"""
        try:
            courses = await self.list_courses()
            for course in courses:
                if course_name.lower() in course['name'].lower():
                    return str(course['id'])
            return None
        except Exception as e:
            logger.error(f"Error getting course ID: {str(e)}")
            return None

    async def close(self):
        """Close the session and all sub-agent sessions"""
        try:
            if self.session:
                await self.session.close()
                self.session = None
            
            await self.announcement_agent.close()
            await self.assignment_agent.close()
            await self.quiz_agent.close()
            logger.info("All sessions closed successfully")
        except Exception as e:
            logger.error(f"Error closing sessions: {str(e)}")