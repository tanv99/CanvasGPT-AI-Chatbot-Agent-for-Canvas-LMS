import aiohttp
import logging
from bs4 import BeautifulSoup
from langchain_openai import ChatOpenAI
from duckduckgo_search import DDGS
import asyncio
import re
import json
import time
from typing import Dict, Any, Optional, List
from urllib.parse import unquote

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WebSearchAgent:
    def __init__(self):
        self.session = None
        self.llm = ChatOpenAI()
        self.last_search_time = 0
        self.min_search_interval = 1
        logger.info("Web Search Agent initialized")

    async def _ensure_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    def _extract_url(self, query: str) -> tuple[Optional[str], str]:
        """Extract URL and query from input"""
        if 'link:' in query:
            url_pattern = r'link:([^\s]+)'
            url_match = re.search(url_pattern, query)
            if url_match:
                url = unquote(url_match.group(1))
                remaining_query = query.replace(f"link:{url_match.group(1)}", "").strip()
                return url, remaining_query
        return None, query

    async def get_page_content(self, url: str) -> Optional[str]:
        """Fetch webpage content"""
        try:
            await self._ensure_session()
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            async with self.session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Remove unwanted elements
                    for tag in soup(['script', 'style', 'nav', 'iframe']):
                        tag.decompose()
                    
                    # Find main content
                    for selector in ['article', 'main', '.content', '#content']:
                        main_content = soup.select_one(selector)
                        if main_content:
                            return main_content.get_text('\n', strip=True)
                    
                    return soup.get_text('\n', strip=True)
                return None
        except Exception as e:
            logger.error(f"Error fetching URL {url}: {str(e)}")
            return None

    def perform_web_search(self, query: str) -> List[Dict[str, str]]:
        """Perform web search using DuckDuckGo"""
        try:
            results = []
            with DDGS() as ddgs:
                for r in ddgs.text(query, max_results=5):
                    results.append({
                        'title': r['title'],
                        'link': r['link'],
                        'snippet': r['body']
                    })
            return results
        except Exception as e:
            logger.error(f"Search error: {str(e)}")
            return []

    async def process(self, query: str, conversation_context: Optional[str] = None) -> str:
        """Process queries for both URL extraction and web search"""
        try:
            # First check for URL extraction
            url, remaining_query = self._extract_url(query)
            
            if url:
                # Handle URL content extraction
                content = await self.get_page_content(url)
                if not content:
                    return "I couldn't access the webpage content. Please check if the URL is correct and accessible."

                prompt = f"""
                Find this specific information: "{remaining_query}"
                From this content: {content[:4000]}
                Provide only the relevant information requested.
                """
                response = await self.llm.apredict(prompt)
                return f"{response}\n\nSource: {url}"
            
            else:
                # Handle web search
                search_results = self.perform_web_search(query)
                if not search_results:
                    return "I couldn't find any relevant information for your query."

                # Gather content from top results
                search_content = []
                for result in search_results[:3]:
                    search_content.append({
                        'title': result['title'],
                        'content': result['snippet'],
                        'url': result['link']
                    })

                prompt = f"""
                Based on these recent search results about "{query}":
                {json.dumps(search_content, indent=2)}

                Provide a comprehensive summary of the latest information.
                Include specific details and developments.
                Format key points clearly.
                """

                response = await self.llm.apredict(prompt)
                sources = "\n\nSources:\n" + "\n".join(
                    f"- {result['title']}" 
                    for result in search_content
                )
                
                return f"{response}{sources}"

        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return "I encountered an error while processing your request. Please try again."

    async def close(self):
        """Clean up resources"""
        if self.session:
            await self.session.close()
            self.session = None