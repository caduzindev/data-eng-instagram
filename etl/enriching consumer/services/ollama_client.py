import aiohttp
import json
from typing import Dict, Any, Optional
import logging

from core.env import CoreEnv

logger = logging.getLogger(__name__)

class OllamaClient:
    """Async client for communication with Ollama API."""
    
    def __init__(self):
        env = CoreEnv()
        self.base_url = env.ollama_base_url
        self.model = env.ollama_model
        self.api_url = f"{self.base_url}/api/generate"
    
    async def generate_json(self, prompt: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        Sends a prompt to Ollama and returns the response as JSON.
        
        Args:
            prompt: The prompt to send
            max_retries: Maximum number of retry attempts on error
            
        Returns:
            Dictionary with JSON response or None on error
        """
        payload = {
            "model": self.model,
            "prompt": prompt,
            "format": "json",
            "stream": False
        }
        
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.api_url,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=60)
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            response_text = result.get("response", "")
                            
                            # Try to parse JSON
                            try:
                                # Remove markdown code blocks if they exist
                                response_text = response_text.strip()
                                if response_text.startswith("```json"):
                                    response_text = response_text[7:]
                                if response_text.startswith("```"):
                                    response_text = response_text[3:]
                                if response_text.endswith("```"):
                                    response_text = response_text[:-3]
                                response_text = response_text.strip()
                                
                                return json.loads(response_text)
                            except json.JSONDecodeError as e:
                                logger.warning(f"Error parsing JSON (attempt {attempt + 1}/{max_retries}): {e}")
                                logger.warning(f"Received response: {response_text[:200]}")
                                if attempt < max_retries - 1:
                                    continue
                                return None
                        else:
                            error_text = await response.text()
                            logger.error(f"HTTP error {response.status}: {error_text}")
                            if attempt < max_retries - 1:
                                continue
                            return None
                            
            except aiohttp.ClientError as e:
                logger.error(f"Connection error with Ollama (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                return None
            except Exception as e:
                logger.error(f"Unexpected error calling Ollama (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                return None
        
        return None
    
    async def enrich_comment(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Enriches a comment with sentiment analysis and intent detection.
        
        Returns:
            Dict with: sentiment_label, sentiment_score, intent, keywords
        """
        prompt = f"""Analyze the following Instagram comment and return a JSON with:
- sentiment_label: "positive", "negative", "neutral" or "mixed"
- sentiment_score: float number between -1.0 and 1.0
- intent: "praise", "complaint", "question", "mention", "spam" or "other"
- keywords: list of strings with main topics mentioned

Comment: {text}

Return ONLY the JSON, without additional text."""

        return await self.generate_json(prompt)
    
    async def enrich_post(self, caption: str) -> Optional[Dict[str, Any]]:
        """
        Enriches a post with content analysis.
        
        Returns:
            Dict with: content_topic, tone, call_to_action_type
        """
        prompt = f"""Analyze the following Instagram post caption and return a JSON with:
- content_topic: "sales", "educational", "lifestyle" or "humor"
- tone: string describing the tone (e.g., "professional", "urgent", "funny")
- call_to_action_type: string describing the call to action type (e.g., "link_bio", "comment", "none")

Caption: {caption}

Return ONLY the JSON, without additional text."""

        return await self.generate_json(prompt)
