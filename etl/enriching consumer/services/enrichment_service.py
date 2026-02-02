import asyncio
import logging
from typing import List, Dict, Any

from services.ollama_client import OllamaClient
from repositories.enrichment_repo import enrichment_repo

logger = logging.getLogger(__name__)

class EnrichmentService:
    """Main service for data enrichment using LLM."""
    
    def __init__(self, ollama_client: OllamaClient, batch_size: int = 10):
        self.ollama_client = ollama_client
        self.batch_size = batch_size
    
    async def process_comments(self, account_sk: str, limit: int = 100) -> Dict[str, Any]:
        """
        Processes unprocessed comments in async batches for a specific account.
        
        Returns:
            Dict with processing statistics
        """
        # Fetch unprocessed comments for this account
        comments = enrichment_repo.get_unprocessed_comments(account_sk=account_sk, limit=limit)
        
        if not comments:
            logger.info(f"No comments to process for account_sk: {account_sk}")
            return {"processed": 0, "failed": 0, "total": 0}
        
        logger.info(f"Starting processing of {len(comments)} comments for account_sk: {account_sk}")
        
        # Process in async batches
        processed = 0
        failed = 0
        
        for i in range(0, len(comments), self.batch_size):
            batch = comments[i:i + self.batch_size]
            logger.info(f"Processing batch {i // self.batch_size + 1} ({len(batch)} comments)")
            
            # Create async tasks for the batch
            tasks = [self._process_single_comment(comment) for comment in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successes and failures
            for result in results:
                if isinstance(result, Exception):
                    failed += 1
                    logger.error(f"Error processing comment: {result}")
                elif result:
                    processed += 1
                else:
                    failed += 1
        
        logger.info(f"Processing completed: {processed} successes, {failed} failures out of {len(comments)} total")
        return {"processed": processed, "failed": failed, "total": len(comments)}
    
    async def _process_single_comment(self, comment: Dict[str, Any]) -> bool:
        """Processes a single comment."""
        comment_sk = comment["comment_sk"]
        text = comment["text"]
        
        try:
            # Call Ollama to enrich
            enrichment = await self.ollama_client.enrich_comment(text)
            
            if not enrichment:
                logger.warning(f"Could not enrich comment {comment_sk}")
                return False
            
            # Validate and extract fields
            sentiment_label = self._validate_sentiment_label(enrichment.get("sentiment_label"))
            sentiment_score = self._validate_sentiment_score(enrichment.get("sentiment_score"))
            intent = self._validate_intent(enrichment.get("intent"))
            keywords = self._validate_keywords(enrichment.get("keywords"))
            
            # Update in BigQuery
            enrichment_repo.update_comment_enrichment(
                comment_sk=comment_sk,
                sentiment_label=sentiment_label,
                sentiment_score=sentiment_score,
                intent=intent,
                keywords=keywords
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing comment {comment_sk}: {e}")
            return False
    
    async def process_posts(self, account_sk: str, limit: int = 100) -> Dict[str, Any]:
        """
        Processes unprocessed posts in async batches for a specific account.
        
        Returns:
            Dict with processing statistics
        """
        # Fetch unprocessed posts for this account
        posts = enrichment_repo.get_unprocessed_posts(account_sk=account_sk, limit=limit)
        
        if not posts:
            logger.info(f"No posts to process for account_sk: {account_sk}")
            return {"processed": 0, "failed": 0, "total": 0}
        
        logger.info(f"Starting processing of {len(posts)} posts for account_sk: {account_sk}")
        
        # Process in async batches
        processed = 0
        failed = 0
        
        for i in range(0, len(posts), self.batch_size):
            batch = posts[i:i + self.batch_size]
            logger.info(f"Processing batch {i // self.batch_size + 1} ({len(batch)} posts)")
            
            # Create async tasks for the batch
            tasks = [self._process_single_post(post) for post in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successes and failures
            for result in results:
                if isinstance(result, Exception):
                    failed += 1
                    logger.error(f"Error processing post: {result}")
                elif result:
                    processed += 1
                else:
                    failed += 1
        
        logger.info(f"Processing completed: {processed} successes, {failed} failures out of {len(posts)} total")
        return {"processed": processed, "failed": failed, "total": len(posts)}
    
    async def _process_single_post(self, post: Dict[str, Any]) -> bool:
        """Processes a single post."""
        post_sk = post["post_sk"]
        caption = post["caption"]
        
        try:
            # Call Ollama to enrich
            enrichment = await self.ollama_client.enrich_post(caption)
            
            if not enrichment:
                logger.warning(f"Could not enrich post {post_sk}")
                return False
            
            # Validate and extract fields
            content_topic = self._validate_content_topic(enrichment.get("content_topic"))
            tone = enrichment.get("tone")
            call_to_action_type = enrichment.get("call_to_action_type")
            
            # Update in BigQuery
            enrichment_repo.update_post_enrichment(
                post_sk=post_sk,
                content_topic=content_topic,
                tone=tone,
                call_to_action_type=call_to_action_type
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing post {post_sk}: {e}")
            return False
    
    def _validate_sentiment_label(self, value: Any) -> str:
        """Validates and normalizes sentiment_label."""
        valid_labels = {"positive", "negative", "neutral", "mixed"}
        if isinstance(value, str) and value.lower() in valid_labels:
            return value.lower()
        return "neutral"  # Default
    
    def _validate_sentiment_score(self, value: Any) -> float:
        """Validates and normalizes sentiment_score."""
        try:
            score = float(value)
            # Ensure it's in range [-1.0, 1.0]
            return max(-1.0, min(1.0, score))
        except (ValueError, TypeError):
            return 0.0  # Default neutral
    
    def _validate_intent(self, value: Any) -> str:
        """Validates and normalizes intent."""
        valid_intents = {"praise", "complaint", "question", "mention", "spam", "other"}
        if isinstance(value, str) and value.lower() in valid_intents:
            return value.lower()
        return "other"  # Default
    
    def _validate_keywords(self, value: Any) -> List[str]:
        """Validates and normalizes keywords."""
        if isinstance(value, list):
            return [str(kw).strip() for kw in value if kw and str(kw).strip()]
        return []
    
    def _validate_content_topic(self, value: Any) -> str:
        """Validates and normalizes content_topic."""
        valid_topics = {"sales", "educational", "lifestyle", "humor"}
        if isinstance(value, str) and value.lower() in valid_topics:
            return value.lower()
        return "lifestyle"  # Default
