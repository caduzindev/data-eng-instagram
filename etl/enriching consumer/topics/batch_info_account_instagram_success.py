from typing import TypedDict
import asyncio
import logging

from core.utils.serialize import safe_get
from services.ollama_client import OllamaClient
from services.enrichment_service import EnrichmentService

logger = logging.getLogger(__name__)

class BatchInfoAccountInstagramSuccessMessage(TypedDict):
    account_sk: str

class BatchInfoAccountInstagramSuccessTopic:
    """Processes success messages from load_raw_data and enriches unprocessed data."""
    
    name = "batch_info_account_instagram_success"
    
    def __init__(self):
        # Initialize Ollama client and enrichment service
        self.ollama_client = OllamaClient()
        self.enrichment_service = EnrichmentService(
            ollama_client=self.ollama_client,
            batch_size=10  # Process 10 records at a time
        )
    
    async def execute(self, message: BatchInfoAccountInstagramSuccessMessage):
        """
        Main method that processes the message and enriches the data.
        Executes asynchronously to process comments and posts in parallel.
        """
        account_sk = safe_get(message, "account_sk")
        
        if not account_sk:
            logger.warning("Message received without account_sk, ignoring")
            return
        
        logger.info(f"Starting enrichment for account_sk: {account_sk}")
        
        try:
            # Process comments and posts in parallel
            comments_result, posts_result = await asyncio.gather(
                self.enrichment_service.process_comments(account_sk=account_sk, limit=100),
                self.enrichment_service.process_posts(account_sk=account_sk, limit=100),
                return_exceptions=True
            )
            
            # Log results
            if isinstance(comments_result, Exception):
                logger.error(f"Error processing comments: {comments_result}")
            else:
                logger.info(
                    f"Comments processed: {comments_result['processed']} successes, "
                    f"{comments_result['failed']} failures out of {comments_result['total']} total"
                )
            
            if isinstance(posts_result, Exception):
                logger.error(f"Error processing posts: {posts_result}")
            else:
                logger.info(
                    f"Posts processed: {posts_result['processed']} successes, "
                    f"{posts_result['failed']} failures out of {posts_result['total']} total"
                )
            
            logger.info(f"Enrichment completed for account_sk: {account_sk}")
            
        except Exception as e:
            logger.error(f"Error processing enrichment for account_sk {account_sk}: {e}")
            raise
