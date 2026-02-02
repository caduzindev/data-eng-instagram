from google.cloud import bigquery
from typing import List, Dict, Any, Optional
import logging

from core.db.bigquery import bigquery_client
from core.env import CoreEnv

logger = logging.getLogger(__name__)

class EnrichmentRepo:
    """Repository for enrichment operations in BigQuery."""
    
    dataset_id = "instagram_data"
    
    def __init__(self):
        self.project_id = CoreEnv().gcp_project_id
    
    def _get_table_id(self, table_name: str) -> str:
        """Returns the full table ID."""
        return f"{self.project_id}.{self.dataset_id}.{table_name}"
    
    def get_unprocessed_comments(self, account_sk: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetches unprocessed comments for a specific account."""
        query = f"""
        SELECT 
            comment_sk,
            text
        FROM `{self._get_table_id("fact_instagram_comment_metrics")}`
        WHERE account_sk = @account_sk
          AND text IS NOT NULL
          AND text != ''
          AND enrichment_sentiment_label IS NULL
        LIMIT {limit}
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("account_sk", "STRING", account_sk),
            ]
        )
        
        try:
            query_job = bigquery_client.query(query, job_config=job_config)
            results = query_job.result()
            
            records = []
            for row in results:
                records.append({
                    "comment_sk": row.comment_sk,
                    "text": row.text
                })
            
            logger.info(f"Found {len(records)} unprocessed comments for account_sk: {account_sk}")
            return records
            
        except Exception as e:
            logger.error(f"Error fetching unprocessed comments: {e}")
            raise
    
    def get_unprocessed_posts(self, account_sk: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetches unprocessed posts for a specific account."""
        query = f"""
        SELECT 
            post_sk,
            caption
        FROM `{self._get_table_id("dim_instagram_post")}`
        WHERE account_sk = @account_sk
          AND caption IS NOT NULL
          AND caption != ''
          AND enrichment_content_topic IS NULL
        LIMIT {limit}
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("account_sk", "STRING", account_sk),
            ]
        )
        
        try:
            query_job = bigquery_client.query(query, job_config=job_config)
            results = query_job.result()
            
            records = []
            for row in results:
                records.append({
                    "post_sk": row.post_sk,
                    "caption": row.caption
                })
            
            logger.info(f"Found {len(records)} unprocessed posts for account_sk: {account_sk}")
            return records
            
        except Exception as e:
            logger.error(f"Error fetching unprocessed posts: {e}")
            raise
    
    def update_comment_enrichment(
        self, 
        comment_sk: str, 
        sentiment_label: Optional[str],
        sentiment_score: Optional[float],
        intent: Optional[str],
        keywords: Optional[List[str]]
    ) -> None:
        """Updates enrichment data for a comment."""
        query = f"""
        UPDATE `{self._get_table_id("fact_instagram_comment_metrics")}`
        SET 
            enrichment_sentiment_label = @sentiment_label,
            enrichment_sentiment_score = @sentiment_score,
            enrichment_intent = @intent,
            enrichment_keywords = @keywords
        WHERE comment_sk = @comment_sk
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sentiment_label", "STRING", sentiment_label),
                bigquery.ScalarQueryParameter("sentiment_score", "FLOAT", sentiment_score),
                bigquery.ScalarQueryParameter("intent", "STRING", intent),
                bigquery.ArrayQueryParameter("keywords", "STRING", keywords or []),
                bigquery.ScalarQueryParameter("comment_sk", "STRING", comment_sk),
            ]
        )
        
        try:
            query_job = bigquery_client.query(query, job_config=job_config)
            query_job.result()  # Wait for completion
            logger.debug(f"Comment {comment_sk} updated successfully")
        except Exception as e:
            logger.error(f"Error updating comment enrichment {comment_sk}: {e}")
            raise
    
    def update_post_enrichment(
        self,
        post_sk: str,
        content_topic: Optional[str],
        tone: Optional[str],
        call_to_action_type: Optional[str]
    ) -> None:
        """Updates enrichment data for a post."""
        query = f"""
        UPDATE `{self._get_table_id("dim_instagram_post")}`
        SET 
            enrichment_content_topic = @content_topic,
            enrichment_tone = @tone,
            enrichment_call_to_action_type = @call_to_action_type
        WHERE post_sk = @post_sk
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("content_topic", "STRING", content_topic),
                bigquery.ScalarQueryParameter("tone", "STRING", tone),
                bigquery.ScalarQueryParameter("call_to_action_type", "STRING", call_to_action_type),
                bigquery.ScalarQueryParameter("post_sk", "STRING", post_sk),
            ]
        )
        
        try:
            query_job = bigquery_client.query(query, job_config=job_config)
            query_job.result()  # Wait for completion
            logger.debug(f"Post {post_sk} updated successfully")
        except Exception as e:
            logger.error(f"Error updating post enrichment {post_sk}: {e}")
            raise

enrichment_repo = EnrichmentRepo()
