from os import getenv
from typing import List, Dict, Any, Optional
from apify_client import ApifyClientAsync
from apify_client.clients.resource_clients import ActorClient, DatasetClient

from .instagram_types import Post
from core.env import CoreEnv

INSTAGRAM_BASE_URL = "https://www.instagram.com"
RESULTS_TYPE_POSTS = "posts"
RESULTS_TYPE_DETAILS = "details"
DEFAULT_RESULTS_LIMIT = 10
DEFAULT_SEARCH_LIMIT = 1

def _get_apify_token() -> str:
    token = CoreEnv().apify_token
    if not token:
        raise ValueError("APIFY_TOKEN não está configurado nas variáveis de ambiente")
    return token

def _get_actor_id() -> str:
    actor_id = CoreEnv().actor_instagram
    if not actor_id:
        raise ValueError("ACTOR_INSTAGRAM não está configurado nas variáveis de ambiente")
    return actor_id

def _create_apify_client() -> ApifyClientAsync:
    return ApifyClientAsync(_get_apify_token())

client_apify = _create_apify_client()

class InstagramApiFy:    
    def __init__(self, client: Optional[ApifyClientAsync] = None):
        self._client = client or client_apify
        self._actor_id = _get_actor_id()
    
    def _build_profile_url(self, account_name: str) -> str:
        if not account_name or not account_name.strip():
            raise ValueError("account_name não pode estar vazio")
        
        account_name = account_name.strip().replace('@', '')
        
        return f"{INSTAGRAM_BASE_URL}/{account_name}/"
    
    def _build_run_input(
        self,
        profile_url: str,
        results_type: str,
        results_limit: int = DEFAULT_RESULTS_LIMIT
    ) -> Dict[str, Any]:
        return {
            "addParentData": False,
            "directUrls": [profile_url],
            "enhanceUserSearchWithFacebookPage": False,
            "isUserReelFeedURL": False,
            "isUserTaggedFeedURL": False,
            "resultsLimit": results_limit,
            "resultsType": results_type,
            "searchLimit": DEFAULT_SEARCH_LIMIT,
            "searchType": "hashtag"
        }
    
    async def _fetch_instagram_data(
        self,
        account_name: str,
        results_type: str,
        results_limit: int = DEFAULT_RESULTS_LIMIT
    ) -> List[Dict[str, Any]]:
        profile_url = self._build_profile_url(account_name)
        run_input = self._build_run_input(profile_url, results_type, results_limit)
        
        try:
            actor_client: ActorClient = self._client.actor(self._actor_id)
            run_result = await actor_client.call(run_input=run_input)
            
            if 'defaultDatasetId' not in run_result:
                raise RuntimeError(
                    f"Resposta do Apify não contém 'defaultDatasetId'. "
                    f"Resposta: {run_result}"
                )
            
            dataset_id = run_result['defaultDatasetId']
            dataset: DatasetClient = self._client.dataset(dataset_id)

            items: List[Dict[str, Any]] = []

            async for item in dataset.iterate_items():
                items.append(item)

            return items
            
        except Exception as e:
            raise RuntimeError(
                f"Erro ao buscar dados do Instagram para a conta '{account_name}': {str(e)}"
            ) from e
    
    async def get_instagram_account_posts_and_comments(
        self, 
        account_name: str,
        results_limit: int = DEFAULT_RESULTS_LIMIT
    ) -> List[Post]:
        results = await self._fetch_instagram_data(
            account_name, 
            RESULTS_TYPE_POSTS,
            results_limit
        )
        return results
  
    async def get_instagram_account_details(
        self, 
        account_name: str,
        results_limit: int = DEFAULT_RESULTS_LIMIT
    ) -> List[Dict[str, Any]]:
        return await self._fetch_instagram_data(
            account_name, 
            RESULTS_TYPE_DETAILS,
            results_limit
        )

instagram_apify = InstagramApiFy()