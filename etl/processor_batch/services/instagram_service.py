import asyncio
import json

from ..store.apify.instagram import instagram_apify
from ..store.apify.instagram_types import Post, Account
from ..store.gcs.storage import storage_gcs
from ..store.gcs.types import UploadFile, ValidExtension

from core.env import CoreEnv
from core.messaging.kafka.producer import send_message_topic
from ..store.utils import safe_get

class InstagramService:
  async def csv_batch_accounts_post_comments(self, file_content: bytes):
    lines = file_content.decode('utf-8').splitlines()
    if not lines or lines[0].strip() != 'account':
        print("Erro: Header inválido no processamento background")
        return
    lines.pop(0)

    chunk = 10
    actual_pointer = 0
    CONCURRENCY_LIMIT = 5
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    while actual_pointer < len(lines):
        lines_chunk = lines[actual_pointer:actual_pointer + chunk]

        tasks = [self.__get_account_details(acc, semaphore) for acc in lines_chunk]
        aggregate_result_tasks = await asyncio.gather(*tasks, return_exceptions=True)

        valid_results = [r for r in aggregate_result_tasks if not isinstance(r, Exception)]

        actual_pointer+=chunk

        storage_saved = storage_gcs.upload_file(UploadFile(
            bucket_name=CoreEnv().bucket_instagram,
            file_name=f'instagram_account_batch({actual_pointer})',
            extension=ValidExtension.JSON,
            buffer=json.dumps(valid_results)
          )
        )

        print("Lote salvo com sucesso", storage_saved.get("saved_path"))

        await send_message_topic(topic="batch_info_account_instagram", value={ "bucket_path": storage_saved.get("saved_path")})

  async def __get_account_details(self, account_name: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        account_detail = await instagram_apify.get_instagram_account_details(account_name)
        account_posts_comments = await instagram_apify.get_instagram_account_posts_and_comments(account_name)

        account_detail_map = [self.__get_account_detail_map(account) for account in account_detail]
        account_posts_comments_map = [self.__get_account_posts_comments_map(post) for post in account_posts_comments]

        return {
          "account": account_detail_map[0],
          "posts": account_posts_comments_map
        }

  def __get_account_detail_map(self, account: Account):
     return {
      "name": safe_get(account, "username"),
      "nick_name": safe_get(account,"fullName"),
      "url": safe_get(account,"url"),
      "followers_count": safe_get(account,"followersCount"),
      "follows_count": safe_get(account,"followsCount"),
      "is_business": safe_get(account,"isBusinessAccount"),
      "category": safe_get(account,"businessCategoryName"),
      "biography": safe_get(account,"biography")
     }

  def __get_account_posts_comments_map(self, post: Post):
    return {
      "shortCode": safe_get(post, "shortCode"),
      "caption": safe_get(post, "caption"),
      "hashtags": safe_get(post, "hashtags"),
      "audioUrl": safe_get(post, "audioUrl"),
      "musicInfo": {
          "artistName": safe_get(post, "musicInfo", "artistName"),
          "songName": safe_get(post, "musicInfo", "songName"),
      },
      "commentsCount": safe_get(post, "commentsCount"),
      "likesCount": safe_get(post, "likesCount"),
      "dimensions": {
          "height": safe_get(post, "dimensionsHeight"),
          "width": safe_get(post, "dimensionsWidth"),
      },
      "video": {
          "url": safe_get(post, "videoUrl"),
          "viewCount": safe_get(post, "videoViewCount"),
          "playCount": safe_get(post, "videoPlayCount"),
          "duration": safe_get(post, "videoDuration"),
      },
      "locationName": safe_get(post, "locationName"),
      "timestamp": safe_get(post, "timestamp"),
      "latest_comments": [
        {
          "text": safe_get(comment,'text'),
          "ownerUsername": safe_get(comment,'ownerUsername'),
          "ownerProfilePicUrl": safe_get(comment,'ownerProfilePicUrl'),
          "repliesCount": safe_get(comment,'repliesCount'),
          "likesCount": safe_get(comment,'likesCount'),
          "timestamp": safe_get(comment,'timestamp'),
        }
        for comment in post.get('latestComments')
      ],
    }

instagram_service = InstagramService()