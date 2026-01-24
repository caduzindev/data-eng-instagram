from typing import List, TypedDict
import json
from datetime import datetime
from uuid import uuid4

from core.infra.gcs.storage import storage_gcs
from core.entities.instagram import DimInstagramAccount, DimInstagramPost, DimInstagramComment, DimDate
from core.entities.pre_file_instagram import PostCommentsMap, AccountDetail
from core.repositories.bigquery.dim_instagram_account_repo import dim_instagram_account_repo
from core.repositories.bigquery.dim_instagram_post_repo import dim_instagram_post_repo
from core.repositories.bigquery.dim_instagram_comment_repo import dim_instagram_comment_repo
from core.repositories.bigquery.dim_date_repo import dim_date_repo
from core.utils.serialize import safe_get

class BatchInfoAccountInstagramMessage(TypedDict):
  bucket_path: str

class BatchInfoAccountInstagramTopic:
  name = "batch_info_account_instagram"

  def execute(self, message: BatchInfoAccountInstagramMessage):
    bucket_path = safe_get(message, "bucket_path")
    file_content = storage_gcs.download_file(bucket_name=bucket_path.split("/")[0], file_name=bucket_path.split("/")[1])

    json_data = json.loads(file_content.decode("utf-8"))

    for data in json_data:
      account: AccountDetail = safe_get(data, "account")
      posts: List[PostCommentsMap] = safe_get(data, "posts")

      account_sk = str(uuid4())
      dim_instagram_account_repo.save([DimInstagramAccount(
        account_sk=account_sk,
        name=safe_get(account, "name"),
        nickname=safe_get(account, "nick_name"),
        url=safe_get(account, "url"),
      )])
      for post in posts:
        date_sk = str(uuid4())
        date = datetime.fromtimestamp(datetime.fromisoformat(safe_get(post, "timestamp").replace('Z', '+00:00')).timestamp())
        dim_date_repo.save([DimDate(
          date_sk=date_sk,
          date=date.strftime("%Y-%m-%d"),
          day=date.day,
          month=date.month,
          year=date.year,
          weekday=date.strftime("%A"),
          is_weekend=date.weekday() in [5, 6],
        )])

        post_sk = str(uuid4())
        music_info = safe_get(post, "musicInfo")
        video = safe_get(post, "video")
        dimensions = safe_get(post, "dimensions")
        
        dim_instagram_post_repo.save([DimInstagramPost(
          post_sk=post_sk,
          account_sk=account_sk,
          date_sk=date_sk,
          external_code=safe_get(post, "shortCode"),
          caption=safe_get(post, "caption"),
          hash_tags=safe_get(post, "hashtags") or [],
          audio_url=safe_get(post, "audioUrl"),
          music_name=safe_get(music_info, "songName"),
          owner_music_name=safe_get(music_info, "artistName"),
          video_url=safe_get(video, "url"),
          video_duration=safe_get(video, "duration"),
          dim_height=safe_get(dimensions, "height"),
          dim_width=safe_get(dimensions, "width"),
          location=safe_get(post, "locationName"),
        )])
        latest_comments = safe_get(post, "latest_comments") or []
        for comment in latest_comments:
          dim_instagram_comment_repo.save([DimInstagramComment(
            comment_sk=str(uuid4()),
            post_sk=post_sk,
            account_sk=account_sk,
            owner_username=safe_get(comment, "ownerUsername"),
            date_sk=date_sk,
          )])
      print('--------------------------------')
      print(f'{safe_get(account, "name")} - {safe_get(post, "shortCode")} saved')
      print('--------------------------------')
