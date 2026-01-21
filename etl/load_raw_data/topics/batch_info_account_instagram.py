from typing import List, TypedDict
import json
from datetime import datetime
from uuid import uuid4

from core.infra.gcs.storage import storage_gcs
from core.entities.instagram import DimInstagramAccount, DimInstagramPost, DimInstagramComment, DimDate
from core.entities.pre_file_instagram import PostCommentsMap, AccountDetail
from core.repositories.bigquery import dim_instagram_account_repo, dim_instagram_post_repo, dim_instagram_comment_repo, dim_date_repo

class BatchInfoAccountInstagramMessage(TypedDict):
  bucket_path: str

class BatchInfoAccountInstagramTopic:
  name = "batch_info_account_instagram"

  def execute(self, message: BatchInfoAccountInstagramMessage):
    bucket_path = message.get("bucket_path")
    file_content = storage_gcs.download_file(bucket_name=bucket_path.split("/")[0], file_name=bucket_path.split("/")[1])

    json_data = json.loads(file_content.decode("utf-8"))

    for data in json_data:
      account: AccountDetail = data.get("account")
      posts: List[PostCommentsMap] = data.get("posts")

      account_sk = uuid4()
      dim_instagram_account_repo.save([DimInstagramAccount(
        account_sk=account_sk,
        name=account.get("name"),
        nickname=account.get("nickname"),
        url=account.get("url"),
      )])
      for post in posts:
        date_sk = uuid4()
        date = datetime.fromtimestamp(post.get("timestamp"))
        dim_date_repo.save([DimDate(
          date_sk=date_sk,
          date=date.strftime("%Y-%m-%d"),
          day=date.day,
          month=date.month,
          year=date.year,
          weekday=date.strftime("%A"),
          is_weekend=date.weekday() in [5, 6],
        )])

        post_sk = uuid4()
        dim_instagram_post_repo.save([DimInstagramPost(
          post_sk=post_sk,
          account_sk=account_sk,
          date_sk=date_sk,
          external_code=post.get("external_code"),
          caption=post.get("caption"),
          hash_tags=post.get("hash_tags"),
          audio_url=post.get("audio_url"),
          music_name=post.get("music_name"),
          owner_music_name=post.get("owner_music_name"),
        )])
        for comment in post.latest_comments:
          dim_instagram_comment_repo.save([DimInstagramComment(
            comment_sk=uuid4(),
            post_sk=post_sk,
            account_sk=account_sk,
            owner_username=comment.get("owner_username"),
            date_sk=date_sk,
          )])