from typing import List, TypedDict, Tuple
import json
from datetime import datetime
from uuid import uuid4

from core.infra.gcs.storage import storage_gcs
from core.entities.instagram import (
  DimInstagramAccount, DimInstagramPost, DimInstagramComment, DimDate,
  FactInstagramAccountSnapshot, FactInstagramPostMetrics, FactInstagramCommentMetrics
)
from core.entities.pre_file_instagram import PostCommentsMap, AccountDetail
from core.repositories.bigquery.dim_instagram_account_repo import dim_instagram_account_repo
from core.repositories.bigquery.dim_instagram_post_repo import dim_instagram_post_repo
from core.repositories.bigquery.dim_instagram_comment_repo import dim_instagram_comment_repo
from core.repositories.bigquery.dim_date_repo import dim_date_repo
from core.repositories.bigquery.fact_instagram_account_snapshot_repo import fact_instagram_account_snapshot_repo
from core.repositories.bigquery.fact_instagram_post_metrics_repo import fact_instagram_post_metrics_repo
from core.repositories.bigquery.fact_instagram_comment_metrics_repo import fact_instagram_comment_metrics_repo
from core.utils.serialize import safe_get
from core.messaging.kafka.producer import send_message_topic

class BatchInfoAccountInstagramMessage(TypedDict):
  bucket_path: str

class BatchInfoAccountInstagramTopic:
  """Processa dados brutos de contas do Instagram e popula tabelas dimensão e fato."""
  
  name = "batch_info_account_instagram"

  def execute(self, message: BatchInfoAccountInstagramMessage):
    """Método principal que processa a mensagem e carrega os dados."""
    bucket_path = safe_get(message, "bucket_path")
    json_data = self._load_data_from_gcs(bucket_path)
    
    for data in json_data:
      account: AccountDetail = safe_get(data, "account")
      posts: List[PostCommentsMap] = safe_get(data, "posts")
      
      account_sk = self._process_account(account)
      self._process_posts(posts, account_sk, account)
      
      self._log_success(account)
      send_message_topic('batch_info_account_instagram_success', {"account_sk": account_sk})

  def _load_data_from_gcs(self, bucket_path: str) -> List[dict]:
    """Carrega e decodifica o arquivo JSON do GCS."""
    bucket_name, file_name = bucket_path.split("/", 1)
    file_content = storage_gcs.download_file(bucket_name=bucket_name, file_name=file_name)
    return json.loads(file_content.decode("utf-8"))

  def _process_account(self, account: AccountDetail) -> str:
    """Processa uma conta do Instagram, salvando dimensão e fato."""
    account_sk = str(uuid4())
    self._save_account_dimension(account, account_sk)
    self._save_account_fact(account, account_sk)
    return account_sk

  def _process_posts(self, posts: List[PostCommentsMap], account_sk: str, account: AccountDetail):
    """Processa todos os posts de uma conta."""
    for post in posts:
      post_sk, post_date_sk = self._process_post(post, account_sk)
      self._process_comments(post, post_sk, account_sk, post_date_sk)
      self._log_post_saved(account, post)

  def _process_post(self, post: PostCommentsMap, account_sk: str) -> Tuple[str, str]:
    """Processa um post, salvando dimensão e fato. Retorna (post_sk, date_sk)."""
    date_sk = self._create_and_save_date_dimension(post)
    post_sk = str(uuid4())
    
    self._save_post_dimension(post, account_sk, date_sk, post_sk)
    self._save_post_fact(post, account_sk, post_sk, date_sk)
    
    return post_sk, date_sk

  def _process_comments(self, post: PostCommentsMap, post_sk: str, account_sk: str, post_date_sk: str):
    """Processa todos os comentários de um post."""
    latest_comments = safe_get(post, "latest_comments") or []
    
    for comment in latest_comments:
      comment_date_sk = self._get_comment_date_sk(comment, post_date_sk)
      comment_sk = str(uuid4())
      
      self._save_comment_dimension(comment, post_sk, account_sk, comment_sk, comment_date_sk)
      self._save_comment_fact(comment, post_sk, account_sk, comment_sk, comment_date_sk)

  def _create_and_save_date_dimension(self, post: PostCommentsMap) -> str:
    """Cria e salva uma dimensão de data a partir do timestamp do post."""
    date_sk = str(uuid4())
    timestamp = safe_get(post, "timestamp")
    
    if timestamp:
      date = self._parse_timestamp(timestamp)
      dim_date_repo.save([DimDate(
        date_sk=date_sk,
        date=date.strftime("%Y-%m-%d"),
        day=date.day,
        month=date.month,
        year=date.year,
        weekday=date.strftime("%A"),
        is_weekend=date.weekday() in [5, 6],
      )])
    
    return date_sk

  def _get_comment_date_sk(self, comment: dict, post_date_sk: str) -> str:
    """Obtém o date_sk do comentário, criando uma nova dimensão se necessário.
    
    Se o comentário tiver timestamp próprio, cria uma nova dimensão de data.
    Caso contrário, reutiliza o date_sk do post.
    """
    comment_timestamp = safe_get(comment, "timestamp")
    
    if not comment_timestamp:
      # Se não tiver timestamp, reutiliza o date_sk do post
      return post_date_sk
    
    try:
      comment_date = self._parse_timestamp(comment_timestamp)
      return self._create_date_sk_from_date(comment_date)
    except Exception:
      # Se falhar ao parsear, reutiliza o date_sk do post
      return post_date_sk

  def _parse_timestamp(self, timestamp: str) -> datetime:
    """Converte um timestamp ISO para datetime."""
    normalized_timestamp = timestamp.replace('Z', '+00:00')
    return datetime.fromtimestamp(
      datetime.fromisoformat(normalized_timestamp).timestamp()
    )

  def _create_date_sk_from_date(self, date: datetime) -> str:
    """Cria um date_sk a partir de uma data, salvando a dimensão."""
    date_sk = str(uuid4())
    dim_date_repo.save([DimDate(
      date_sk=date_sk,
      date=date.strftime("%Y-%m-%d"),
      day=date.day,
      month=date.month,
      year=date.year,
      weekday=date.strftime("%A"),
      is_weekend=date.weekday() in [5, 6],
    )])
    return date_sk

  def _save_account_dimension(self, account: AccountDetail, account_sk: str):
    """Salva a dimensão de conta do Instagram."""
    dim_instagram_account_repo.save([DimInstagramAccount(
      account_sk=account_sk,
      name=safe_get(account, "name"),
      nickname=safe_get(account, "nick_name"),
      url=safe_get(account, "url"),
    )])

  def _save_account_fact(self, account: AccountDetail, account_sk: str):
    """Salva o fato de snapshot da conta, se houver dados disponíveis."""
    if not self._has_account_fact_data(account):
      return
    
    fact_instagram_account_snapshot_repo.save([FactInstagramAccountSnapshot(
      account_sk=account_sk,
      followers_count=safe_get(account, "followers_count") or 0,
      follows_count=safe_get(account, "follows_count") or 0,
      is_business=safe_get(account, "is_business") or False,
      category=safe_get(account, "category"),
      biography=safe_get(account, "biography"),
    )])

  def _has_account_fact_data(self, account: AccountDetail) -> bool:
    """Verifica se há dados disponíveis para o fato de conta."""
    return any([
      safe_get(account, "followers_count") is not None,
      safe_get(account, "follows_count") is not None,
      safe_get(account, "is_business") is not None,
      safe_get(account, "category") is not None,
      safe_get(account, "biography") is not None
    ])

  def _save_post_dimension(self, post: PostCommentsMap, account_sk: str, date_sk: str, post_sk: str):
    """Salva a dimensão de post do Instagram."""
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

  def _save_post_fact(self, post: PostCommentsMap, account_sk: str, post_sk: str, date_sk: str):
    """Salva o fato de métricas do post, se houver dados disponíveis."""
    if not self._has_post_fact_data(post):
      return
    
    video = safe_get(post, "video")
    
    fact_instagram_post_metrics_repo.save([FactInstagramPostMetrics(
      account_sk=account_sk,
      post_sk=post_sk,
      comments_count=safe_get(post, "commentsCount") or 0,
      likes_count=safe_get(post, "likesCount") or 0,
      video_url=None,  # Schema não tem este campo
      video_duration=None,  # Schema não tem este campo
      video_view_count=safe_get(video, "viewCount"),
      video_play_count=safe_get(video, "playCount"),
      dim_height=None,  # Schema não tem este campo
      dim_width=None,  # Schema não tem este campo
      location=None,  # Schema não tem este campo
      date_sk=date_sk,
    )])

  def _has_post_fact_data(self, post: PostCommentsMap) -> bool:
    """Verifica se há dados disponíveis para o fato de post."""
    video = safe_get(post, "video")
    return any([
      safe_get(post, "commentsCount") is not None,
      safe_get(post, "likesCount") is not None,
      safe_get(video, "viewCount") is not None,
      safe_get(video, "playCount") is not None
    ])

  def _save_comment_dimension(self, comment: dict, post_sk: str, account_sk: str, 
                              comment_sk: str, date_sk: str):
    """Salva a dimensão de comentário do Instagram."""
    dim_instagram_comment_repo.save([DimInstagramComment(
      comment_sk=comment_sk,
      post_sk=post_sk,
      account_sk=account_sk,
      owner_username=safe_get(comment, "ownerUsername"),
      date_sk=date_sk,
    )])

  def _save_comment_fact(self, comment: dict, post_sk: str, account_sk: str, 
                         comment_sk: str, date_sk: str):
    """Salva o fato de métricas do comentário, se houver dados disponíveis."""
    if not self._has_comment_fact_data(comment):
      return
    
    fact_instagram_comment_metrics_repo.save([FactInstagramCommentMetrics(
      account_sk=account_sk,
      post_sk=post_sk,
      comment_sk=comment_sk,
      text=safe_get(comment, "text"),
      owner_pic_url=safe_get(comment, "ownerProfilePicUrl"),
      replies_count=safe_get(comment, "repliesCount") or 0,
      likes_count=safe_get(comment, "likesCount") or 0,
      date_sk=date_sk,
    )])

  def _has_comment_fact_data(self, comment: dict) -> bool:
    """Verifica se há dados disponíveis para o fato de comentário."""
    return any([
      safe_get(comment, "text") is not None,
      safe_get(comment, "ownerProfilePicUrl") is not None,
      safe_get(comment, "repliesCount") is not None,
      safe_get(comment, "likesCount") is not None
    ])

  def _log_success(self, account: AccountDetail):
    """Loga o sucesso do processamento de uma conta."""
    account_name = safe_get(account, "name")
    print('--------------------------------')
    print(f'Account {account_name} processed successfully')
    print('--------------------------------')

  def _log_post_saved(self, account: AccountDetail, post: PostCommentsMap):
    """Loga o salvamento de um post."""
    account_name = safe_get(account, "name")
    post_code = safe_get(post, "shortCode")
    print(f'  ✓ Post {post_code} saved for account {account_name}')
