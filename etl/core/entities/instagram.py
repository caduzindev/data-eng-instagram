from dataclasses import dataclass
from typing import Optional, List

@dataclass
class DimInstagramAccount:
    """Dimensão de contas do Instagram"""
    account_sk: str
    name: str
    url: str


@dataclass
class FactInstagramAccountSnapshot:
    """Fato de snapshot de contas do Instagram"""
    account_sk: str
    followers_count: int
    follows_count: int
    is_business: bool
    category: Optional[str]
    biography: Optional[str]


@dataclass
class DimInstagramPost:
    """Dimensão de posts do Instagram"""
    account_sk: str
    external_code: str
    caption: str
    hash_tags: List[str]
    audio_url: Optional[str]
    music_name: Optional[str]
    owner_music_name: Optional[str]


@dataclass
class FactInstagramPostMetrics:
    """Fato de métricas de posts do Instagram"""
    account_sk: str
    post_sk: str
    comments_count: int
    likes_count: int
    video_url: Optional[str]
    video_duration: Optional[float]
    video_view_count: Optional[int]
    video_play_count: Optional[int]
    dim_height: Optional[int]
    dim_width: Optional[int]
    location: Optional[str]
    date_sk: str


@dataclass
class DimInstagramComment:
    """Dimensão de comentários do Instagram"""
    post_sk: str
    account_sk: str
    owner_username: str


@dataclass
class FactInstagramCommentMetrics:
    """Fato de métricas de comentários do Instagram"""
    account_sk: str
    post_sk: str
    comment_sk: str
    text: Optional[str]
    owner_pic_url: Optional[str]
    replies_count: int
    likes_count: int
    date_sk: str


@dataclass
class DimDate:
    """Dimensão de data"""
    date: str
    day: str
    month: str
    year: str
    weekday: str
    is_weekend: str

