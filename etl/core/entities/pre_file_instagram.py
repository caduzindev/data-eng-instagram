from dataclasses import dataclass
from typing import Optional, List

@dataclass
class AccountDetail:
    """Detalhes de conta do Instagram mapeados do Apify"""
    name: Optional[str]
    nick_name: Optional[str]
    url: Optional[str]
    followers_count: Optional[int]
    follows_count: Optional[int]
    is_business: Optional[bool]
    category: Optional[str]
    biography: Optional[str]


@dataclass
class MusicInfo:
    """Informações de música de um post do Instagram"""
    artistName: Optional[str]
    songName: Optional[str]


@dataclass
class Dimensions:
    """Dimensões de um post do Instagram"""
    height: Optional[int]
    width: Optional[int]


@dataclass
class VideoInfo:
    """Informações de vídeo de um post do Instagram"""
    url: Optional[str]
    viewCount: Optional[int]
    playCount: Optional[int]
    duration: Optional[float]


@dataclass
class CommentDetail:
    """Detalhes de um comentário do Instagram"""
    text: Optional[str]
    ownerUsername: Optional[str]
    ownerProfilePicUrl: Optional[str]
    repliesCount: Optional[int]
    likesCount: Optional[int]
    timestamp: Optional[str]


@dataclass
class PostCommentsMap:
    """Mapeamento de post com comentários do Instagram"""
    shortCode: Optional[str]
    caption: Optional[str]
    hashtags: Optional[List[str]]
    audioUrl: Optional[str]
    musicInfo: Optional[MusicInfo]
    commentsCount: Optional[int]
    likesCount: Optional[int]
    dimensions: Optional[Dimensions]
    video: Optional[VideoInfo]
    locationName: Optional[str]
    timestamp: Optional[str]
    latest_comments: Optional[List[CommentDetail]]