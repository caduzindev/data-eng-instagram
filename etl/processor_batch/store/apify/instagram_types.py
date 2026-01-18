from typing import TypedDict, List, Optional


class Owner(TypedDict):
    id: str
    is_verified: bool
    profile_pic_url: str
    username: str


class Reply(TypedDict, total=False):
    id: str
    text: str
    ownerUsername: str
    ownerProfilePicUrl: str
    timestamp: str
    repliesCount: int
    replies: List["Reply"]
    likesCount: int
    owner: Owner


class LatestComment(TypedDict):
    id: str
    text: str
    ownerUsername: str
    ownerProfilePicUrl: str
    timestamp: str
    repliesCount: int
    replies: List[Reply]
    likesCount: int
    owner: Owner

class MusicInfo(TypedDict):
    artist_name: str
    song_name: str
    uses_original_audio: bool
    should_mute_audio: bool
    should_mute_audio_reason: str
    audio_id: str

class Post(TypedDict):
    id: str
    type: str
    shortCode: str
    caption: str
    hashtags: List[str]
    mentions: List[str]
    audioUrl: Optional[str]
    url: str
    commentsCount: int
    firstComment: Optional[str]
    latestComments: List[LatestComment]
    dimensionsHeight: int
    dimensionsWidth: int
    likesCount: int
    timestamp: str
    images: List[str]
    musicInfo: Optional[MusicInfo]
    videoUrl: Optional[str]
    videoViewCount: Optional[int]
    videoPlayCount: Optional[int]
    videoDuration: Optional[float]
    locationName: Optional[str]

class Account(TypedDict):
    id: str
    username: str
    fullName: str
    followersCount: int
    followsCount: int
    isBusinessAccount: bool
    businessCategoryName: str
    biography: str
