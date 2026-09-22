"""
S3 media storage.

Object layout:
    <business_id>/<contact_id>/<case_id>_<message_id>.<extension>

Only media bytes live in S3. All metadata, messages, manifests, deduplication data,
and contact leases live in PostgreSQL.
"""

import os

from cachetools import LRUCache
from pathlib import PurePosixPath
from threading import Lock

from sofia_utils.printing import get_qualname as here

from .case_handler_models import MediaObject
from .S3_bucket_io import (
    async_b3_delete,
    async_b3_exists,
    async_b3_get_file,
    async_b3_put_media,
    b3_delete,
    b3_exists,
    b3_get_file,
    b3_put_media,
)


MEDIA_CACHE_DEFAULT_MB = 16


def _media_cache_size() -> int :
    """
    Return the configured process-wide media-cache byte limit.
    """
    configured = os.getenv(
        "WA_AGENTS_MEDIA_CACHE_MB",
        str(MEDIA_CACHE_DEFAULT_MB),
    )
    try :
        max_mb = int(configured)
    except ValueError as ex :
        raise ValueError(
            f"In {here()}: Environment variable "
            "'WA_AGENTS_MEDIA_CACHE_MB' must be a nonnegative integer"
        ) from ex
    if max_mb < 0 :
        raise ValueError(
            f"In {here()}: Environment variable "
            "'WA_AGENTS_MEDIA_CACHE_MB' must be a nonnegative integer"
        )
    
    return max_mb * 1024 * 1024


_media_cache : LRUCache[ str, bytes] = LRUCache(
    maxsize   = _media_cache_size(),
    getsizeof = len,
)
_media_cache_lock = Lock()


def _media_cache_get( object_key : str) -> bytes | None :
    
    with _media_cache_lock :
        try :
            return _media_cache[object_key]
        except KeyError :
            return None


def _media_cache_set( object_key : str, content : bytes) -> None :
    
    with _media_cache_lock :
        if (
            ( not _media_cache.maxsize            ) or
            ( len(content) > _media_cache.maxsize )
        ) :
            _media_cache.pop( object_key, None)
            return
        
        _media_cache[object_key] = content
    
    return


def _media_cache_discard( object_key : str) -> None :
    
    with _media_cache_lock :
        _media_cache.pop( object_key, None)
    
    return


def media_object_key(
    business_id : int,
    contact_id  : int,
    case_id     : int,
    filename    : str,
) -> str :
    """
    Build a media object key using the normalized S3 tree.
    """
    ids = (
        ( "business_id", business_id),
        ( "contact_id",  contact_id ),
        ( "case_id",     case_id    ),
    )
    for label, value in ids :
        if (
            isinstance( value, bool) or
            ( ( not isinstance( value, int) ) or ( value <= 0 ) )
        ) :
            raise ValueError(
                f"In {here()}: Argument '{label}' must be a positive integer"
            )
    
    if (
        ( not isinstance( filename, str) ) or
        ( not filename                   ) or
        ( filename != filename.strip()   ) or
        ( filename in ( ".", "..")       ) or
        ( "/"  in filename               ) or
        ( "\\" in filename               )
    ) :
        raise ValueError(
            f"In {here()}: Argument 'filename' must be a non-empty path component"
        )
    
    return str(
        PurePosixPath(
            str(business_id),
            str(contact_id),
            f"{case_id}_{filename}",
        )
    )


class S3BucketStorage :
    """
    Sequential media-only S3 adapter.
    """
    
    def media_write(
        self,
        business_id : int,
        contact_id  : int,
        case_id     : int,
        media       : MediaObject,
    ) -> str :
        """
        Store media bytes and return the object key to persist in PostgreSQL.
        """
        if media.content is None :
            raise ValueError(
                f"In {here()}: media.content is required for an S3 write"
            )
        if not media.name :
            raise ValueError(
                f"In {here()}: media.name is required for an S3 write"
            )
        
        object_key = media_object_key(
            business_id,
            contact_id,
            case_id,
            media.name,
        )
        
        b3_put_media( object_key, media.content, media.mime)
        _media_cache_set( object_key, media.content)
        
        return object_key
    
    def media_exists( self, object_key : str) -> bool :
        """
        Return whether a media object exists.
        """
        return b3_exists(object_key)
    
    def media_read( self, object_key : str) -> bytes :
        """
        Read media bytes using the object key stored in PostgreSQL
        """
        content = _media_cache_get(object_key)
        if content is None :
            content = b3_get_file(object_key)
            _media_cache_set( object_key, content)
        
        return content
    
    def media_delete( self, object_key : str) -> None :
        """
        Delete a media object.
        """
        b3_delete(object_key)
        _media_cache_discard(object_key)
        return


class AsyncS3BucketStorage :
    """
    Asynchronous media-only S3 adapter.
    """
    
    async def media_write(
        self,
        business_id : int,
        contact_id  : int,
        case_id     : int,
        media       : MediaObject,
    ) -> str :
        """
        Store media bytes and return the object key to persist in PostgreSQL.
        """
        if media.content is None :
            raise ValueError(
                f"In {here()}: media.content is required for an S3 write"
            )
        if not media.name :
            raise ValueError(
                f"In {here()}: media.name is required for an S3 write"
            )
        
        object_key = media_object_key(
            business_id,
            contact_id,
            case_id,
            media.name,
        )
        
        await async_b3_put_media( object_key, media.content, media.mime)
        _media_cache_set( object_key, media.content)
        
        return object_key
    
    async def media_exists( self, object_key : str) -> bool :
        """
        Return whether a media object exists.
        """
        return await async_b3_exists(object_key)
    
    async def media_read( self, object_key : str) -> bytes :
        """
        Read media bytes using the object key stored in PostgreSQL.
        """
        content = _media_cache_get(object_key)
        if content is None :
            content = await async_b3_get_file(object_key)
            _media_cache_set( object_key, content)
        
        return content
    
    async def media_delete( self, object_key : str) -> None :
        """
        Delete a media object.
        """
        await async_b3_delete(object_key)
        _media_cache_discard(object_key)
        return
