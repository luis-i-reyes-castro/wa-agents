"""
S3 media storage.

Object layout:
    <business_id>/<contact_id>/<case_id>/<filename>

Only media bytes live in S3. All metadata, messages, manifests, deduplication data,
and contact leases live in PostgreSQL.
"""

from inspect import currentframe
from pathlib import PurePosixPath

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


def media_object_key(
    business_id : int,
    contact_id  : int,
    case_id     : int,
    filename    : str,
) -> str :
    """
    Build a media object key using the normalized S3 tree.
    """
    here = currentframe().f_code.co_name
    
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
                f"In {here}: Argument '{label}' must be a positive integer"
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
            f"In {here}: Argument 'filename' must be a non-empty path component"
        )
    
    return str(
        PurePosixPath( str(business_id), str(contact_id), str(case_id), filename)
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
        here = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        
        if media.content is None :
            raise ValueError(
                f"In {here}: media.content is required for an S3 write"
            )
        if not media.name :
            raise ValueError(
                f"In {here}: media.name is required for an S3 write"
            )
        
        object_key = media_object_key(
            business_id,
            contact_id,
            case_id,
            media.name,
        )
        
        b3_put_media( object_key, media.content, media.mime)
        
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
        return b3_get_file(object_key)
    
    def media_delete( self, object_key : str) -> None :
        """
        Delete a media object.
        """
        b3_delete(object_key)
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
        here = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        
        if media.content is None :
            raise ValueError(
                f"In {here}: media.content is required for an S3 write"
            )
        if not media.name :
            raise ValueError(
                f"In {here}: media.name is required for an S3 write"
            )
        
        object_key = media_object_key(
            business_id,
            contact_id,
            case_id,
            media.name,
        )
        
        await async_b3_put_media( object_key, media.content, media.mime)
        
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
        return await async_b3_get_file(object_key)
    
    async def media_delete( self, object_key : str) -> None :
        """
        Delete a media object.
        """
        await async_b3_delete(object_key)
        return
