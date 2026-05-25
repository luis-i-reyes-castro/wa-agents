"""
Digital Ocean Spaces S3 Bucket: Input/Output (IO) Functions
"""

from __future__ import annotations

import aioboto3
import os

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from boto3 import client as boto3_client
from botocore.config import Config
from botocore.exceptions import ClientError
from io import BytesIO
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)

from sofia_utils.io import write_to_json_string

if TYPE_CHECKING :
    from types_aiobotocore_s3.client import S3Client as AsyncS3Client
else :
    AsyncS3Client = Any


# =========================================================================================
# MODULE SETUP

BUCKET_REGION     = os.getenv("BUCKET_REGION")
BUCKET_KEY        = os.getenv("BUCKET_KEY_ID")
BUCKET_KEY_SECRET = os.getenv("BUCKET_KEY_SECRET")
BUCKET_NAME       = os.getenv("BUCKET_NAME")

missing = [ label
            for label, value in
            [ ( "BUCKET_REGION",     BUCKET_REGION),
              ( "BUCKET_KEY_ID",     BUCKET_KEY),
              ( "BUCKET_KEY_SECRET", BUCKET_KEY_SECRET),
              ( "BUCKET_NAME",       BUCKET_NAME) ]
            if not value ]

if missing :
    missing_str = ", ".join(missing)
    raise RuntimeError(f"Missing S3 bucket environment variables: {missing_str}")

boto3_client_args = {
    "config"                : Config( s3 = { "addressing_style" : "virtual" } ),
    "region_name"           : BUCKET_REGION,
    "endpoint_url"          : f"https://{BUCKET_REGION}.digitaloceanspaces.com",
    "aws_access_key_id"     : BUCKET_KEY,
    "aws_secret_access_key" : BUCKET_KEY_SECRET
}
"""
Arguments for both sequential and async clients
"""

b3 = boto3_client( service_name = "s3", **boto3_client_args)
"""
Secuential client
"""

aio_session = aioboto3.Session()
"""
Session for the async client
"""

@asynccontextmanager
async def async_boto3_client() -> AsyncIterator[AsyncS3Client] :
    """
    Build an async S3 client context manager
    """
    async with aio_session.client(
        service_name = "s3",
        **boto3_client_args,
    ) as b3a :
        yield cast( "AsyncS3Client", b3a)

# =========================================================================================
# I/O OPERATIONS

def b3_exists( key : str | Path) -> bool :
    """
    Check if an object key exists using a HEAD request \\
    Args:
        key : Object key relative to the bucket root
    Returns:
        True if the object is present; else False.
    """
    try :
        b3.head_object( Bucket = BUCKET_NAME, Key = str(key))
        return True
    
    except ClientError :
        pass
    
    return False

async def async_b3_exists( key : str | Path) -> bool :
    """
    Check if an object key exists using an async HEAD request \\
    Args:
        key : Object key relative to the bucket root
    Returns:
        True if the object is present; else False.
    """
    try :
        async with async_boto3_client() as b3a :
            await b3a.head_object( Bucket = BUCKET_NAME, Key = str(key))
        return True
    
    except ClientError :
        pass
    
    return False

def b3_list_objects(
    prefix : str | Path,
) -> list[ dict[ str, float | str] ] :
    """
    List object metadata under a prefix including last-modified timestamps \\
    Args:
        prefix : Key prefix (use empty string for the root)
    Returns:
        List of dictionaries with `Key` and `LastModified` entries.
    """
    paginator = b3.get_paginator("list_objects_v2")
    page_iter = paginator.paginate( Bucket = BUCKET_NAME, Prefix = str(prefix))
    
    keys = []
    for page in page_iter :
        for obj in page.get( "Contents", []) :
            obj_dict = { "Key" : obj["Key"],
                         "LastModified" : getattr( obj["LastModified"],
                                                   "timestamp",
                                                   lambda : 0.0 )() }
            keys.append(obj_dict)
    
    return keys

async def async_b3_list_objects(
    prefix : str | Path,
) -> list[ dict[ str, float | str] ] :
    """
    List object metadata under a prefix including last-modified timestamps \\
    Args:
        prefix : Key prefix (use empty string for the root)
    Returns:
        List of dictionaries with `Key` and `LastModified` entries.
    """
    keys = []
    
    async with async_boto3_client() as b3a :
        
        paginator = b3a.get_paginator("list_objects_v2")
        page_iter = paginator.paginate( Bucket = BUCKET_NAME, Prefix = str(prefix))
        
        async for page in page_iter :
            for obj in page.get( "Contents", []) :
                obj_dict = { "Key" : obj["Key"],
                             "LastModified" : getattr( obj["LastModified"],
                                                       "timestamp",
                                                       lambda : 0.0 )() }
                keys.append(obj_dict)
    
    return keys

def b3_list_directories( prefix : str | Path) -> list[str] :
    """
    List top-level directory names within a prefix \\
    Args:
        prefix : Key prefix (use empty string for the root)
    Returns:
        List of directory names without the prefix.
    """
    # Prepare prefix path and string form
    prefix_path = Path(prefix)
    prefix_str  = ( str(prefix_path) + '/' ) if prefix else ""
    
    # Prepare paginator
    paginator = b3.get_paginator("list_objects_v2")
    page_iter = paginator.paginate( Bucket    = BUCKET_NAME,
                                    Prefix    = prefix_str,
                                    Delimiter = '/' )
    
    # Iterate through pages and objects in each page
    directories = []
    for page in page_iter :
        for obj in page.get( "CommonPrefixes", []) :
            # Extract directory name from the prefix using pathlib
            dir_path = Path(obj["Prefix"])
            # If prefix is root then just take the first part of the path
            if prefix_str == "" :
                dir_name = dir_path.parts[0] if dir_path.parts else ""
            # Else use method to get relative path and take the first part
            else :
                dir_name = dir_path.relative_to(prefix_path).parts[0]
            # Append to list
            if dir_name :
                directories.append(dir_name)
    
    return directories

async def async_b3_list_directories( prefix : str | Path) -> list[str] :
    """
    List top-level directory names within a prefix asynchronously \\
    Args:
        prefix : Key prefix (use empty string for the root)
    Returns:
        List of directory names without the prefix.
    """
    prefix_path = Path(prefix)
    prefix_str  = ( str(prefix_path) + '/' ) if prefix else ""
    directories = []
    
    async with async_boto3_client() as b3a :
        
        paginator = b3a.get_paginator("list_objects_v2")
        page_iter = paginator.paginate( Bucket    = BUCKET_NAME,
                                        Prefix    = prefix_str,
                                        Delimiter = '/' )
        
        async for page in page_iter :
            for obj in page.get( "CommonPrefixes", []) :
                dir_path = Path(obj["Prefix"])
                if prefix_str == "" :
                    dir_name = dir_path.parts[0] if dir_path.parts else ""
                else :
                    dir_name = dir_path.relative_to(prefix_path).parts[0]
                if dir_name :
                    directories.append(dir_name)
    
    return directories

def b3_clear_prefix( prefix : str | Path) -> None :
    """
    Delete every object under a given prefix in batches of 1000 keys \\
    Args:
        prefix : Prefix whose contents should be removed
    """
    keys =[ obj["Key"] for obj in b3_list_objects(prefix) ]
    if not keys :
        return
    
    for i in range( 0, len(keys), 1000) :
        chunk = [ { "Key" : k } for k in keys[ i : i + 1000 ] ]
        b3.delete_objects( Bucket = BUCKET_NAME, Delete = { "Objects" : chunk })
    
    return

async def async_b3_clear_prefix( prefix : str | Path) -> None :
    """
    Delete every object under a given prefix in batches of 1000 keys asynchronously \\
    Args:
        prefix : Prefix whose contents should be removed
    """
    keys = [ obj["Key"] for obj in await async_b3_list_objects(prefix) ]
    if not keys :
        return
    
    async with async_boto3_client() as b3a :
        
        for i in range( 0, len(keys), 1000) :
            chunk = [ { "Key" : k } for k in keys[ i : i + 1000 ] ]
            await b3a.delete_objects( Bucket = BUCKET_NAME,
                                      Delete = { "Objects" : chunk } )
    
    return

def b3_delete( key : str) -> None :
    """
    Delete a single object from the bucket \\
    Args:
        key : Object key to delete
    """
    b3.delete_object( Bucket = BUCKET_NAME, Key = key)
    
    return

async def async_b3_delete( key : str) -> None :
    """
    Delete a single object from the bucket asynchronously \\
    Args:
        key : Object key to delete
    """
    async with async_boto3_client() as b3a :
        await b3a.delete_object( Bucket = BUCKET_NAME, Key = key)
    
    return

# =========================================================================================

def b3_get_file( key : str | Path) -> Any :
    """
    Download file content from the storage bucket \\
    Args:
        key : Object key in the bucket
    Returns:
        Raw file content as bytes
    """
    obj = b3.get_object( Bucket = BUCKET_NAME, Key = str(key))
    
    return obj["Body"].read()

async def async_b3_get_file( key : str | Path) -> Any :
    """
    Download file content from the storage bucket asynchronously \\
    Args:
        key : Object key in the bucket
    Returns:
        Raw file content as bytes
    """
    async with async_boto3_client() as b3a :
        
        obj = await b3a.get_object( Bucket = BUCKET_NAME, Key = str(key))
        
        async with obj["Body"] as stream :
            return await stream.read()

def b3_put_json( key : str | Path, obj : Any) -> None :
    """
    Serialize and upload JSON content to the bucket \\
    Args:
        key : Object key in the bucket
        obj : JSON-serializable Python object
    """
    body = BytesIO(write_to_json_string(obj).encode("utf-8"))
    
    b3.put_object( Bucket      = BUCKET_NAME,
                   Key         = str(key),
                   Body        = body.getvalue(),
                   ContentType = "application/json" )
    
    return

async def async_b3_put_json( key : str | Path, obj : Any) -> None :
    """
    Serialize and upload JSON content to the bucket asynchronously \\
    Args:
        key : Object key in the bucket
        obj : JSON-serializable Python object
    """
    body = BytesIO(write_to_json_string(obj).encode("utf-8"))
    
    async with async_boto3_client() as b3a :
        
        await b3a.put_object( Bucket      = BUCKET_NAME,
                              Key         = str(key),
                              Body        = body.getvalue(),
                              ContentType = "application/json" )

    return

def b3_put_media( key     : str | Path,
                  content : bytes,
                  mime    : str ) -> dict[ str : str] :
    """
    Upload binary media content using the provided MIME type \\
    Args:
        key     : Destination object key
        content : Raw file content as bytes
        mime    : MIME type string (e.g., image/jpeg)
    Returns:
        Dictionary with the bucket name and stored object key.
    """
    b3.put_object( Bucket      = BUCKET_NAME,
                   Key         = str(key),
                   Body        = content,
                   ContentType = mime,
                   ACL         = "private" )
    
    return { "bucket" : BUCKET_NAME, "key" : str(key) }

async def async_b3_put_media( key     : str | Path,
                              content : bytes,
                              mime    : str ) -> dict[ str, str] :
    """
    Upload binary media content using the provided MIME type asynchronously \\
    Args:
        key     : Destination object key
        content : Raw file content as bytes
        mime    : MIME type string (e.g., image/jpeg)
    Returns:
        Dictionary with the bucket name and stored object key.
    """
    async with async_boto3_client() as b3a :
        
        await b3a.put_object( Bucket      = BUCKET_NAME,
                              Key         = str(key),
                              Body        = content,
                              ContentType = mime,
                              ACL         = "private" )
    
    return { "bucket" : BUCKET_NAME, "key" : str(key) }

# =========================================================================================

def b3_get_error_code( ex : ClientError) -> str | None :
    """
    Extract and format an error code from a boto3 ClientError \\
    Args:
        ex : ClientError raised during an S3 operation
    Returns:
        String such as 'Error code AccessDenied', or None if missing.
    """
    code = ex.response.get( "Error", {}).get( "Code", "")
    
    return f"Error code {code}" if code else None

def presign( action  : str,
             key     : str | Path,
             expires : int = 3600 ) -> str :
    """
    Generate a presigned GET or PUT URL for the bucket \\
    Args:
        action  : Either "get" or "put"
        key     : Object key
        expires : Expiration time in seconds (default 1 hour)
    Returns:
        Generated presigned URL or an error message if action invalid.
    """
    if action not in ( "get", "put") :
        return f"Error in function presign: Invalid action '{action}'"
    
    gpu_args = { "ClientMethod" : f"{action}_object",
                 "Params"       : { "Bucket" : BUCKET_NAME, "Key": str(key) },
                 "ExpiresIn"    : expires }
    
    return b3.generate_presigned_url(**gpu_args)
