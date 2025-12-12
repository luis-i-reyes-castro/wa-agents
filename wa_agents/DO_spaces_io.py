"""
Digital Ocean Spaces S3 Bucket: Input/Output (IO) Functions
"""

from boto3 import client as boto3_client
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from io import BytesIO
from os import getenv
from pathlib import Path
from typing import Any

from sofia_utils.io import write_to_json_string


# =========================================================================================

DOT_ENV_RESULT    = load_dotenv()
BUCKET_REGION     = getenv("SPACES_REGION")
BUCKET_KEY        = getenv("SPACES_KEY_ID")
BUCKET_KEY_SECRET = getenv("SPACES_KEY_SECRET")
BUCKET_NAME       = getenv("SPACES_BUCKET")

boto3_client_args = \
    { 
    "service_name"          : "s3",
    "config"                : Config( s3 = { "addressing_style" : "virtual" } ),
    "region_name"           : BUCKET_REGION,
    "endpoint_url"          : f"https://{BUCKET_REGION}.digitaloceanspaces.com",
    "aws_access_key_id"     : BUCKET_KEY,
    "aws_secret_access_key" : BUCKET_KEY_SECRET
    }

b3 = boto3_client(**boto3_client_args)

# =========================================================================================

def b3_exists( key : str | Path) -> bool :
    """
    Check if object key exists. Uses HEAD (faster than GET).
    Args:
        key: Object key
    Returns:
        True if it exists else False
    """
    try :
        b3.head_object( Bucket = BUCKET_NAME, Key = str(key))
        return True
    except ClientError as ex :
        pass
    return False

def b3_list_objects( prefix : str | Path) -> list[str] :
    """
    List all object keys under a prefix with Key and LastModified timestamp
    Args:
        prefix: Key prefix. NOTE: Use "" for root.
    Returns:
        List of full keys (including the prefix)
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

def b3_list_directories( prefix : str | Path) -> list[str] :
    """
    List all directory names under a prefix (first-level subdirectories only).
    Args:
        prefix: Key prefix. NOTE: Use "" for root.
    Returns:
        List of directory names (not full paths)
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

def b3_clear_prefix( prefix : str | Path) -> None :
    """
    Delete all objects under a prefix. No-op if empty.
    Performs chunked deletes in batches of 1000.
    Args:
        prefix: Key prefix
    """
    keys =[ obj["Key"] for obj in b3_list_objects(prefix) ]
    if not keys :
        return
    
    # S3 DeleteObjects processes up to 1000 keys per request
    for i in range( 0, len(keys), 1000) :
        chunk = [ { "Key" : k } for k in keys[ i : i + 1000 ] ]
        b3.delete_objects( Bucket = BUCKET_NAME, Delete = { "Objects" : chunk })
    
    return

def b3_delete( key : str) -> None :
    
    b3.delete_object( Bucket = BUCKET_NAME, Key = key)
    
    return

# =========================================================================================

def b3_get_file( key : str | Path) -> Any :
    """
    Download file content from storage bucket.
    Args:
        key: Object key in the bucket
    Returns:
        Raw file content as bytes
    """
    obj = b3.get_object( Bucket = BUCKET_NAME, Key = str(key))
    
    return obj["Body"].read()

def b3_put_json( key : str | Path, obj : Any) -> None :
    """
    Upload JSON object to storage bucket.
    Args:
        key: Object key in the bucket
        obj: Python object to serialize as JSON
    """
    body = BytesIO(write_to_json_string(obj).encode("utf-8"))
    
    b3.put_object( Bucket      = BUCKET_NAME,
                   Key         = str(key),
                   Body        = body.getvalue(),
                   ContentType = "application/json" )
    
    return

def b3_put_media( key     : str | Path,
                  content : bytes,
                  mime    : str ) -> dict[ str : str] :
    """
    Upload media file for a specific user and case.
    Args:
        user_id:  User ID
        case_id:  Case ID
        filename: Name of the media file
        content:  Raw file content as bytes
        mime: MIME type of the file
    Returns:
        Dictionary with bucket name and object key
    """
    
    b3.put_object( Bucket      = BUCKET_NAME,
                   Key         = str(key),
                   Body        = content,
                   ContentType = mime,
                   ACL         = "private" )
    
    return { "bucket" : BUCKET_NAME, "key" : str(key) }

# =========================================================================================

def b3_get_error_code( ex : ClientError) -> str | None :
    """
    Extract error code from boto3 ClientError exception.
    Args:
        ex: ClientError exception from boto3 operation
    Returns:
        String with formatted error code
    """
    
    code = ex.response.get( "Error", {}).get( "Code", "")
    
    return f"Error code {code}" if code else None

def presign( action  : str,
             key     : str | Path,
             expires : int = 3600 ) -> str :
    """
    Generate a presigned URL. For possible future use.
    Args:
        action:  Either "get" or "put"
        key:     Object key
        expires: Presigned URL expiration time in seconds
    Returns:
        String with generated presigned URL
    """
    if action not in ( "get", "put") :
        return f"Error in function presign: Invalid action '{action}'"
    
    gpu_args = { "ClientMethod" : f"{action}_object",
                 "Params"       : { "Bucket" : BUCKET_NAME, "Key": str(key) },
                 "ExpiresIn"    : expires }
    
    return b3.generate_presigned_url(**gpu_args)
