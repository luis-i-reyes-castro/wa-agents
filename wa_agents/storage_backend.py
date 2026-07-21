"""
Storage backend selection for case handlers.
"""

import os
from typing import Any


def get_storage_backend() -> str :
    """
    Read and validate the configured storage backend. \\
    Returns:
        Either `supabase` or `s3`.
    """
    backend = os.getenv( "WA_AGENTS_STORAGE_BACKEND", "supabase").lower()
    
    if backend not in ( "supabase", "s3") :
        raise RuntimeError(
            "Invalid WA_AGENTS_STORAGE_BACKEND "
            f"'{backend}'. Expected 'supabase' or 's3'."
        )
    
    return backend


def get_sync_storage_classes() -> tuple[ type[Any], type[Any]] :
    """
    Select sync storage and lock classes for the configured backend.
    """
    if get_storage_backend() == "s3" :
        from .do_bucket_lock import DOBucketLock
        from .do_bucket_storage import DOBucketStorage
        
        return DOBucketStorage, DOBucketLock
    
    from .supabase_storage import (
        SyncSupabaseStorage,
        SyncSupabaseStorageLock,
    )
    
    return SyncSupabaseStorage, SyncSupabaseStorageLock


def get_async_storage_classes() -> tuple[type[Any], type[Any]] :
    """
    Select async storage and lock classes for the configured backend.
    """
    if get_storage_backend() == "s3" :
        from .do_bucket_lock import AsyncDOBucketLock
        from .do_bucket_storage import AsyncDOBucketStorage
        
        return AsyncDOBucketStorage, AsyncDOBucketLock
    
    from .supabase_storage import (
        AsyncSupabaseStorage,
        AsyncSupabaseStorageLock,
    )
    
    return AsyncSupabaseStorage, AsyncSupabaseStorageLock
