"""
S3 Bucket Storage
----------------
Directory layout:
<business_id>/
    <contact_id>/
        <case_id>/
                <media_id>.<extension>
"""

import asyncio
import os
import time

from botocore.exceptions import ClientError
from datetime import ( datetime,
                       timezone )
from inspect import currentframe
from pathlib import Path
from pydantic import BaseModel
from socket import gethostname
from types import TracebackType
from typing import Any
from uuid import uuid4

from sofia_utils.io import load_json_string
from sofia_utils.stamps import utc_iso_to_dt

from .case_handler_models import (
    CaseManifest,
    MediaContent,
    Message,
    UserContentMsg,
)
from .S3_bucket_io import (
    b3_delete,
    b3_exists,
    b3_get_file,
    b3_list_directories,
    b3_list_objects,
    b3_put_json,
    b3_put_media,
    async_b3_delete,
    async_b3_exists,
    async_b3_get_file,
    async_b3_list_directories,
    async_b3_list_objects,
    async_b3_put_json,
    async_b3_put_media,
)


class S3BucketLock :
    """
    Best-effort distributed lock for DigitalOcean Spaces using a lease file. \\
    - Each contender writes a unique token object under a common prefix:
        storage/<user_id>/locks/<name>/<token>.json
    - The *earliest* non-stale token is the owner.
    - Stale owner detection via TTL; stale token is deleted opportunistically.
    This mirrors your DirLock TTL cleanup approach for object storage.
    """
    
    def __init__( self,
                  prefix   : str | Path,
                  timeout  : float = 10.0,
                  poll     : float = 0.05,
                  ttl      : float = 30.0,
                  owner_id : str | None = None ) -> None :
        """
        Configure the distributed lock helper \\
        Args:
            prefix   : Base prefix (user root) for the locks directory
            timeout  : Max seconds to wait for the lock
            poll     : Interval between acquisition retries
            ttl      : Lease duration used to detect stale tokens
            owner_id : Optional identifier used for debugging/logging
        """
        self.prefix   = str( Path(prefix) / "locks" )
        self.timeout  = timeout
        self.poll     = poll
        self.ttl      = ttl
        
        # lock prefix and our token object key
        host          = gethostname()
        pid           = str(os.getpid()) if "os" in globals() else "0"
        self.owner_id = owner_id or f"{host}:{pid}"
        self.token    = f"{self.owner_id}-{uuid4().hex}"
        self.key      = f"{self.prefix}/{self.token}.json"
        self.acquired = False
        
        return

    def __enter__( self ) -> "S3BucketLock" :
        """
        Acquire the distributed lock by writing/contending lease tokens \\
        Returns:
            Self when the current token becomes the earliest non-stale entry.
        """
        # Write our token (lease) with a small payload
        now   = time.time()
        lease = { "owner_id"   : self.owner_id,
                  "token"      : self.token,
                  "created_at" : now,
                  "ttl"        : self.ttl }
        b3_put_json( self.key, lease)
        
        start = time.time()
        while True :
            # 1) Gather contenders under the prefix
            objs = b3_list_objects(self.prefix)
            
            # 2) Opportunistic stale cleanup
            #    If the earliest is stale, try removing it.
            if objs :
                earliest = min( objs, key = lambda obj : obj["LastModified"] )
                age      = time.time() - earliest["LastModified"]
                if age > ( self.ttl + 1.0 ) :
                    try :
                        b3_delete( earliest["Key"] )
                        # re-list after cleanup
                        objs = b3_list_objects(self.prefix)
                    except ClientError :
                        pass
            
            # 3) Decide winner: earliest non-empty set wins
            if objs :
                winner = min( objs, key = lambda obj : obj["LastModified"])
                if winner["Key"] == self.key :
                    self.acquired = True
                    return self
            
            # 4) Timeout / retry
            if time.time() - start > self.timeout :
                raise TimeoutError( f"Lock timeout for prefix '{self.prefix}'" )
            
            time.sleep( self.poll )
    
    def __exit__( self,
                  exc_type : type[BaseException] | None,
                  exc      : BaseException | None,
                  tb       : TracebackType | None ) -> None :
        """
        Release the lease token when leaving the context \\
        Args:
            exc_type : Exception type raised within the context (if any)
            exc      : Exception instance (if any)
            tb       : Traceback object (if any)
        """
        # Only owner attempts to release its own token
        if self.acquired :
            try :
                b3_delete(self.key)
            except ClientError :
                pass
        return


class AsyncS3BucketLock :
    """
    Async best-effort distributed lock for DigitalOcean Spaces using a lease file.
    """
    
    def __init__( self,
                  prefix   : str | Path,
                  timeout  : float = 10.0,
                  poll     : float = 0.05,
                  ttl      : float = 30.0,
                  owner_id : str | None = None ) -> None :
        """
        Configure the distributed lock helper \\
        Args:
            prefix   : Base prefix (user root) for the locks directory
            timeout  : Max seconds to wait for the lock
            poll     : Interval between acquisition retries
            ttl      : Lease duration used to detect stale tokens
            owner_id : Optional identifier used for debugging/logging
        """
        self.prefix   = str( Path(prefix) / "locks" )
        self.timeout  = timeout
        self.poll     = poll
        self.ttl      = ttl
    
        host          = gethostname()
        pid           = str(os.getpid()) if "os" in globals() else "0"
        self.owner_id = owner_id or f"{host}:{pid}"
        self.token    = f"{self.owner_id}-{uuid4().hex}"
        self.key      = f"{self.prefix}/{self.token}.json"
        self.acquired = False
        
        return
    
    async def __aenter__(self) -> "AsyncS3BucketLock" :
        """
        Acquire the distributed lock by writing/contending lease tokens \\
        Returns:
            Self when the current token becomes the earliest non-stale entry.
        """
        now   = time.time()
        lease = { "owner_id"   : self.owner_id,
                  "token"      : self.token,
                  "created_at" : now,
                  "ttl"        : self.ttl }
        await async_b3_put_json( self.key, lease)
        
        start = time.time()
        while True :
            objs = await async_b3_list_objects(self.prefix)
            
            if objs :
                earliest = min( objs, key = lambda obj : obj["LastModified"] )
                age      = time.time() - earliest["LastModified"]
                if age > ( self.ttl + 1.0 ) :
                    try :
                        await async_b3_delete( earliest["Key"] )
                        objs = await async_b3_list_objects(self.prefix)
                    except ClientError :
                        pass
            
            if objs :
                winner = min( objs, key = lambda obj : obj["LastModified"])
                if winner["Key"] == self.key :
                    self.acquired = True
                    return self
            
            if time.time() - start > self.timeout :
                raise TimeoutError( f"Lock timeout for prefix '{self.prefix}'" )
            
            await asyncio.sleep(self.poll)
    
    async def __aexit__( self,
                         exc_type : type[BaseException] | None,
                         exc      : BaseException | None,
                         tb       : TracebackType | None ) -> None :
        """
        Release the lease token when leaving the context \\
        Args:
            exc_type : Exception type raised within the context (if any)
            exc      : Exception instance (if any)
            tb       : Traceback object (if any)
        """
        if self.acquired :
            try :
                await async_b3_delete(self.key)
            except ClientError :
                pass
        
        return

class S3BucketStorage :
    
    # -------------------------------------------------------------------------------------
    # ORIGINAL CLASS DEFINITION
    # -------------------------------------------------------------------------------------
    
    # Set user ID
    def __init__( self,
                  operator_id : str | int,
                  user_id     : str | int ) -> None :
        """
        Initialize a storage helper for an operator/user pair \\
        Args:
            operator_id : WhatsApp phone-number ID (bucket root partition)
            user_id     : User phone-number ID
        """
        
        self.operator_id = str(operator_id)
        self.user_id     = str(user_id)
        self.case_id     = None
        
        return
    
    # Set case ID
    def set_case_id( self, case_id : str | int) -> None :
        """
        Validate and store the active case identifier \\
        Args:
            case_id : Numeric case identifier or digit-only string
        """
        
        if case_id and isinstance( case_id, int) :
            self.case_id = case_id
        
        elif case_id and isinstance( case_id, str) and case_id.isdigit() :
            self.case_id = int(case_id)
        
        else :
            e_orig = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
            e_msg  = f"In {e_orig}: Invalid 'case_id' type {type(case_id)}"
            raise ValueError(e_msg)
        
        return
    
    # -------------------------------------------------------------------------------------
    # PATHS TO DIRECTORIES
    
    def dir_user(self) -> Path :
        """
        Compute `<operator_id>/<user_id>` directory path \\
        Returns:
            Base path shared by all user assets
        """
        p = Path(self.operator_id) / Path(self.user_id)
        return p
    
    def dir_case(self) -> Path :
        """
        Compute `<operator_id>/<user_id>/cases/<case_id>` path \\
        Returns:
            Full case directory path for the selected case_id
        """
        if self.case_id :
            p = self.dir_user() / "cases" / str(self.case_id)
            return p
        else :
            e_orig = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
            e_msg  = f"In {e_orig}: 'case_id' has not been initialized"
            raise ValueError(e_msg)
    
    def dir_dedup(self) -> Path :
        """
        Compute `<operator_id>/<user_id>/dedup` path \\
        Returns:
            Directory containing idempotency markers
        """
        p = self.dir_user() / "dedup"
        return p
    
    def dir_messages(self) -> Path :
        """
        Compute `<operator_id>/<user_id>/cases/<case_id>/messages` path \\
        Returns:
            Directory containing serialized messages for the case
        """
        p = self.dir_case() / "messages"
        return p
    
    def dir_media(self) -> Path :
        """
        Compute `<operator_id>/<user_id>/cases/<case_id>/media` path \\
        Returns:
            Directory containing uploaded media for the case
        """
        p = self.dir_case() / "media"
        return p
    
    # -------------------------------------------------------------------------------------
    # PATHS TO FILES
    
    def path_user_data(self) -> Path :
        """
        Path to `.../user_data.json` \\
        Returns:
            Location for the persisted UserData document
        """
        p = self.dir_user() / "user_data.json"
        return p
    
    def path_case_index(self) -> Path :
        """
        Path to `.../case_index.json` \\
        Returns:
            Location for the CaseIndex file storing the open case id
        """
        p = self.dir_user() / "case_index.json"
        return p
    
    def path_manifest(self) -> Path :
        """
        Path to `.../cases/<case_id>/case_manifest.json` \\
        Returns:
            Location for the CaseManifest file
        """
        p = self.dir_case() / "case_manifest.json"
        return p
    
    def path_message( self, message_id : str) -> Path :
        """
        Path to `.../cases/<case_id>/messages/<message_id>.json` \\
        Args:
            message_id : Message identifier
        """
        p = self.dir_messages() / f"{message_id}.json"
        return p
    
    # -------------------------------------------------------------------------------------
    # DEDUPLICATION
    # -------------------------------------------------------------------------------------
    
    def dedup_exists( self, idempotency_key : str) -> bool :
        """
        Check if an idempotency marker already exists for the user \\
        Args:
            idempotency_key : Unique key assigned to an incoming message
        Returns:
            True if the marker is present, else False.
        """
        p = self.dir_dedup() / f"{idempotency_key}.json"
        
        return b3_exists(p)
    
    def dedup_write( self, idempotency_key : str) -> None :
        """
        Persist a processed-id marker under the dedup directory \\
        Args:
            idempotency_key : Unique key assigned to an incoming message
        """
        p = self.dir_dedup() / f"{idempotency_key}.json"
        b3_put_json( p, True)
        
        return
    
    # -------------------------------------------------------------------------------------
    # JSON I/O
    # -------------------------------------------------------------------------------------
    
    def json_read( self, path : Path) -> Any | None :
        """
        Read and deserialize a JSON document from the bucket \\
        Args:
            path : Object key resolved via the helper's directory methods
        Returns:
            Parsed Python object or None if the file does not exist.
        """
        if b3_exists(path) :
            return load_json_string(b3_get_file(path))
        
        return None
    
    def json_write( self, path : Path, obj : Any) -> None :
        """
        Serialize and store an object as JSON in the bucket \\
        Args:
            path : Destination object key
            obj  : JSON-serializable Python object
        """
        b3_put_json( path, obj)
        
        return
    
    # -------------------------------------------------------------------------------------
    # MESSAGE I/O
    # -------------------------------------------------------------------------------------
    
    def message_read( self, message_id : str) -> Message | None :
        """
        Load and deserialize a stored message document \\
        Args:
            message_id : Message identifier
        Returns:
            Message subclass instance if present, else None.
        """
        p        = self.path_message(message_id)
        msg_dict = self.json_read(p)
        msg_obj  = None
        
        if msg_dict :
            msg_bm = msg_dict.get("basemodel")
            if msg_bm and isinstance( msg_bm, str) :
                
                from . import basemodels
                MsgBM = getattr( basemodels, msg_bm, None)
                
                if MsgBM and issubclass( MsgBM, BaseModel) :
                    msg_obj = MsgBM.model_validate(msg_dict)
        
        return msg_obj
    
    def message_write( self, message : Message) -> None :
        """
        Persist a message document under the case directory \\
        Args:
            message : Message subclass instance
        """
        p = self.path_message(message.id)
        
        self.json_write( p, message.model_dump())
        
        return
    
    def media_get( self, filename : str) -> bytes | None :
        """
        Download stored media content for the active case \\
        Args:
            filename : Media filename (with extension)
        Returns:
            Media content bytes or None if the file is absent.
        """
        p = self.dir_media() / filename
        
        return b3_get_file(p) if b3_exists(p) else None
    
    def media_write( self,
                     message : UserContentMsg,
                     media   : MediaContent ) -> None :
        """
        Store media bytes associated with a user content message \\
        Args:
            message : User message containing metadata (filename, mime)
            media   : Media content payload to persist
        """
        media_path = self.dir_media() / message.media.name
        
        if not b3_exists(media_path) :
            media_content = media.content if media.content else b""
            b3_put_media( media_path, media_content, media.mime)
        
        return
    
    # -------------------------------------------------------------------------------------
    # MANIFEST
    # -------------------------------------------------------------------------------------
    
    def get_next_case_id(self) -> int :
        """
        Determine the next sequential case ID for the user \\
        Returns:
            First positive integer not currently assigned to a case
        """
        
        max_id    = 0
        prefix    = self.dir_user() / "cases"
        case_dirs = b3_list_directories(prefix)
        
        for case_dir_str in case_dirs :
            if case_dir_str.isdigit() :
                max_id = max( max_id, int(case_dir_str))
        
        return max_id + 1 if max_id > 0 else 1
    
    def manifest_append( self,
                         manifest : CaseManifest,
                         message  : Message ) -> None :
        """
        Append message metadata to the manifest and refresh timestamps \\
        Args:
            manifest : Case Manifest model for the active case
            message  : Message to record
        """
        
        # Append message to manifest
        if message.id not in manifest.message_ids :
            manifest.message_ids.append(message.id)
        
        # Update manifest time_last_message
        
        existing_last = utc_iso_to_dt(manifest.time_last_message)
        msg_time      = utc_iso_to_dt(message.time_created) or  \
                        utc_iso_to_dt(message.time_received) or \
                        datetime.now(timezone.utc)
        
        if ( not existing_last ) or ( existing_last < msg_time ) :
            msg_time = msg_time.replace( microsecond = 0)
            msg_time = msg_time.isoformat().replace( "+00:00", "Z")
            manifest.time_last_message = msg_time
        
        # Re-write manifest
        self.manifest_write(manifest)
        
        return
    
    def manifest_load(self) -> CaseManifest | None :
        """
        Load the active case manifest from the bucket \\
        Returns:
            CaseManifest instance if data exists, else None.
        """
        p    = self.path_manifest()
        data = self.json_read(p)
        
        return CaseManifest.model_validate(data) if data else None
    
    def manifest_write( self, manifest : CaseManifest) -> None :
        """
        Persist the manifest JSON for the active case \\
        Args:
            manifest : CaseManifest instance
        """
        p = self.path_manifest()
        self.json_write( p, manifest.model_dump())
        
        return


class AsyncS3BucketStorage (S3BucketStorage) :
    """
    Async Digital Ocean Spaces storage helper
    """
    
    # -------------------------------------------------------------------------------------
    # DEDUPLICATION
    
    async def dedup_exists( self, idempotency_key : str) -> bool :
        """
        Check if an idempotency marker already exists for the user asynchronously \\
        Args:
            idempotency_key : Unique key assigned to an incoming message
        Returns:
            True if the marker is present, else False.
        """
        p = self.dir_dedup() / f"{idempotency_key}.json"
        
        return await async_b3_exists(p)
    
    async def dedup_write( self, idempotency_key : str) -> None :
        """
        Persist a processed-id marker under the dedup directory asynchronously \\
        Args:
            idempotency_key : Unique key assigned to an incoming message
        """
        p = self.dir_dedup() / f"{idempotency_key}.json"
        await async_b3_put_json( p, True)
        
        return
    
    # -------------------------------------------------------------------------------------
    # JSON I/O
    
    async def json_read( self, path : Path) -> Any | None :
        """
        Read and deserialize a JSON document from the bucket asynchronously \\
        Args:
            path : Object key resolved via the helper's directory methods
        Returns:
            Parsed Python object or None if the file does not exist.
        """
        if await async_b3_exists(path) :
            return load_json_string(await async_b3_get_file(path))
        
        return None
    
    async def json_write( self, path : Path, obj : Any) -> None :
        """
        Serialize and store an object as JSON asynchronously \\
        Args:
            path : Destination object key
            obj  : JSON-serializable Python object
        """
        await async_b3_put_json( path, obj)
        
        return
    
    # -------------------------------------------------------------------------------------
    # MESSAGE I/O
    
    async def message_read( self, message_id : str) -> Message | None :
        """
        Load and deserialize a stored message document asynchronously \\
        Args:
            message_id : Message identifier
        Returns:
            Message subclass instance if present, else None.
        """
        p        = self.path_message(message_id)
        msg_dict = await self.json_read(p)
        msg_obj  = None
        
        if msg_dict :
            msg_bm = msg_dict.get("basemodel")
            if msg_bm and isinstance( msg_bm, str) :
                
                from . import basemodels
                MsgBM = getattr( basemodels, msg_bm, None)
                
                if MsgBM and issubclass( MsgBM, BaseModel) :
                    msg_obj = MsgBM.model_validate(msg_dict)
        
        return msg_obj
    
    async def message_write( self, message : Message) -> None :
        """
        Persist a message document under the case directory asynchronously \\
        Args:
            message : Message subclass instance
        """
        p = self.path_message(message.id)
        
        await self.json_write( p, message.model_dump())
        
        return
    
    async def media_get( self, filename : str) -> bytes | None :
        """
        Download stored media content for the active case asynchronously \\
        Args:
            filename : Media filename (with extension)
        Returns:
            Media content bytes or None if the file is absent.
        """
        p = self.dir_media() / filename
        
        return await async_b3_get_file(p) if await async_b3_exists(p) else None
    
    async def media_write( self,
                           message : UserContentMsg,
                           media   : MediaContent ) -> None :
        """
        Store media bytes associated with a user content message asynchronously \\
        Args:
            message : User message containing metadata (filename, mime)
            media   : Media content payload to persist
        """
        media_path = self.dir_media() / message.media.name
        
        if not await async_b3_exists(media_path) :
            media_content = media.content if media.content else b""
            await async_b3_put_media( media_path, media_content, media.mime)
        
        return
    
    # -------------------------------------------------------------------------------------
    # MANIFEST
    
    async def get_next_case_id(self) -> int :
        """
        Determine the next sequential case ID for the user asynchronously \\
        Returns:
            First positive integer not currently assigned to a case
        """
        max_id    = 0
        prefix    = self.dir_user() / "cases"
        case_dirs = await async_b3_list_directories(prefix)
        
        for case_dir_str in case_dirs :
            if case_dir_str.isdigit() :
                max_id = max( max_id, int(case_dir_str))
        
        return max_id + 1 if max_id > 0 else 1
    
    async def manifest_append( self,
                               manifest : CaseManifest,
                               message  : Message ) -> None :
        """
        Append message metadata to the manifest and refresh timestamps asynchronously \\
        Args:
            manifest : Case Manifest model for the active case
            message  : Message to record
        """
        if message.id not in manifest.message_ids :
            manifest.message_ids.append(message.id)
        
        existing_last = utc_iso_to_dt(manifest.time_last_message)
        msg_time      = utc_iso_to_dt(message.time_created) or  \
                        utc_iso_to_dt(message.time_received) or \
                        datetime.now(timezone.utc)
        
        if ( not existing_last ) or ( existing_last < msg_time ) :
            msg_time = msg_time.replace( microsecond = 0)
            msg_time = msg_time.isoformat().replace( "+00:00", "Z")
            manifest.time_last_message = msg_time
        
        await self.manifest_write(manifest)
        
        return
    
    async def manifest_load(self) -> CaseManifest | None :
        """
        Load the active case manifest from the bucket asynchronously \\
        Returns:
            CaseManifest instance if data exists, else None.
        """
        p    = self.path_manifest()
        data = await self.json_read(p)
        
        return CaseManifest.model_validate(data) if data else None
    
    async def manifest_write( self, manifest : CaseManifest) -> None :
        """
        Persist the manifest JSON for the active case asynchronously \\
        Args:
            manifest : CaseManifest instance
        """
        p = self.path_manifest()
        await self.json_write( p, manifest.model_dump())
        
        return
