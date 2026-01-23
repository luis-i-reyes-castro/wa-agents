"""
Digital Ocean Spaces S3 Bucket: Per-user directory lock
"""

import os
import time

from botocore.exceptions import ClientError
from pathlib import Path
from socket import gethostname
from types import TracebackType
from uuid import uuid4

from .do_bucket_io import ( b3_delete,
                            b3_list_objects,
                            b3_put_json )


class DOBucketLock :
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

    def __enter__( self ) -> "DOBucketLock" :
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
