"""
Digital Ocean Spaces S3 Bucket
----------------
Directory layout:
<operator_id>/
    <user_id>/
        user_data.json                  # UserData
        case_index.json                 # CaseIndex
        dedup/
            <idempotency_key.json>
            ...
        cases/
            <case_id>/
                case_manifest.json      # CaseManifest
                messages/
                    <message_id>.json   # Message
                    ...
                media/
                    <message_id>.<extension>
                    ...
            ...
"""

from datetime import ( datetime,
                       timezone )
from inspect import currentframe
from pathlib import Path
from pydantic import BaseModel
from typing import Any

from sofia_utils.io import load_json_string
from sofia_utils.stamps import utc_iso_to_dt

from .basemodels import ( CaseManifest,
                          MediaContent,
                          Message,
                          UserContentMsg )
from .do_bucket_io import ( b3_exists,
                            b3_get_file,
                            b3_list_directories,
                            b3_put_json,
                            b3_put_media )


class DOBucketStorage :
    
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
    # JSON I/O
    
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
