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
from .DO_spaces_io import ( b3_exists,
                            b3_get_file,
                            b3_list_directories,
                            b3_put_json,
                            b3_put_media )


class DOSpacesBucket :
    
    # -------------------------------------------------------------------------------------
    # ORIGINAL CLASS DEFINITION
    # -------------------------------------------------------------------------------------
    
    # Set user ID
    def __init__( self,
                  operator_id : str | int,
                  user_id     : str | int ) -> None :
        
        self.operator_id = str(operator_id)
        self.user_id     = str(user_id)
        
        return
    
    # Set case ID
    def set_case_id( self, case_id : str | int) -> None :
        
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
        Returns: <operator_id> / <user_id>
        """
        p = Path(self.operator_id) / Path(self.user_id)
        return p
    
    def dir_case(self) -> Path :
        """
        Returns: <operator_id> / <user_id> / cases / <case_id>
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
        Returns: <operator_id> / <user_id> / dedup
        """
        p = self.dir_user() / "dedup"
        return p
    
    def dir_messages(self) -> Path :
        """
        Returns: <operator_id> / <user_id> / cases / <case_id> / messages
        """
        p = self.dir_case() / "messages"
        return p
    
    def dir_media(self) -> Path :
        """
        Returns: <operator_id> / <user_id> / cases / <case_id> / media
        """
        p = self.dir_case() / "media"
        return p
    
    # -------------------------------------------------------------------------------------
    # PATHS TO FILES
    
    def path_user_data(self) -> Path :
        """
        Returns: ... / user_data.json
        """
        p = self.dir_user() / "user_data.json"
        return p
    
    def path_case_index(self) -> Path :
        """
        Returns: ... / case_index.json
        """
        p = self.dir_user() / "case_index.json"
        return p
    
    def path_manifest(self) -> Path :
        """
        Returns: ... / cases / <case_id> / case_manifest.json
        """
        p = self.dir_case() / "case_manifest.json"
        return p
    
    def path_message( self, message_id : str) -> Path :
        """
        Returns: ... / cases / <case_id> / messages / <message_id>.json
        """
        p = self.dir_messages() / f"{message_id}.json"
        return p
    
    # -------------------------------------------------------------------------------------
    # JSON I/O
    
    def json_read( self, path : Path) -> Any | None :
        
        if b3_exists(path) :
            return load_json_string(b3_get_file(path))
        
        return None
    
    def json_write( self, path : Path, obj : Any) -> None :
        
        b3_put_json( path, obj)
        
        return
    
    # -------------------------------------------------------------------------------------
    # DEDUPLICATION
    # -------------------------------------------------------------------------------------
    
    def dedup_exists( self, idempotency_key : str) -> bool :
        """
        Check if idempotency key path exists\n
        Args:
            idempotency_key: Idempotency key
        Returns:
            True if path found, else False.
        """
        p = self.dir_dedup() / f"{idempotency_key}.json"
        
        return b3_exists(p)
    
    def dedup_write( self, idempotency_key : str) -> None :
        """
        Write idempotency key to dedup dir\n
        Args:
            idempotency_key: Idempotency key
        """
        p = self.dir_dedup() / f"{idempotency_key}.json"
        b3_put_json( p, True)
        
        return
    
    # -------------------------------------------------------------------------------------
    # MESSAGE I/O
    # -------------------------------------------------------------------------------------
    
    def message_read( self, message_id : str) -> Message | None :
        """
        Read from message path\n
        Args:
            message_id: Message ID
        Returns:
            Message if path exists, else None.
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
        Write message\n
        Args:
            message: Message to write
        """
        p = self.path_message(message.id)
        
        self.json_write( p, message.model_dump())
        
        return
    
    def media_get( self, filename : str) -> bytes | None :
        """
        Retrieve media as bytes\n
        Args:
            filename: Media filename
        Returns:
            Media content as bytes, or None if not found.
        """
        p = self.dir_media() / filename
        
        return b3_get_file(p) if b3_exists(p) else None
    
    def media_write( self,
                     message : UserContentMsg,
                     media   : MediaContent ) -> None :
        """
        Write media contents to storage and return media data\n
        Args:
            message: User Message object with non-empty 'media' field
            media:   Media contents object
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
        If necessary:
            Append message to manifest (enforcing idempotency)
            Update manifest time of last message.
        Args:
            manifest: Case Manifest
            message:  Message
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
        Load manifest\n
        Returns
            Case Manifest if data exists, else None
        """
        p    = self.path_manifest()
        data = self.json_read(p)
        
        return CaseManifest.model_validate(data) if data else None
    
    def manifest_write( self, manifest : CaseManifest) -> None :
        """
        Write manifest\n
        Args:
            manifest: Case Manifest
        """
        p = self.path_manifest()
        self.json_write( p, manifest.model_dump())
        
        return
