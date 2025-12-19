"""
Case Handler Base Class
"""
from abc import ( ABC,
                  abstractmethod )
from copy import deepcopy
from datetime import ( datetime,
                       timezone,
                       timedelta )
from inspect import currentframe

from sofia_utils.stamps import *
from sofia_utils.io import write_to_json_string

from .basemodels import ( AssistantMsg,
                          CaseIndex,
                          CaseManifest,
                          MediaData,
                          MediaContent,
                          Message,
                          ServerInteractiveOptsMsg,
                          ServerTextMsg,
                          ToolResultsMsg,
                          UserContentMsg,
                          UserData,
                          UserInteractiveReplyMsg,
                          UserMsg,
                          WhatsAppContact,
                          WhatsAppMetaData,
                          WhatsAppMsg )
from .do_bucket_storage import DOBucketStorage
from .do_bucket_lock import DOBucketLock
from .whatsapp_functions import ( send_whatsapp_text,
                                  send_whatsapp_interactive)


class CaseHandlerBase(ABC) :
    """
    Class for case and context management
    * Looks up user data
    * Decides when to open or close cases
    * Manages case manifest and index
    * Builds, scans and prints context
    * Provides unified message sending
    """
    
    MAX_CONTEXT_LEN  = 20
    TIME_LIMIT_STALE = 48
    
    def __init__( self,
                  operator : WhatsAppMetaData,
                  user     : WhatsAppContact,
                  debug    : bool = False ) -> None :
        
        self.operator_num = operator.display_phone_number
        self.operator_id  = operator.phone_number_id
        self.user_id      = user.wa_id
        self.user_name    = user.profile.name
        self.debug        = debug
        
        self.user_data     : UserData      = None
        self.case_id       : int           = None
        self.case_manifest : CaseManifest  = None
        self.case_context  : list[Message] = None
        
        self.storage      = DOBucketStorage( self.operator_num, self.user_id)
        self.storage_lock = DOBucketLock
        
        self.user_root = self.storage.dir_user()
        self.user_data_lookup()
        
        return
    
    # =====================================================================================
    # CASE MANAGEMENT
    # =====================================================================================
    
    def user_data_lookup(self) -> None :
        
        # Initialize user data update flag
        need_to_update = False
        # Look for user data
        p    = self.storage.path_user_data()
        data = self.storage.json_read(p)
        # If data found then load
        if data and isinstance( data, dict) :
            self.user_data = UserData.model_validate(data)
        # Else get user data from phone number
        else :
            self.user_data = UserData.from_phone_number(self.user_id)
            need_to_update = True
        
        # If user name is new then append to list of user names
        if self.user_name and ( self.user_name not in self.user_data.names ) :
            self.user_data.names.append(self.user_name)
            need_to_update = True
        
        # If necessary then update user data
        if need_to_update :
            with self.storage_lock(self.user_root) :
                self.storage.json_write( p, self.user_data.model_dump())
        
        return
    
    def case_decide(self) -> tuple[ int, CaseManifest] :
        """
        Decide case. Logic:
        * Look for open case index in root / <user_id> / index.json
        * IF open case index not found or is null then open new case
        * ELSE open case index was found
            * Load manifest
            * If manifest status is not open or if case stale:
                * Open new case
            * Else return case ID and manifest
        Returns:
            Case ID, Case Manifest
        """
        
        # Initialize open case index
        index = CaseIndex( open_case_id = None)
        # Look for open case index
        p    = self.storage.path_case_index()
        data = self.storage.json_read(p)
        if data and isinstance( data, dict) :
            index = CaseIndex( open_case_id = data.get("open_case_id"))
        
        # 1) IF open case index was not found or is null then open new case
        if not index.open_case_id :
            return self.case_open_new()
        
        # 2) ELSE open case index was found
        # 2-1) Load manifest
        self.storage.set_case_id(index.open_case_id)
        manifest = self.storage.manifest_load()
        
        # 2-2) If manifest status is not open then open new case
        if manifest.status != "open" :
            return self.case_open_new()
        
        # 2-3) If stale then open new case
        now  = datetime.now(timezone.utc)
        last = utc_iso_to_dt(manifest.time_last_message) or \
               utc_iso_to_dt(manifest.time_opened) or now
        if ( now - last ) > timedelta( hours = self.TIME_LIMIT_STALE) :
            manifest.status = "timeout"
            self.storage.manifest_write(manifest)
            return self.case_open_new()
        
        # 2-4) Else return case ID and manifest
        return manifest.case_id, manifest
    
    def case_open_new(self) -> tuple[ int, CaseManifest] :
        """
        Open new case
        Returns:
            Case ID, Case Manifest
        """
        # Query storage for next case ID
        case_id = self.storage.get_next_case_id()
        self.storage.set_case_id(case_id)
        
        # Initialize manifest
        manifest = CaseManifest( case_id = case_id)
        
        # Write manifest and set open case ID
        self.storage.manifest_write(manifest)
        self.case_set_open_case_id(case_id)
        
        return case_id, manifest
    
    def case_mark_as_resolved(self) -> None :
        """
        Mark current case as resolved
        """
        self.case_manifest.status      = "resolved"
        self.case_manifest.time_closed = get_now_utc_iso()
        
        self.storage.manifest_write(self.case_manifest)
        self.case_set_open_case_id(None)
        
        return
    
    def case_set_open_case_id( self, case_id : int | None) -> None :
        """
        Set open case ID
        Args:
            case_id: Open case ID or None
        """
        
        # Update root / <user_id> / index.json
        p = self.storage.path_case_index()
        self.storage.json_write( p, { "open_case_id": case_id })
        
        return
    
    # =====================================================================================
    # CONTEXT
    # =====================================================================================
    
    def context_build( self, truncate : bool = True) -> None :
        """
        Build context\n
        Args:
            truncate: Whether or not to enforce the max content length
            debug:    Debug mode flag
        """
        
        # Ensure we know which case to load
        if not ( self.case_id and self.case_manifest ) :
            self.case_id, self.case_manifest = self.case_decide()
        
        # Get messages in manifest
        self.case_context = []
        for msg_id in self.case_manifest.message_ids:
            message = self.storage.message_read(msg_id)
            if message :
                self.case_context.append(message)
        
        # Sort by time_created and time_received
        self.case_context.sort( key = lambda m : ( utc_iso_to_dt(m.time_created),
                                                   utc_iso_to_dt(m.time_received) ) )
        
        # Enforce max context length
        if truncate and ( len(self.case_context) > self.MAX_CONTEXT_LEN ) :
            self.case_context = self.case_context[ -self.MAX_CONTEXT_LEN : ]
        
        # The grand finale
        return
    
    def context_update( self, message : Message) -> None :
        """
        Update context:
        * Write message to storage
        * Append message to case manifest and re-write manifest
        * If the case context has been initialized then append message\n
        Args:
            message: Instance of a subclass of Message
            debug :  Debug mode flag
        """
        
        # Get user store and lock it
        with self.storage_lock(self.user_root) :
            # Write message JSON and append message to manifest
            self.storage.message_write(message)
            self.storage.manifest_append( self.case_manifest, message)
            # Mark idempotency key (after successful write)
            if message.idempotency_key :
                self.storage.dedup_write(message.idempotency_key)
        
        # Update context
        if self.case_context :
            self.case_context.append(message)
        
        return
    
    # =====================================================================================
    # MESSAGE DEDUP AND INGESTION
    # =====================================================================================
    
    def dedup_and_ingest_message( self,
                                  message       : WhatsAppMsg,
                                  media_content : MediaContent | None = None,
                                ) -> UserMsg | None :
        """
        Deduplicate and ingest a WhatsApp message
        """
        _orig_ = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        
        # If message already processed then return None
        if self.storage.dedup_exists(message.id) :
            return None
        
        # Determine target case_id
        self.case_id, self.case_manifest = self.case_decide()
        
        # Process text or media
        msg = None
        if message.text or message.media_data :
            
            text       = None
            media_data = None
            if message.text :
                text = message.text.body
            elif message.media_data :
                text       = message.media_data.caption
                media_data = MediaData.from_content(media_content)
            
            msg = UserContentMsg(
                    origin          = _orig_,
                    case_id         = self.case_id,
                    idempotency_key = message.id,
                    time_created    = unix_to_utc_iso(message.timestamp),
                    text            = text,
                    media           = media_data )
            msg.print()
        
        # Process Interactive Reply Message
        elif message.interactive :
            
            msg = UserInteractiveReplyMsg(
                    origin          = _orig_,
                    case_id         = self.case_id,
                    idempotency_key = message.id,
                    time_created    = unix_to_utc_iso(message.timestamp),
                    choice          = message.interactive.choice )
            msg.print()
        
        if msg :
            # Write message to storage and update manifest
            self.context_update(msg)
            # Write message media to storage
            if isinstance( msg, UserContentMsg) and msg.media :
                with self.storage_lock(self.user_root) :
                    self.storage.media_write( msg, media_content)
        
        # The grand finale
        return msg
    
    # =====================================================================================
    # MESSAGE SENDING FUNCTIONS
    # =====================================================================================
    
    def send_text( self,
                   message : ServerTextMsg | AssistantMsg | ToolResultsMsg
                 ) -> bool :
        
        try :
            # If not in debug mode: Send only Assistant Message text
            if not self.debug :
                if isinstance( message, ( ServerTextMsg, AssistantMsg)) \
                and message.text :
                    send_whatsapp_text( self.operator_id, self.user_id, message.text)
                return True
            
            # Debug mode: Assistant Message
            elif isinstance( message, ( ServerTextMsg, AssistantMsg)) :
                if message.text :
                    text_str = message.text
                    if len(text_str) > 4096 :
                        text_str = "[Result too long to display here]"
                    msg_display = "📝 Text:\n" + text_str
                    send_whatsapp_text( self.operator_id, self.user_id, msg_display)
                if isinstance( message, AssistantMsg) :
                    for tc in message.tool_calls :
                        msg_display = "🔧 Tool call:\n" \
                                    + write_to_json_string(tc.model_dump())
                        send_whatsapp_text( self.operator_id, self.user_id, msg_display)
            
            # Debug mode: Tool Results Message
            elif isinstance( message, ToolResultsMsg) :
                for tr in message.tool_results :
                    tr_str = write_to_json_string(tr.model_dump())
                    if len(tr_str) > 4096 :
                        tr_str = "[Result too long to display here]"
                    msg_display = "📊 Tool result:\n" + tr_str
                    send_whatsapp_text( self.operator_id, self.user_id, msg_display)
            
            return True
        
        except Exception as ex :
            print(f"In {self.__class__.__name__} send_message: {ex}")
        
        return False
    
    def send_interactive( self, message : ServerInteractiveOptsMsg) -> bool :
        
        if isinstance( message, ServerInteractiveOptsMsg) :
            try :
                if not self.debug :
                    send_whatsapp_interactive( self.operator_id, self.user_id, message)
                else :
                    message_cp      = deepcopy(message)
                    message_cp.body = "📝 Interactive Message:\n" \
                                    + str(message_cp.body)
                    send_whatsapp_interactive( self.operator_id, self.user_id, message)
                
                return True
            
            except Exception as ex :
                print(f"In {self.__class__.__name__} send_interactive: {ex}")
        
        return False
    
    # =====================================================================================
    # ABSTRACT METHODS TO BE IMPLEMENTED BY THE CASEHANDLER
    # =====================================================================================
    
    @abstractmethod
    def process_message( self,
                         message       : WhatsAppMsg,
                         media_content : MediaContent | None = None
                       ) -> bool :
        """
        Process WhatsApp message\n
        Args:
            message:       WhatsApp message object
            media_content: If WhatsApp message is a media message then use this field to pass the media content
        Returns:
            True if message needs to be replied to, False otherwise.
        """
        
        raise NotImplementedError
    
    @abstractmethod
    def generate_response( self,
                           max_tokens : int | None = None
                         ) -> bool :
        """
        Generate response\n
        Args:
            max_tokens: Optional maximum number of input+output tokens
        Returns:
            bool: True if need to generate more responses, False otherwise.
        """
        
        raise NotImplementedError
