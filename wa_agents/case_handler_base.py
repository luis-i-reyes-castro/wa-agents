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
from typing import Type

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
from .state_machine_base import StateMachineBase
from .whatsapp_functions import ( send_whatsapp_text,
                                  send_whatsapp_interactive)


class CaseHandlerBase(ABC) :
    """
    Class for case and context management
    * Looks up user data
    * Decides when to open or close cases
    * Manages case index and manifest
    * Builds, scans and prints context
    * Provides unified message sending
    """
    
    MAX_CONTEXT_LEN  = 20
    """ Maximum context length (in number of messages) """
    
    TIME_LIMIT_STALE = 48
    """ Case staleness time limit (in hours) """
    
    def __init__( self,
                  operator : WhatsAppMetaData,
                  user     : WhatsAppContact,
                  debug    : bool = False ) -> None :
        """
        Initialize the handler with WhatsApp metadata and user info \\
        Args:
            operator : WhatsApp metadata payload describing the business account
            user     : WhatsApp contact representing the end user
            debug    : When True, send verbose WhatsApp copies of assistant output
        """
        
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
        
        self.state_machine : Type[StateMachineBase] = None
        
        return
    
    # =====================================================================================
    # CASE MANAGEMENT
    # =====================================================================================
    
    def user_data_lookup(self) -> None :
        """
        Load or initialize UserData for the WhatsApp contact \\
        Ensures the latest name list persists back to storage with locking.
        """
        
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
        Decide whether to continue on the open case or create a new one.
        
        Logic:
        * Try to load `case_index.json` to find an open case id
        * If `case_index.json` does not exist, or if the case index is `null`, then open a new case.
        * Otherwise try to load `case_manifest.json`
        * If `case_manifest.json` exists, status is `open` and case is not stale (i.e., time since last message not greater than `TIME_LIMIT_STALE`), then continue on that case.
        * Otherwise open a new case.
        
        Returns:
            Tuple of `(case_id, CaseManifest)` describing the active case.
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
        Create a new case manifest and update the open-case index \\
        Returns:
            Tuple of `(case_id, CaseManifest)` for the newly opened case.
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
        Mark the active case as resolved and persist the closing timestamp
        """
        self.case_manifest.status      = "resolved"
        self.case_manifest.time_closed = get_now_utc_iso()
        
        self.storage.manifest_write(self.case_manifest)
        self.case_set_open_case_id(None)
        
        return
    
    def case_set_open_case_id( self, case_id : int | None) -> None :
        """
        Update the open case index \\
        Args:
            case_id : Either open case ID or None
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
        Build the ordered case context and feed it to the state machine \\
        Args:
            truncate : Whether to limit context to the last `MAX_CONTEXT_LEN` messages
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
        
        # Feed context to state machine
        if self.state_machine :
            self.state_machine.reset()
            for message in self.case_context :
                self.state_machine.ingest_message(message)
        
        # The grand finale
        return
    
    def context_update( self, message : Message) -> None :
        """
        Persist a new message and refresh the in-memory context
        
        Steps:
        * Write the message JSON to storage
        * Append it to the manifest (with locking)
        * If case context available then append message
        * If state machine available then feed message to machine
        
        Args:
            message : Instance of a Message subclass
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
        
        # Update state machine
        if self.state_machine :
            self.state_machine.ingest_message(message)
        
        return
    
    # =====================================================================================
    # MESSAGE DEDUP AND INGESTION
    # =====================================================================================
    
    def dedup_and_ingest_message( self,
                                  message       : WhatsAppMsg,
                                  media_content : MediaContent | None = None,
                                ) -> UserMsg | None :
        """
        Convert a webhook payload into domain messages, skipping duplicates \\
        Args:
            message       : WhatsApp webhook message
            media_content : Fetched media bytes when applicable
        Returns:
            User message object queued for processing, or None if duplicate.
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
                    idempotency_key = message.id,
                    time_created    = unix_to_utc_iso(message.timestamp),
                    text            = text,
                    media           = media_data )
            msg.print()
        
        # Process Interactive Reply Message
        elif message.interactive :
            
            msg = UserInteractiveReplyMsg(
                    origin          = _orig_,
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
        """
        Send textual assistant output or debugging artifacts via WhatsApp \\
        Args:
            message : Assistant, server, or tool-results message to deliver
        Returns:
            True on success; False if sending fails.
        """
        
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
        """
        Send WhatsApp interactive messages or their debug copy \\
        Args:
            message : Interactive options payload ready for WhatsApp APIs
        Returns:
            True on success; False otherwise.
        """
        
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
        Process a single WhatsApp webhook message \\
        Args:
            message       : WhatsApp message object
            media_content : Media content associated with the message (if any)
        Returns:
            True if additional responses should be generated; else False.
        """
        
        raise NotImplementedError
    
    @abstractmethod
    def generate_response( self,
                           max_tokens : int | None = None
                         ) -> bool :
        """
        Generate an assistant reply (potentially multi-turn) \\
        Args:
            max_tokens : Optional maximum number of tokens
        Returns:
            True if another response pass is required; else False.
        """
        
        raise NotImplementedError
