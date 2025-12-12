"""
Case Handler Base Class
"""

from copy import deepcopy
from datetime import ( datetime,
                       timezone,
                       timedelta )

from sofia_utils.stamps import *
from sofia_utils.io import ( JSON_INDENT,
                             write_to_json_string )

from .basemodels import ( AssistantMsg,
                          Message,
                          ServerInteractiveOptsMsg,
                          ServerTextMsg,
                          ToolResultsMsg,
                          UserData,
                          CaseIndex,
                          CaseManifest )
from .DO_spaces_storage import DOSpacesBucket
from .DO_spaces_dirlock import DOSpacesLock
from .whatsapp_functions import ( send_whatsapp_text,
                                  send_whatsapp_interactive)


class CaseHandlerBase :
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
    
    def __init__( self, user_id : str, user_name : str | None = None) -> None :
        
        self.user_id   = user_id
        self.user_name = user_name
        
        self.user_data     : UserData      = None
        self.case_id       : int           = None
        self.case_manifest : CaseManifest  = None
        self.case_context  : list[Message] = None
        
        self.storage = DOSpacesBucket(self.user_id)
        self.DirLock = DOSpacesLock
        
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
            with self.DirLock(self.user_root) :
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
    
    def case_set_drone_model( self, drone_model : str | None) -> None :
        """
        Set current case drone model
        Args:
            drone_model: 'T40' | 'T50'
        """
        if drone_model and isinstance( drone_model, str) and \
           drone_model in self.tool_server.dkdb.MODELS_AVAILABLE :
            self.case_manifest.model = drone_model
            self.storage.manifest_write(self.case_manifest)
        
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
    
    def context_build( self, truncate : bool = True, debug : bool = False) -> None :
        """
        Build context
        Args:
            truncate: Whether or not to enforce the max content length
            debug:    Debug mode flag
        """
        
        # Get manifest
        if not self.case_manifest :
            self.case_manifest = self.storage.manifest_load()
        # Initialize DKDB
        if self.case_manifest and self.case_manifest.model \
                              and ( not self.tool_server.dkdb.model ) :
            self.tool_server.dkdb.set_model(self.case_manifest.model)
        
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
        
        # Scan for triggers
        self.triggers = self.state_machine.process( self.case_context, debug)
        
        # The grand finale
        return
    
    def context_print(self) -> str :
        """
        Generate a formatted string representation of the current case context.
        Returns:
            str: Formatted string with user ID and message details
        """
        if not self.case_id :
            self.case_id, self.case_manifest = self.case_decide()
        
        self.context_build( truncate = False)
        
        # Build the output string
        output = []
        output.append("USER DATA")
        output.append(self.user_data.model_dump_json( indent = JSON_INDENT))
        output.append("CONTEXT:")
        for i, msg in enumerate( self.case_context, 1) :
            output.append(f"MESSAGE {i}:")
            output.append(msg.model_dump_json( indent = JSON_INDENT))
        
        return "\n".join(output)
    
    def context_update( self,
                        message  : Message,
                        triggers : bool = True,
                        debug    : bool = False ) -> None :
        """
        Update context:
        * Write message to storage
        * Append message to case manifest and re-write manifest to storage
        * If the case context has been initialized then append message
        * If requested then update triggers
        
        Args:
            message:  Instance of a subclass of Message
            triggers: Whether or not to update triggers
            debug :   Debug mode flag
        """
        
        # Get user store and lock it
        with self.DirLock(self.user_root) :
            # Write message JSON and append message to manifest
            self.storage.message_write(message)
            self.storage.manifest_append( self.case_manifest, message)
            # Mark idempotency key (after successful write)
            if message.idempotency_key :
                self.storage.dedup_write(message.idempotency_key)
        
        # Update context
        if self.case_context :
            self.case_context.append(message)
        
        # Update triggers
        if triggers :
            self.triggers = self.state_machine.update( message, debug)
        
        return
    
    # =====================================================================================
    # SEND MESSAGE FUNCTION
    # =====================================================================================
    
    def send_text( self,
                   message : ServerTextMsg | AssistantMsg | ToolResultsMsg,
                   debug   : bool = False ) -> bool :
        
        try :
            # If not in debug mode: Send only Assistant Message text
            if not debug :
                if isinstance( message, ( ServerTextMsg, AssistantMsg)) \
                and message.text :
                    send_whatsapp_text( self.user_id, message.text)
                return True
            
            # Debug mode: Assistant Message
            elif isinstance( message, ( ServerTextMsg, AssistantMsg)) :
                if message.text :
                    text_str = message.text
                    if len(text_str) > 4096 :
                        text_str = "[Result too long to display here]"
                    msg_display = "📝 Text:\n" + text_str
                    send_whatsapp_text( self.user_id, msg_display)
                if isinstance( message, AssistantMsg) :
                    for tc in message.tool_calls :
                        msg_display = "🔧 Tool call:\n" \
                                    + write_to_json_string(tc.model_dump())
                        send_whatsapp_text( self.user_id, msg_display)
            
            # Debug mode: Tool Results Message
            elif isinstance( message, ToolResultsMsg) :
                for tr in message.tool_results :
                    tr_str = write_to_json_string(tr.model_dump())
                    if len(tr_str) > 4096 :
                        tr_str = "[Result too long to display here]"
                    msg_display = "📊 Tool result:\n" + tr_str
                    send_whatsapp_text( self.user_id, msg_display)
            
            return True
        
        except Exception as ex :
            print(f"In {self.__class__.__name__} send_message: {ex}")
        
        return False
    
    def send_interactive( self,
                          message : ServerInteractiveOptsMsg,
                          debug   : bool = False ) -> bool :
        
        if isinstance( message, ServerInteractiveOptsMsg) :
            try :
                if not debug :
                    send_whatsapp_interactive( self.user_id, message)
                else :
                    message_cp      = deepcopy(message)
                    message_cp.body = "📝 Interactive Message:\n" \
                                    + str(message_cp.body)
                    send_whatsapp_interactive( self.user_id, message)
                
                return True
            
            except Exception as ex :
                print(f"In {self.__class__.__name__} send_interactive: {ex}")
        
        return False
