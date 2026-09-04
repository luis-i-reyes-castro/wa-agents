"""
BaseModel Classes
"""

from abc import (
    ABC,
    abstractmethod,
)
from mimetypes import guess_type
from pathlib import Path
from pydantic import (
    BaseModel,
    Field,
    NonNegativeInt,
    ValidationError,
    model_validator,
)
from typing import (
    Annotated,
    Any,
    Literal,
    Self,
)

from sofia_utils.io import JSON_INDENT
from sofia_utils.printing import (
    print_ind,
    print_sep,
)
from sofia_utils.pydantic import NE_str
from sofia_utils.stamps import (
    generate_UUID,
    get_now_utc_iso,
    get_sha256,
)

from .phone_numbers import get_country_and_language
from .whatsapp_models import (
    WhatsAppInteractiveBody,
    WhatsAppInteractiveButtonLabel,
    WhatsAppInteractiveHeaderFooter,
    WhatsAppInteractiveOption,
    WhatsAppTemplateLanguageCode,
)


# -----------------------------------------------------------------------------------------
# USER DATA

class UserData(BaseModel) :
    """
    User data class
        `user_id`  : "<user_id>"
        `code_reg` : "<region code>" | null
        `code_lan` : "<language code>" | null
        `country`  : "<country>" | null
        `language` : "<language>" | null
        `name`     : "<name>" | null
    """
    user_id  : NE_str
    code_reg : NE_str | None = None
    code_lan : NE_str | None = None
    country  : NE_str | None = None
    language : NE_str | None = None
    names    : Annotated[ list[NE_str], Field( default_factory = list)]
    
    @classmethod
    def from_phone_number( cls, phone_number) -> "UserData" :
        
        result = get_country_and_language(phone_number)
        
        return cls( user_id  = phone_number,
                    code_reg = result.get("code_region"),
                    code_lan = result.get("code_language"),
                    country  = result.get("country_en"),
                    language = result.get("language_en") )

# -----------------------------------------------------------------------------------------
# MESSAGES (ABSTRACT BASE CLASSES)

class Message( BaseModel, ABC) :
    """
    Message abstract base class. \\
    Must implement abstract property `role`.
        `basemodel`       : "<class name (for deserialization)>"
        `origin`          : "<optional field for tracking purposes>" | null
        `idempotency_key` : "<provider message ID>",
        `time_created`    : "<timestamp>"
        `time_received`   : "<timestamp>"
        `id`              : "<timestamp>_<random B62 8-char string>" | null
    """
    basemodel       : NE_str | None = None
    origin          : NE_str | None = None
    idempotency_key : NE_str = Field( default_factory = generate_UUID)
    time_created    : NE_str = Field( default_factory = get_now_utc_iso)
    time_received   : NE_str = Field( default_factory = get_now_utc_iso)
    id              : NE_str | None = None
    
    def model_post_init( self, __context : Any) -> None :
        """
        Populate fields `basemodel` and `id`
        """
        self.basemodel = self.__class__.__name__
        
        if not self.id :
            
            time_str = str(self.time_received)
            time_str = time_str.replace( "T", "_").replace( ":", "-")
            time_str = time_str.replace( ".", "-").replace( "Z",  "")
            
            self.id  = f"{time_str}_{self.basemodel}"
        
        return
    
    def print(self) -> None :
        """
        Print itself
        """
        print_sep()
        print("[INFO] WA-AGENTS MESSAGE:")
        print(self.model_dump_json( indent = JSON_INDENT))
        return
    
    @property
    @abstractmethod
    def role(self) -> str :
        return NotImplementedError

class BasicMsg( Message, ABC) :
    """
    Basic Message abstract base class. \\
    Has field `text`.
    """
    text : str | None = None

class StructuredDataMsg( Message, ABC) :
    """
    Structured Data Message abstract base class. \\
    Must implement abstract method `as_text`.
    """
    @abstractmethod
    def as_text(self) -> str :
        return NotImplementedError

# -----------------------------------------------------------------------------------------
# CLASSES FOR MESSAGE CONTENTS: MEDIA, TOOL CALLS AND TOOL RESULTS

class MediaBase( BaseModel, ABC) :
    """
    Media (Abstract Base Class)
        `mime` : "<MIME type>"
    """
    mime : NE_str
    
    @property
    def extension(self) -> str :
        return self.mime.split("/")[1]
    
    @property
    def type(self) -> str :
        return self.mime.split("/")[0]

class MediaContent(MediaBase) :
    """
    Media Content
        `content` :  <bytes>
    """
    content : bytes

class MediaData(MediaBase) :
    """
    Media Data
        `name`   : "<filename>_<index>.<extension>",
        `sha256` : "<hash of attachment content>" | null,
        `size`   :  <attachment size> | null
    """
    name   : NE_str
    sha256 : NE_str | None = None
    size   : NonNegativeInt | None = None
    
    @classmethod
    def from_content( cls, media_content : MediaContent) -> "MediaData" :
        """
        Instantiate a `MediaData` object from a `MediaContent` object
        """
        return cls( name   = cls.__class__.__name__,
                    mime   = media_content.mime,
                    sha256 = get_sha256(media_content.content),
                    size   = len(media_content.content) )

def load_media( path : str | Path) -> tuple[ MediaData, MediaContent] :
    """
    Load a media file from disk and produce matching Media models \\
    Args:
        path : Filesystem path to the media file
    Returns:
        Tuple of ( MediaData, MediaContent); each element may be None if invalid.
    """
    
    media_path = Path(path)
    media_mime = guess_type(media_path.name)[0]
    media_cont = media_path.read_bytes()
    
    if not ( media_mime and media_cont ) :
        return None, None
    
    md_obj = MediaData( mime   = media_mime,
                        name   = media_path.name,
                        sha256 = get_sha256(media_cont),
                        size   = len(media_cont))
    
    mc_obj = MediaContent( mime    = media_mime,
                           content = media_cont)
    
    return md_obj, mc_obj

class OutgoingMediaMsg(MediaBase) :
    """
    Outgoing Media Message
    """
    filepath  : NE_str
    content   : bytes
    caption   : NE_str | None = None
    upload_id : NE_str | None = None

class OutgoingDocumentMsg(OutgoingMediaMsg) :
    """
    Outgoing PDF Document Message
    """
    mime     : Literal["application/pdf"] = "application/pdf"
    filename : NE_str | None = None
    
    def model_post_init( self, __context : Any) -> None :
        
        if not self.filename :
            self.filename = Path(self.filepath).name
        
        return
    
    @property
    def type(self) -> str :
        return "document"

class ToolCall(BaseModel) :
    """
    Tool Call
        `id`    : "<tool call ID>",
        `name`  : "<tool name>" | null,
        `input` :  <tool input object as per schema> | null
    """
    id    : NE_str = Field( default_factory = generate_UUID)
    name  : NE_str = Field( default = "tool_name")
    input : Annotated[ dict[ NE_str, Any] | None, Field( default_factory = dict)]

class ToolResult(BaseModel) :
    """
    Tool Result
        `id`      : "<tool call ID>",
        `content` : "<tool call result>" | null,
        `error`   : false | true | null,
        `_silent` : true | false | null
    NOTE:
        Set `_silent = True` when the tool already produced the intended
        user-facing side effect on its own, for example sending a WhatsApp
        contact card, location card, or interactive list. Case handlers can use
        this flag to stop the agent loop after recording the tool result,
        preventing an unnecessary follow-up assistant message.
    """
    id      : NE_str
    content : Any  | None = None
    error   : bool | None = False
    _silent : bool | None = None

# -----------------------------------------------------------------------------------------
# USER MESSAGES

class UserMsg( BasicMsg, ABC) :
    """
    User Message
    """
    @property
    def role(self) -> str :
        return "user"

class UserContentMsg(UserMsg) :
    """
    User Message containing either text or media
    """
    media : MediaData | None = None
    
    def model_post_init( self, __context : Any) -> None :
        
        super().model_post_init(__context)
        
        if self.media :
            self.media.name = f"{self.id}.{self.media.extension}"
        
        return
    
    @model_validator( mode = "after")
    def check_nonempty(self) -> Self :
        if not ( self.text or self.media ) :
            raise ValueError(f"In {self.basemodel}: No text or media")
        return self

class UserInteractiveReplyMsg( UserMsg, StructuredDataMsg) :
    """
    User Interactive Reply Message ( User -> Server )
    """
    choice : WhatsAppInteractiveOption
    
    def as_text(self) -> str :
        return self.choice.model_dump_json()

# -----------------------------------------------------------------------------------------
# SERVER MESSAGES

class ServerMsg( Message, ABC) :
    """
    Server Message
    """
    
    is_state : bool = False
    """
    Message persists handler-owned internal replay state and should be excluded from both the user view and the LLM context.
    """
    
    user_eyes : bool = False
    """
    Message is intended only for the end user and should usually be excluded from LLM context. Typical examples are ephemeral UX copy such as "Agent thinking..." or "Looking up in database...".
    """
    
    @property
    def role(self) -> str :
        return "user"

class ServerTextMsg( ServerMsg, BasicMsg) :
    """
    Server Text Message
    """
    pass

class ServerInteractiveOptsMsg( ServerMsg, StructuredDataMsg) :
    """
    Server Interactive Options Message ( Server -> User )
    """
    type    : Literal[ "button", "list"]
    header  : WhatsAppInteractiveHeaderFooter | None = None
    body    : WhatsAppInteractiveBody
    footer  : WhatsAppInteractiveHeaderFooter | None = None
    button  : WhatsAppInteractiveButtonLabel  | None = None
    options : Annotated[ list[WhatsAppInteractiveOption],
                         Field( min_length = 1, default_factory = list)]
    
    @model_validator( mode = "after")
    def validate_message(self) -> Self :
        
        e_msg = f"In {self.basemodel}: "
        
        if self.type == "button" and len(self.options) > 3 :
            e_msg += "Type 'button' only supports up to 3 options"
            raise ValueError(e_msg)
        
        elif self.type == "list" and len(self.options) > 10 :
            e_msg += "Type 'list' only supports up to 10 options"
            raise ValueError(e_msg)
        
        if self.type == "button" :
            if self.button is not None :
                e_msg += "Type 'button' must not define field 'button'"
                raise ValueError(e_msg)
            if any( opt.description is not None for opt in self.options ) :
                e_msg += "Type 'button' options do not support descriptions"
                raise ValueError(e_msg)
            if any( len(opt.title) > 20 for opt in self.options ) :
                e_msg += "Type 'button' option titles support a max length of 20 chars"
                raise ValueError(e_msg)
        
        elif self.type == "list" :
            if not self.button :
                e_msg += "Type 'list' requires non-empty field 'button'"
                raise ValueError(e_msg)
        
        return self
    
    def as_text(self) -> str :
        return self.model_dump_json( include = { "header", "body", "options" })
    
    @property
    def opts_str(self) -> str :
        return "_".join( opt.id for opt in self.options )

class ServerTemplateMsg( ServerMsg, StructuredDataMsg) :
    """
    Server Template Message ( Server -> User )
    """
    
    name       : NE_str
    language   : WhatsAppTemplateLanguageCode
    parameters : Annotated[ list[str] | dict[ str, str], Field( min_length = 1)]
    
    def as_text(self) -> str :
        return self.model_dump_json(
            include = { "name", "language", "parameters" },
            exclude_none = True,
        )

# -----------------------------------------------------------------------------------------
# ASSISTANT MESSAGES

class AssistantMsg(BasicMsg) :
    """
    Assistant (AI/LLM) Message
    """
    tool_calls : Annotated[ list[ToolCall], Field( default_factory = list)]
    st_output  : dict | None = None
    st_out_bm  : str  | None = None
    
    agent         : NE_str | None = None
    api           : NE_str | None = None
    model         : NE_str | None = None
    tokens_input  : NonNegativeInt | None = None
    tokens_output : NonNegativeInt | None = None
    tokens_total  : NonNegativeInt | None = None
    instructions  : NE_str | None = None
    tools         : list[Any]    | None = None
    context       : list[NE_str] | None = None
    
    def append_to_text( self, text_block : str | None) -> None :
        
        if text_block and isinstance( text_block, str): 
            if not self.text :
                self.text = text_block
            else :
                if self.text.endswith("\n") :
                    self.text += ( "\n" + text_block )
                else :
                    self.text += ( "\n\n" + text_block )
        
        return
    
    def is_empty(self) -> bool :
        return not bool( self.text or self.tool_calls or self.st_output )
    
    @property
    def role(self) -> str :
        return "assistant"

# -----------------------------------------------------------------------------------------
# TOOL RESULTS MESSAGES

class ToolResultsMsg(Message) :
    """
    Tool Results Message
    """
    tool_results : Annotated[ list[ToolResult],
                              Field( min_length = 1, default_factory = list)]
    
    @property
    def role(self) -> str :
        return "tool"

# -----------------------------------------------------------------------------------------
# CASE INDEX AND MANIFEST

class CaseIndex(BaseModel) :
    """
    Open Case Index
        `open_case_id` : <case ID>
    """
    open_case_id : NonNegativeInt | None = None

class CaseManifest(BaseModel) :
    """
    Manifest
        `case_id`           : <case ID>,
        `model`             : "T40" | "T50" | null
        `status`            : "open" | "resolved" | "timeout",
        `time_opened`       : "<timestamp>",
        `time_last_message` : "<timestamp>" | null,
        `time_closed`       : "<timestamp>" | null,
        `message_ids`       : [ "<message ID>", ... ],
    """
    case_id           : NonNegativeInt
    model             : NE_str | None = None
    status            : NE_str        = "open"
    time_opened       : NE_str = Field( default_factory = get_now_utc_iso)
    time_last_message : NE_str | None = None
    time_closed       : NE_str | None = None
    message_ids       : Annotated[ list[NE_str], Field( default_factory = list)]

# =========================================================================================
# UTILITY FUNCTIONS
# =========================================================================================

def is_llm_readable( message : Message) -> bool :
    
    return not (
        isinstance( message, ServerMsg) and
        ( message.is_state or message.user_eyes )
    )

def llm_context_len( context : list[Message]) -> int :
    
    return sum( is_llm_readable(message) for message in context )

def llm_context_truncate(
    messages : list[Message],
    max_len  : int | None,
) -> list[Message] :
    
    if ( not max_len ) or ( llm_context_len(messages) <= max_len ) :
        return messages
    
    pending_tool_result = False
    context             = []
    count               = 0
    
    for message in reversed(messages) :
        
        context.append(message)
        
        if is_llm_readable(message) :
            count += 1
        
        if isinstance( message, ToolResultsMsg) :
            pending_tool_result = True
        elif isinstance( message, AssistantMsg) and message.tool_calls :
            pending_tool_result = False
        
        if count >= max_len and not pending_tool_result :
            break
    
    return list(reversed(context))

def print_validation_errors( validation_error : ValidationError,
                             indent           : int = JSON_INDENT) -> None :
    """
    Pretty-print pydantic validation errors with indentation \\
    Args:
        validation_error : ValidationError object raised by pydantic
        indent           : Indentation level when printing
    """
    
    for error in validation_error.errors() :
        
        location_raw = error.get( "loc", ())
        if location_raw :
            location = " -> ".join( str(part) for part in location_raw )
        else :
            location = "<root>"
        
        message = error.get( "msg", "Validation error")
        
        print_ind( f"Location : {location}", indent)
        print_ind( f"Message  : {message}",  indent)
    
    return
