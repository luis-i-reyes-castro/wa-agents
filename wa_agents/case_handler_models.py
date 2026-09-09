"""
BaseModel Classes
"""

from abc import (
    ABC,
    abstractmethod,
)
from datetime import (
    UTC,
    datetime,
)
from mimetypes import guess_type
from pathlib import Path
from pydantic import (
    BaseModel,
    Field,
    NonNegativeInt,
    model_validator,
)
from typing import (
    Annotated,
    Any,
    Literal,
    Self,
)

from sofia_utils.io import JSON_INDENT
from sofia_utils.printing import print_sep
from sofia_utils.pydantic import (
    MIME_Type,
    NE_str,
    NE_var_name,
    SHA256_Hex,
)
from sofia_utils.stamps import (
    generate_UUID,
    get_sha256,
)

from .phone_numbers import get_country_and_language
from .whatsapp_models import (
    WhatsAppInteractiveBody,
    WhatsAppInteractiveButtonLabel,
    WhatsAppInteractiveHeaderFooter,
    WhatsAppInteractiveOption,
    WhatsAppTemplateLanguageCode,
    WhatsAppUsername,
)


# -----------------------------------------------------------------------------------------
# USER DATA

class LanguageRegionData (BaseModel) :
    """
    Language and region data class
        `code_lan` : "<language code>" | null
        `code_reg` : "<region code>"   | null
        `language` : "<language>"      | null
        `country`  : "<country>"       | null
    """
    code_lan : NE_str | None = None
    code_reg : NE_str | None = None
    language : NE_str | None = None
    country  : NE_str | None = None
    
    @classmethod
    def from_phone_number( cls, phone_number) -> Self :
        
        result = get_country_and_language(phone_number)
        
        return cls(
            code_lan = result.get("code_language"),
            code_reg = result.get("code_region"),
            language = result.get("language_en"),
            country  = result.get("country_en"),
        )

class UserData (BaseModel) :
    """
    User data class
        `id`           : wa_api_contacts.id
        `name`         : "<name>"              | null
        `username`     : "<WhatsApp username>" | null
        `lan_reg_data` : <LanguageRegionData>  | null
    """
    id           : NonNegativeInt
    name         : str                | None = None
    username     : WhatsAppUsername   | None = None
    lan_reg_data : LanguageRegionData | None = None
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if not ( self.name or self.username ) :
            raise ValueError(
                f"In {self.__class__.__name__}: "
                f"Missing both fields 'name' and 'username'"
            )
        
        return self

# -----------------------------------------------------------------------------------------
# MESSAGES (ABSTRACT BASE CLASSES)

class Message ( BaseModel, ABC) :
    """
    Message abstract base class.
        `id`        : wa_case_handler_messages.id | null
        `ts`        : <timestamp>
        `basemodel` : "<class name>"              | null
        `origin`    : "<optional string>"         | null
    NOTE:
        Must implement abstract property `role`.
    """
    id        : NonNegativeInt | None = None
    ts        : datetime = Field( default_factory = lambda : datetime.now(UTC))
    basemodel : NE_str         | None = None
    origin    : NE_str         | None = None
    
    def model_post_init( self, __context : Any) -> None :
        """
        Populates `basemodel` with the class name
        """
        self.basemodel = self.__class__.__name__
        
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

class BasicMsg ( Message, ABC) :
    """
    Basic Message ABC. Only field is `text`.
    """
    text : str | None = None

class StructuredDataMsg ( Message, ABC) :
    """
    Structured Data Message ABC. No fields. \\
    NOTE:
        Must implement method `as_text`.
    """
    @abstractmethod
    def as_text(self) -> str :
        return NotImplementedError

# -----------------------------------------------------------------------------------------
# MEDIA: Unites the former MediaBase, MediaContent and MediaData

class MediaObject (BaseModel) :
    """
    Media Object
        `mime`    : "<MIME type>"
        `name`    : "<filename>_<index>.<extension>",
        `sha256`  : "<hash of attachment content>" | null,
        `size`    :  <attachment size>             | null
        `content` :  <bytes (if available)>        | null
    NOTE:
        Field `content` is excluded from serialization
    FORMERLY:
        * `MediaBase` owned `mime`
        * Both `MediaContent` and `MediaData` inherited from MediaBase
        * `MediaContent` owned `content`
            * Used to model bytes from/to the WhatsApp API
        * `MediaData` owned `name`, `sha256`, `size`
            * Used to model metadata without manipulating the underlying bytes
    """
    mime    : MIME_Type
    name    : NE_str         | None = None
    sha256  : SHA256_Hex     | None = None
    size    : NonNegativeInt | None = None
    content : bytes          | None = Field( default = None, exclude = True)
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if not ( self.name or self.content ) :
            raise ValueError(f"In {self.__class__.__name__}: No name or content")
        
        return self
    
    @property
    def extension(self) -> str :
        return self.mime.split("/")[1]
    
    @property
    def type(self) -> str :
        return self.mime.split("/")[0]

def load_media( path : str | Path) -> MediaObject | None :
    """
    Load a media file from disk into a `MediaObject` \\
    Args:
        path : Filesystem path to the media file
    Returns:
        Media object containing its metadata and bytes, or None if the MIME type
        is unknown or the file is empty.
    """
    media_path = Path(path)
    media_mime = guess_type(media_path.name)[0]
    media_cont = media_path.read_bytes()
    
    if not ( media_mime and media_cont ) :
        return None
    
    media = MediaObject(
        mime    = media_mime,
        name    = media_path.name,
        sha256  = get_sha256(media_cont),
        size    = len(media_cont),
        content = media_cont,
    )
    
    return media

# -----------------------------------------------------------------------------------------
# TOOL CALLS & TOOL RESULTS

class ToolCall (BaseModel) :
    """
    Tool Call
        `id`    : "<tool call ID>",
        `name`  : "<tool name>" | null,
        `input` :  <tool input object as per schema> | null
    """
    id    : NE_str = Field( default_factory = generate_UUID)
    name  : NE_str = Field( default = "tool_name")
    input : Annotated[ dict[ NE_str, Any] | None, Field( default_factory = dict)]

class ToolResult (BaseModel) :
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

class UserMsg ( BasicMsg, ABC) :
    """
    User Message
    """
    @property
    def role(self) -> str :
        return "user"

class UserContentMsg (UserMsg) :
    """
    User Message containing either text or media
    """
    media : MediaObject | None = None
    
    def model_post_init( self, __context : Any) -> None :
        
        super().model_post_init(__context)
        
        if ( self.id and self.media ) :
            self.media.name = f"{self.id}.{self.media.extension}"
        
        return
    
    @model_validator( mode = "after")
    def check_nonempty(self) -> Self :
        if not ( self.text or self.media ) :
            raise ValueError(f"In {self.basemodel}: No text or media")
        return self

class UserInteractiveReplyMsg ( UserMsg, StructuredDataMsg) :
    """
    User Interactive Reply Message ( User -> Server )
    """
    choice : WhatsAppInteractiveOption
    
    def as_text(self) -> str :
        return self.choice.model_dump_json()

# -----------------------------------------------------------------------------------------
# SERVER MESSAGES

class ServerMsg ( Message, ABC) :
    """
    Server Message
    """
    
    user_eyes : bool = False
    """
    Message is intended only for the end user and should usually be excluded from
    LLM context. Typical examples are ephemeral UX copy such as "Agent thinking..."
    or "Looking up in database...".
    """
    
    @property
    def role(self) -> str :
        return "user"

class ServerTextMsg ( ServerMsg, BasicMsg) :
    """
    Server Text Message
    """
    pass

class ServerInteractiveOptsMsg ( ServerMsg, StructuredDataMsg) :
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

class ServerTemplateMsg ( ServerMsg, StructuredDataMsg) :
    """
    Server Template Message ( Server -> User )
    """
    
    name       : NE_str
    language   : WhatsAppTemplateLanguageCode
    parameters : Annotated[
                    list[NE_str] | dict[ NE_var_name, NE_str],
                    Field( min_length = 1),
                 ]
    
    def as_text(self) -> str :
        return self.model_dump_json(
            include = { "name", "language", "parameters" },
            exclude_none = True,
        )

class ServerMediaMsg ( MediaObject, ServerMsg) :
    """
    Server Media Message
    """
    user_eyes : Literal[True] = True
    filepath  : NE_str
    caption   : NE_str | None = None
    upload_id : NE_str | None = None
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if not self.name :
            self.name = Path(self.filepath).name
        
        return super().validate()

class ServerDocumentMsg (ServerMediaMsg) :
    """
    Server PDF Document Message
    """
    mime     : Literal["application/pdf"] = "application/pdf"
    filename : NE_str | None = None
    
    def model_post_init( self, __context : Any) -> None :
        
        super().model_post_init(__context)
        
        if not self.filename :
            self.filename = Path(self.filepath).name
        
        return
    
    @property
    def type(self) -> str :
        return "document"

# -----------------------------------------------------------------------------------------
# ASSISTANT MESSAGES

class AssistantMsg (BasicMsg) :
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
    instructions  : str            | None = None
    tools         : list[Any]      | None = None
    context       : list[NE_str]   | None = None
    
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

class ToolResultsMsg (Message) :
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

class CaseManifest (BaseModel) :
    """
    Manifest
        `id`            : wa_case_handler_case_manifests.id
        `contact`       : wa_case_handler_case_manifests.contact
        `created_at`    : <timestamp>
        `updated_at`    : <timestamp> | null
        `is_open`       : bool
        `machine_state` : "<optional string>" | null
    """
    id            : NonNegativeInt
    contact       : NonNegativeInt
    created_at    : datetime = Field( default_factory = lambda : datetime.now(UTC))
    updated_at    : datetime | None = None
    is_open       : bool            = True
    machine_state : NE_str   | None = None
    
    message_ids   : Annotated[ list[NonNegativeInt], Field( default_factory = list)]

# =========================================================================================
# UTILITY FUNCTIONS
# =========================================================================================

def is_llm_readable( message : Message) -> bool :
    
    return not ( isinstance( message, ServerMsg) and message.user_eyes )

def llm_context_truncate(
    messages : list[Message],
    max_len  : int | None,
) -> list[Message] :
    
    if (
        ( not max_len ) or
        ( sum( is_llm_readable(msg) for msg in messages ) <= max_len )
    ) :
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
