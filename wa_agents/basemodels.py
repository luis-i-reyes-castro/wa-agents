"""
BaseModel Classes
"""

from abc import ( ABC,
                  abstractmethod )
from decimal import Decimal
from pydantic import ( BaseModel,
                       Field,
                       model_validator,
                       ValidationError )
from typing import ( Annotated,
                     Any,
                     Literal,
                     Self )

from sofia_utils.io import JSON_INDENT
from sofia_utils.phone_numbers import get_country_and_language
from sofia_utils.printing import print_sep
from sofia_utils.stamps import ( generate_UUID,
                                 get_now_utc_iso,
                                 get_repo_main_hash,
                                 get_sha256 )


# =========================================================================================
# COMMON TYPES AND BASEMODELS
# =========================================================================================

type NN_Decimal  = Annotated[ Decimal, Field( ge = 0)]
type NN_int      = Annotated[ int,     Field( ge = 0)]
type NE_str      = Annotated[ str,     Field( min_length = 2)]
type NE_list_str = Annotated[ list[ NE_str ],     Field( min_length = 1)]
type NE_dict_str = Annotated[ dict[ NE_str, Any], Field( min_length = 1)]

class InteractiveOption(BaseModel) :
    """
    Interactive Message Option
    """
    id    : NE_str
    title : NE_str

# =========================================================================================
# WHATSAPP BASEMODELS
# =========================================================================================

# MESSAGES

class WhatsAppContext(BaseModel) :
    
    # Message is a reply to a previous message
    user : NE_str | None = Field( alias = "from", default = None)
    id   : NE_str | None = None
    # Message was forwarded
    forwarded            : bool | None = None
    frequently_forwarded : bool | None = None
    # Message refers to a product in the catalog
    referred_product : dict[ str, str] | None = None

class WhatsAppText(BaseModel) :
    
    body : NE_str

class WhatsAppInteractiveReply(BaseModel) :
    
    type         : Literal[ "button_reply", "list_reply"]
    button_reply : InteractiveOption | None = None
    list_reply   : InteractiveOption | None = None
    
    @model_validator( mode = "after")
    def check_content(self) -> Self :
        
        type_attribute = getattr( self, self.type, None)
        if not type_attribute :
            e_msg = f"Interactive reply of type '{self.type}' " \
                  + f"must have nontrivial attribute '{self.type}'"
            raise ValidationError(e_msg)
        
        return self
    
    @property
    def choice(self) -> InteractiveOption | None :
        
        if self.button_reply :
            return self.button_reply
        elif self.list_reply :
            return self.list_reply
        
        return

class WhatsAppMediaData(BaseModel) :
    
    id        : NE_str
    mime_type : NE_str
    sha256    : NE_str
    caption   : NE_str | None = None # image and video
    voice     : bool   | None = None # audio
    animated  : bool   | None = None # sticker
    
    @property
    def extension(self) -> str :
        return self.mime_type.split("/")[1]
    
    @property
    def type(self) -> str :
        return self.mime_type.split("/")[0]

class WhatsAppReaction(BaseModel) :
    
    message_id : NE_str
    emoji      : str | None = None

class WhatsAppMsg(BaseModel) :

    context   : WhatsAppContext | None = None
    
    user      : NE_str = Field( alias = "from")
    id        : NE_str
    timestamp : NE_str
    type      : Literal[ "text",
                         "interactive",
                         "image",
                         "video",
                         "audio",
                         "sticker",
                         "reaction",
                         "unsupported",
                         "contacts",
                         "location" ]
    
    text        : WhatsAppText             | None = None
    interactive : WhatsAppInteractiveReply | None = None
    image       : WhatsAppMediaData        | None = None
    video       : WhatsAppMediaData        | None = None
    audio       : WhatsAppMediaData        | None = None
    sticker     : WhatsAppMediaData        | None = None
    reaction    : WhatsAppReaction         | None = None
    
    @model_validator( mode = "after")
    def check_content(self) -> Self :
        
        if not self.type == "unsupported" :
            type_attribute = getattr( self, self.type, None)
            if not type_attribute :
                e_msg = f"Message of type '{self.type}' " \
                      + f"must have nontrivial attribute '{self.type}'"
                raise ValidationError(e_msg)
        
        return self
    
    @property
    def media_data(self) -> WhatsAppMediaData | None :
        
        if self.image :
            return self.image
        elif self.video :
            return self.video
        elif self.audio :
            return self.audio
        elif self.sticker :
            return self.sticker
        
        return None

# -----------------------------------------------------------------------------------------
# CONTACTS

class WhatsAppProfile(BaseModel) :
    
    name : NE_str

class WhatsAppContact(BaseModel) :
    
    wa_id   : NE_str
    profile : WhatsAppProfile | None = None

# =========================================================================================
# CASEFLOW BASEMODELS
# =========================================================================================

# USER DATA

class UserData(BaseModel) :
    """
    User Data
    {
    "user_id"  : "<user_id>"
    "code_reg" : "<region code>" | null
    "code_lan" : "<language code>" | null
    "country"  : "<country>" | null
    "language" : "<language>" | null
    "name"     : "<name>" | null
    }
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
    Message abstract base class. Must implement abstract property `role`.
    {
    
    "basemodel"       : "<class name (for deserialization)>"
    "repo_main_hash"  : "<hash of the repo's main branch (for deserialization)>"
    
    "origin"          : "CaseHandler/<method>"
    "case_id"         : <case ID>,
    
    "idempotency_key" : "<provider message ID>",
    "time_created"    : "<timestamp>"
    "time_received"   : "<timestamp>"
    "id"              : "<timestamp>_<random B62 8-char string>" | null
    
    }
    """
    basemodel       : NE_str | None = None
    repo_main_hash  : NE_str | None = None
    
    origin          : NE_str
    case_id         : NN_int
    
    idempotency_key : NE_str = Field( default_factory = generate_UUID)
    time_created    : NE_str = Field( default_factory = get_now_utc_iso)
    time_received   : NE_str = Field( default_factory = get_now_utc_iso)
    id              : NE_str | None = None
    
    def model_post_init( self, __context : Any) -> None :
        """
        * Populate basemodel and repo_main_hash
        * Ensure initialization of idempotency key
        """
        self.basemodel      = self.__class__.__name__
        self.repo_main_hash = get_repo_main_hash()
        
        if not self.id :
            
            time_str = str(self.time_received)
            time_str = time_str.replace( "T", "_").replace( ":", "-")
            time_str = time_str.replace( ".", "-").replace( "Z",  "")
            
            self.id  = f"{time_str}_{self.basemodel}"
        
        return
    
    def print(self) -> None :
        
        print_sep()
        print( "[INFO] Message:\n" + self.model_dump_json( indent= JSON_INDENT) )
        
        return
    
    @property
    @abstractmethod
    def role(self) -> str :
        return NotImplementedError

class BasicMsg( Message, ABC) :
    """
    Basic Message abstract base class. Has field `text`.
    """
    text : str | None = None

class StructuredDataMsg( Message, ABC) :
    """
    Structured Data Message abstract base class.
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
    {
    "mime" : "<MIME type>"
    }
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
    {
    "content" :  <bytes>
    }
    """
    content : bytes

class MediaData(MediaBase) :
    """
    Media Data
    {
    "name"   : "<filename>_<index>.<extension>",
    "sha256" : "<hash of attachment content>" | null,
    "size"   :  <attachment size> | null
    }
    """
    name   : NE_str
    sha256 : NE_str | None = None
    size   : NN_int | None = None
    
    @classmethod
    def from_content( cls, media_content : MediaContent) -> "MediaData" :
        
        return cls( name   = cls.__class__.__name__,
                    mime   = media_content.mime,
                    sha256 = get_sha256(media_content.content),
                    size   = len(media_content.content) )

class OutgoingMediaMsg(MediaBase) :
    """
    Outgoint Media Message
    """
    filepath  : NE_str
    content   : bytes
    caption   : NE_str | None = None
    upload_id : NE_str | None = None

class ToolCall(BaseModel) :
    """
    Tool Call
    {
    "id"    : "<tool call ID>",
    "name"  : "<tool name>" | null,
    "input" :  <tool input object as per schema> | null
    }
    """
    id    : NE_str = Field( default_factory = generate_UUID)
    name  : NE_str = Field( default = "tool_name")
    input : Annotated[ dict[ NE_str, Any] | None, Field( default_factory = dict)]

class ToolResult(BaseModel) :
    """
    Tool Result
    {
    "id"      : "<tool call ID>",
    "content" : "<tool call result>" | null,
    "error"   : false | true | null
    }
    """
    id      : NE_str
    content : Any  | None = None
    error   : bool | None = False

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
            raise ValidationError(f"In {self.basemodel}: No text or media")
        return self

class UserInteractiveReplyMsg( UserMsg, StructuredDataMsg) :
    """
    User Interactive Reply Message ( User -> Server )
    """
    choice : InteractiveOption
    
    def as_text(self) -> str :
        return self.choice.model_dump_json()

# -----------------------------------------------------------------------------------------
# SERVER MESSAGES

class ServerMsg( BasicMsg, ABC) :
    """
    Server Message
    """
    user_eyes : bool = False
    
    @property
    def role(self) -> str :
        return "user"

class ServerTextMsg(ServerMsg) :
    """
    Server Text Message
    """
    pass

class ServerInteractiveOptsMsg( ServerMsg, StructuredDataMsg) :
    """
    Server Interactive Options Message ( Server -> User )
    """
    type    : Literal[ "button", "list"]
    header  : NE_str | None = None
    body    : NE_str
    footer  : NE_str | None = None
    button  : NE_str | None = None
    options : Annotated [ list[InteractiveOption],
                          Field( min_length = 2, default_factory = list)]
    
    @model_validator( mode = "after")
    def validate_message(self) -> Self :
        
        e_msg = f"In {self.basemodel}: "
        
        if self.type == "button" and len(self.options) > 3 :
            e_msg += "Type 'button' only supports up to 3 options"
            raise ValidationError(e_msg)
        
        elif self.type == "list" and len(self.options) > 10 :
            e_msg += "Type 'list' only supports up to 10 options"
            raise ValidationError(e_msg)
        
        return self
    
    def as_text(self) -> str :
        return self.model_dump_json( include = { "header", "body", "options" })

# -----------------------------------------------------------------------------------------
# ASSISTANT CONTENT AND MESSAGES

class AssistantContent(BaseModel) :
    """
    Assistant (AI/LLM) Content
    """
    text_out   : str | None = None
    tool_calls : Annotated[ list[ToolCall], Field( default_factory = list)]
    st_output  : dict | None = None
    st_out_bm  : str  | None = None
    
    agent         : NE_str | None = None
    api           : NE_str | None = None
    model         : NE_str | None = None
    tokens_input  : NN_int | None = None
    tokens_output : NN_int | None = None
    tokens_total  : NN_int | None = None
    instructions  : NE_str | None = None
    tools         : list[Any]    | None = None
    context       : list[NE_str] | None = None
    
    def append_to_text( self, text_block : str | None) -> None :
        
        if text_block and isinstance( text_block, str): 
            if not self.text_out :
                self.text_out = text_block
            else :
                if self.text_out.endswith("\n") :
                    self.text_out += ( "\n" + text_block )
                else :
                    self.text_out += ( "\n\n" + text_block )
        
        return
    
    def is_empty(self) -> bool :
        return not bool( self.text_out or self.tool_calls or self.st_output )

class AssistantMsg( AssistantContent, BasicMsg) :
    """
    Assistant (AI/LLM) Message
    NOTE:
    * Instantiate only via class method `from_content`
    * Do not instantiate directly using class constructor
    """
    
    @model_validator( mode = "after")
    def ensure_nonempty(self) -> Self :
        
        if self.is_empty() :
            e_msg = f"In {self.basemodel}: No text, tool calls or structured output."
            raise ValidationError(e_msg)
        
        return self
    
    @property
    def role(self) -> str :
        return "assistant"
    
    @classmethod
    def from_content( cls,
                      origin  : str,
                      case_id : int,
                      content : AssistantContent ) -> "AssistantMsg" :
        
        arguments = { "origin"  : origin,
                      "case_id" : case_id,
                      "text"    : content.text_out }
        
        arguments.update( content.model_dump( exclude = {"text_out"}) )
        
        return cls(**arguments)
    
    def is_empty(self) -> bool :
        return not bool( self.text or self.tool_calls or self.st_output )

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
    {
    "open_case_id" : <case ID>
    }
    """
    open_case_id : NN_int | None = None

class CaseManifest(BaseModel) :
    """
    Manifest
    {
    "case_id"           : <case ID>,
    "model"             : "T40" | "T50" | null
    "status"            : "open" | "resolved" | "timeout",
    "time_opened"       : "<timestamp>",
    "time_last_message" : "<timestamp>" | null,
    "time_closed"       : "<timestamp>" | null,
    "message_ids"       : [ "<message ID>", ... ],
    }
    """
    case_id           : NN_int
    model             : NE_str | None = None
    status            : NE_str        = "open"
    time_opened       : NE_str = Field( default_factory = get_now_utc_iso)
    time_last_message : NE_str | None = None
    time_closed       : NE_str | None = None
    message_ids       : Annotated[ list[NE_str], Field( default_factory = list)]
