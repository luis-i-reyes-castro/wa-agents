"""
BaseModel Classes
"""

from abc import ( ABC,
                  abstractmethod )
from decimal import Decimal
from mimetypes import guess_type
from pathlib import Path
from pydantic import ( BaseModel,
                       ConfigDict,
                       Field,
                       model_validator,
                       ValidationError )
from typing import ( Annotated,
                     Any,
                     Literal,
                     Self )

from sofia_utils.io import JSON_INDENT
from sofia_utils.printing import ( print_ind,
                                   print_sep )
from sofia_utils.stamps import ( generate_UUID,
                                 get_now_utc_iso,
                                 get_sha256 )

from .phone_numbers import get_country_and_language


# =========================================================================================
# COMMON TYPES AND BASEMODELS
# =========================================================================================

type NN_Decimal = Annotated[ Decimal, Field( ge = 0)]
""" Non-negative Decimal """

type NN_int = Annotated[ int, Field( ge = 0)]
""" Non-negative integer """

type NE_str = Annotated[ str, Field( min_length = 2)]
""" Non-empty string (at least 2 chars) """

type NE_list_str = Annotated[ list[ NE_str ], Field( min_length = 1)]
""" Non-empty list of strings (at least 1 string) """

type NE_dict_str = Annotated[ dict[ NE_str, Any], Field( min_length = 1)]
""" Non-empty dict of strings (at least 1 key-value pair) """

class InteractiveOption(BaseModel) :
    """
    Interactive Message Option
        `id`    : "<option ID>"
        `title` : "<option title>"
    """
    id    : NE_str
    title : NE_str

# =========================================================================================
# WHATSAPP BASEMODELS
# Reference: https://developers.facebook.com/docs/whatsapp/cloud-api/webhooks/reference/messages
# =========================================================================================

# MESSAGES

class WhatsAppContext(BaseModel) :
    """
    WhatsApp message context
        `user`                 : "<sender phone number>" | null
        `id`                   : "<replied-to message ID>" | null
        `forwarded`            : true | false | null
        `frequently_forwarded` : true | false | null
        `referred_product`     : { "<key>": "<value>", ... } | null
    """
    
    model_config = ConfigDict( frozen           = True,
                               populate_by_name = True)
    
    # Fields below present only if message is a reply
    user : NE_str | None = Field( alias = "from", default = None)
    id   : NE_str | None = None # ID of message being replied to
    
    # Fields below present only if message was forwarded
    forwarded            : bool | None = None
    frequently_forwarded : bool | None = None
    
    # Field below present only if message refers to a catalog product
    referred_product : dict[ str, str] | None = None

class WhatsAppText(BaseModel) :
    """
    WhatsApp text payload
        `body` : "<message text>"
    """
    
    model_config = ConfigDict( frozen = True)
    
    body : NE_str

class WhatsAppInteractiveReply(BaseModel) :
    """
    WhatsApp interactive reply
        `type`         : "button_reply" | "list_reply"
        `button_reply` : InteractiveOption | null
        `list_reply`   : InteractiveOption | null
    """
    
    model_config = ConfigDict( frozen = True)
    
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
    """
    WhatsApp media descriptor
        `id`        : "<media ID>"
        `mime_type` : "<MIME type>"
        `sha256`    : "<sha256 checksum>"
        `caption`   : "<caption>" | null
        `voice`     : true | false | null
        `animated`  : true | false | null
    """
    
    model_config = ConfigDict( frozen = True)
    
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
    """
    WhatsApp reaction
        `message_id` : "<message ID>"
        `emoji`      : "<emoji>" | null
    """
    
    model_config = ConfigDict( frozen = True)
    
    message_id : NE_str
    emoji      : str | None = None

class WhatsAppMsg(BaseModel) :
    """
    WhatsApp message payload
        `user`        : "<sender phone number>"
        `id`          : "<message ID>"
        `timestamp`   : "<unix timestamp>"
        `type`        : "<message type>"
        `text`        : WhatsAppText | null
        `interactive` : WhatsAppInteractiveReply | null
        `image`       : WhatsAppMediaData | null
        `video`       : WhatsAppMediaData | null
        `audio`       : WhatsAppMediaData | null
        `sticker`     : WhatsAppMediaData | null
        `reaction`    : WhatsAppReaction | null
    """
    
    model_config = ConfigDict( frozen           = True,
                               populate_by_name = True)
    
    context   : WhatsAppContext | None = None
    
    user      : NE_str = Field( alias = "from") # Sender Phone Number
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
    
    # In a WhatsApp message only one of the fields below will be present
    # (more precisely, the field that matches the message `type`).
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

# CONTACTS

class WhatsAppProfile(BaseModel) :
    """
    WhatsApp contact profile
        `name` : "<display name>"
    """
    
    model_config = ConfigDict( frozen = True)
    
    name : NE_str

class WhatsAppContact(BaseModel) :
    """
    WhatsApp contact record
        `wa_id`   : "<sender phone number>"
        `profile` : WhatsAppProfile | null
    """
    
    model_config = ConfigDict( frozen = True)
    
    wa_id   : NE_str # Sender Phone Number
    profile : WhatsAppProfile | None = None

# PAYLOADS

class WhatsAppMetaData(BaseModel) :
    """
    WhatsApp webhook metadata
        `display_phone_number` : "<receiver phone number>"
        `phone_number_id`      : "<receiver WhatsApp number ID>"
    """
    
    model_config = ConfigDict( frozen = True)
    
    display_phone_number : NE_str # Receiver Phone Number
    phone_number_id      : NE_str # Receiver WhatsApp Number ID

class WhatsAppValue(BaseModel) :
    """
    WhatsApp change value payload
        `messaging_product` : "whatsapp"
        `metadata`          : WhatsAppMetaData
        `contacts`          : tuple[ WhatsAppContact, ...]
        `messages`          : tuple[ WhatsAppMsg, ...]
    """
    
    model_config = ConfigDict( frozen = True)
    
    messaging_product : NE_str = "whatsapp"
    
    metadata : WhatsAppMetaData
    contacts : tuple[ WhatsAppContact, ...]
    messages : tuple[ WhatsAppMsg, ...]

class WhatsAppChange_(BaseModel) :
    """
    WhatsApp change item
        `value` : WhatsAppValue
        `field` : "messages"
    """
    
    model_config = ConfigDict( frozen = True)
    
    value : WhatsAppValue
    field : NE_str = "messages"

class WhatsAppChanges(BaseModel) :
    """
    WhatsApp change wrapper
        `id`      : "<receiver WABA number>"
        `changes` : tuple[ WhatsAppChange_, ...]
    """
    
    model_config = ConfigDict( frozen = True)
    
    id      : NE_str # Receiver WABA Number
    changes : tuple[ WhatsAppChange_, ...]

class WhatsAppPayload(BaseModel) :
    """
    Top-level WhatsApp webhook payload
        `title` : "whatsapp_business_account"
        `entry` : tuple[ WhatsAppChanges, ...]
    """
    
    model_config = ConfigDict( frozen = True)
    
    title : NE_str = Field( alias   = "object",
                            default = "whatsapp_business_account")
    entry : tuple[ WhatsAppChanges, ...]

# =========================================================================================
# CASEFLOW BASEMODELS
# =========================================================================================

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
        print( "[INFO] Message:\n" + self.model_dump_json( indent= JSON_INDENT) )
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
    size   : NN_int | None = None
    
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
    Outgoint Media Message
    """
    filepath  : NE_str
    content   : bytes
    caption   : NE_str | None = None
    upload_id : NE_str | None = None

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
        `error`   : false | true | null
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

class ServerMsg( Message, ABC) :
    """
    Server Message
    """
    user_eyes : bool = False
    
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
    tokens_input  : NN_int | None = None
    tokens_output : NN_int | None = None
    tokens_total  : NN_int | None = None
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
    open_case_id : NN_int | None = None

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
    case_id           : NN_int
    model             : NE_str | None = None
    status            : NE_str        = "open"
    time_opened       : NE_str = Field( default_factory = get_now_utc_iso)
    time_last_message : NE_str | None = None
    time_closed       : NE_str | None = None
    message_ids       : Annotated[ list[NE_str], Field( default_factory = list)]

# =========================================================================================
# VALIDATION ERROR PRINTING
# =========================================================================================

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
