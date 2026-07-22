"""
BaseModel Classes
"""

from abc import (
    ABC,
    abstractmethod,
)
from decimal import Decimal
from mimetypes import guess_type
from pathlib import Path
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
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
from sofia_utils.stamps import (
    generate_UUID,
    get_now_utc_iso,
    get_sha256,
)

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

type WA_TextBody = Annotated[ str, Field( min_length = 1)]
""" WhatsApp inbound text body """

type NE_var_name = Annotated[ str, Field( pattern = r"^[A-Za-z\_]\w+$")]
""" Non-empty variable name (at least 2 chars) """

type WA_InteractiveId = Annotated[ str, Field( min_length = 1, max_length = 200)]
""" WhatsApp interactive option id """

type WA_InteractiveTitle = Annotated[ str, Field( min_length = 1, max_length = 24)]
""" WhatsApp interactive option title """

type WA_InteractiveDescription = Annotated[
    str,
    Field( min_length = 1, max_length = 72),
]
""" WhatsApp interactive option description """

type WA_InteractiveHeaderFooter = Annotated[
    str,
    Field( min_length = 1, max_length = 60),
]
""" WhatsApp interactive header/footer text """

type WA_InteractiveBody = Annotated[ str, Field( min_length = 1, max_length = 1024)]
""" WhatsApp interactive body text """

type WA_InteractiveButtonLabel = Annotated[
    str,
    Field( min_length = 1, max_length = 20),
]
""" WhatsApp interactive button label """

type NE_list_str = Annotated[ list[ NE_str ], Field( min_length = 1)]
""" Non-empty list of strings (at least 1 string) """

type NE_dict_str = Annotated[ dict[ NE_str, Any], Field( min_length = 1)]
""" Non-empty dict of strings (at least 1 key-value pair) """

class InteractiveOption(BaseModel) :
    """
    Interactive Message Option
        `id`          : "<option ID>"
        `title`       : "<option title>"
        `description` : "<option detail line>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    id          : WA_InteractiveId
    title       : WA_InteractiveTitle
    description : WA_InteractiveDescription | None = None

# =========================================================================================
# WHATSAPP BASEMODELS
# Reference: https://developers.facebook.com/docs/whatsapp/cloud-api/webhooks/reference/messages
# =========================================================================================

# METADATA

class WhatsAppMetaData(BaseModel) :
    """
    WhatsApp message or status recipient metadata.
        `display_phone_number` : "<receiver phone number>"
        `phone_number_id`      : "<receiver WhatsApp number ID>"
    """
    model_config = ConfigDict( frozen = True)
    
    display_phone_number : NE_str # Receiver Phone Number
    phone_number_id      : NE_str # Receiver WhatsApp Number ID

# CONTACTS ASSOCIATED WITH INCOMING MESSAGES (NOT CONTACT CARDS)

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
    NOTE:
        * This class models contact data ASSOCIATED WITH an incoming WhatsAppMsg
        * Different from `WhatsAppContactPayload`
    """
    model_config = ConfigDict( frozen = True)
    
    wa_id   : NE_str # Sender Phone Number
    profile : WhatsAppProfile | None = None

# -----------------------------------------------------------------------------------------
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
    
    body : WA_TextBody

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
            raise ValueError(e_msg)
        
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

class WhatsAppContactPayload_Name(BaseModel) :
    """
    WhatsApp incoming contact name
        `formatted_name` : "<name>"
        `first_name`     : str | null
        `middle_name`    : str | null
        `last_name`      : str | null
        `prefix`         : str | null
        `suffix`         : str | null
    """
    model_config = ConfigDict( frozen = True)
    
    formatted_name : str
    first_name     : str | None = None
    middle_name    : str | None = None
    last_name      : str | None = None
    prefix         : str | None = None
    suffix         : str | None = None
    
    @model_validator( mode = "after")
    def ensure_at_least_one_name(self) -> Self :
        """
        Satisfy META's requirement that the payload have at least:
        * Formatted name
        * At least one of: first name, middle name, last name.
        """
        
        if not ( self.first_name or self.middle_name or self.last_name ) :
            raise ValueError("Must have at least one name")
        
        return self

class WhatsAppContactPayload_Phone(BaseModel) :
    """
    WhatsApp incoming contact phone
        `phone` : "<phone number starting with plus sign>"
        `type`  : "CELL" | "Mobile" | "Landline" | str
        `wa_id` : "<WhatsApp phone number ID>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    phone : str
    type  : str
    wa_id : str | None = None

class WhatsAppContactPayload_Email(BaseModel) :
    """
    WhatsApp incoming contact email
        `email` : "<email>"
        `type`  : "Work" | "Personal" | str
    """
    model_config = ConfigDict( frozen = True)
    
    email : str
    type  : str

class WhatsAppContactPayload_Org(BaseModel) :
    """
    WhatsApp incoming contact organization
        `company` : "<company name>"
    """
    model_config = ConfigDict( frozen = True)
    
    company    : str
    department : str | None = None
    title      : str | None = None

class WhatsAppContactPayload_Address(BaseModel) :
    """
    WhatsApp incoming contact address
        `type`         : "HOME" | "WORK" | str | null
        `city`         : "<city>" | null
        `country`      : "<country>" | null
        `country_code` : "<2-letter ISO country code>" | null
        `state`        : "<state>" | null
        `street`       : "<street>" | null
        `zip`          : "<zip code>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    type         : str | None = None
    city         : str | None = None
    country      : str | None = None
    country_code : str | None = None
    state        : str | None = None
    street       : str | None = None
    zip          : str | None = None
    
    @model_validator( mode = "after")
    def ensure_not_empty(self) -> Self :
        
        if not (
            self.city         or
            self.country      or
            self.country_code or
            self.state        or
            self.street       or
            self.zip
            ) :
            raise ValueError("No data")
        
        return self

class WhatsAppContactPayload_Url(BaseModel) :
    """
    WhatsApp incoming contact URL
        `type` : "HOME" | "WORK" | str | null
        `url`  : "<URL>"
    """
    model_config = ConfigDict( frozen = True)
    
    type : str | None = None
    url  : str

class WhatsAppContactPayload(BaseModel) :
    """
    WhatsApp incoming contact payload (a.k.a. contact card)
        `name`   : `WhatsAppContactPayload_Name`
        `phones` : `tuple[ WhatsAppContactPayload_Phone, ...]`
        `org`    : `WhatsAppContactPayload_Org`                | null
        `emails` : `tuple[ WhatsAppContactPayload_Email, ...]` | null
    NOTE:
        * This class models contact data payload ATTACHED to a WhatsAppMsg
        * Different from `WhatsAppContact`
    """
    model_config = ConfigDict( frozen = True)
    
    name   : WhatsAppContactPayload_Name
    phones : tuple[ WhatsAppContactPayload_Phone, ...]
    org    : WhatsAppContactPayload_Org                | None = None
    emails : tuple[ WhatsAppContactPayload_Email, ...] | None = None
    birthday  : str                                         | None = None
    addresses : tuple[ WhatsAppContactPayload_Address, ...] | None = None
    urls      : tuple[ WhatsAppContactPayload_Url, ...]     | None = None

class WhatsAppLocation(BaseModel) :
    """
    WhatsApp location
        `latitude`  : <degrees>
        `longitude` : <degrees>
    """
    model_config = ConfigDict( frozen = True)
    
    latitude  : float
    longitude : float
    name      : str | None = None
    address   : str | None = None

class WhatsAppMsg(BaseModel) :
    """
    WhatsApp message payload
        `user`        : "<sender phone number>"
        `id`          : "<message ID>"
        `timestamp`   : "<unix timestamp>"
        `type`        : "<message type>"
        `text`        : `WhatsAppText`             | null
        `interactive` : `WhatsAppInteractiveReply` | null
        `image`       : `WhatsAppMediaData`        | null
        `video`       : `WhatsAppMediaData`        | null
        `audio`       : `WhatsAppMediaData`        | null
        `sticker`     : `WhatsAppMediaData`        | null
        `reaction`    : `WhatsAppReaction`         | null
        `contacts`    : `tuple[ WhatsAppContactPayload, ...]` | null
        `location`    : `WhatsAppLocation`                    | null
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
                         "contacts",
                         "location",
                         "unsupported" ]
    
    # In a WhatsApp message only one of the fields below will be present
    # (more precisely, the field that matches the message `type`).
    text        : WhatsAppText             | None = None
    interactive : WhatsAppInteractiveReply | None = None
    image       : WhatsAppMediaData        | None = None
    video       : WhatsAppMediaData        | None = None
    audio       : WhatsAppMediaData        | None = None
    sticker     : WhatsAppMediaData        | None = None
    reaction    : WhatsAppReaction         | None = None
    contacts    : tuple[ WhatsAppContactPayload, ...] | None = None
    location    : WhatsAppLocation                    | None = None
    
    @model_validator( mode = "after")
    def check_content(self) -> Self :
        
        if not self.type == "unsupported" :
            type_attribute = getattr( self, self.type, None)
            if not type_attribute :
                e_msg = f"Message of type '{self.type}' " \
                      + f"must have nontrivial attribute '{self.type}'"
                raise ValueError(e_msg)
        
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
# STATUS

class WhatsAppConversationOrigin (BaseModel) :
    """
    WhatsApp conversation origin
        `type` : "authentication" | "authentication_international" | "marketing" | "marketing_lite" | "referral_conversion" | "service" | "utility"
    """
    model_config = ConfigDict( frozen = True)
    
    type : Literal[
        "authentication",
        "authentication_international",
        "marketing",
        "marketing_lite",
        "referral_conversion",
        "service",
        "utility",
    ]

class WhatsAppConversation (BaseModel) :
    """
    WhatsApp status conversation data
        `id`                   : "<conversation ID>"
        `origin`               : WhatsAppConversationOrigin | null
        `expiration_timestamp` : "<unix timestamp>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    id                   : NE_str
    origin               : WhatsAppConversationOrigin | None = None
    expiration_timestamp : NE_str                     | None = None

class WhatsAppPricing (BaseModel) :
    """
    WhatsApp status pricing data
        `billable`      : true | false | null
        `category`      : "authentication" | "authentication-international" | "marketing" | "marketing_lite" | "referral_conversion" | "service" | "utility" | null
        `pricing_model` : "CBP" | "PMP" | null
        `type`          : "free_customer_service" | "free_entry_point" | "regular" | null
    """
    model_config = ConfigDict( frozen = True)
    
    billable : bool | None = None
    category : Literal[
        "authentication",
        "authentication-international",
        "marketing",
        "marketing_lite",
        "referral_conversion",
        "service",
        "utility",
    ] | None = None
    pricing_model : Literal[
        "CBP",
        "PMP",
    ] | None = None
    type          : Literal[
        "free_customer_service",
        "free_entry_point",
        "regular",
    ] | None = None

class WhatsAppStatusErrorData (BaseModel) :
    """
    WhatsApp status error details
        `details` : "<error details>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    details : str | None = None

class WhatsAppStatusError (BaseModel) :
    """
    WhatsApp status error
        `code`       : <error code>
        `title`      : "<error title>"
        `message`    : "<error message>" | null
        `error_data` : WhatsAppStatusErrorData | null
        `href`       : "<error code URL>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    code       : int
    title      : NE_str
    message    : NE_str                  | None = None
    error_data : WhatsAppStatusErrorData | None = None
    href       : NE_str                  | None = None

class WhatsAppStatus (BaseModel) :
    """
    WhatsApp outbound message status update
        `id`            : "<WhatsApp message ID>"
        `recipient_id`  : "<user phone number or group ID>"
        `status`        : "delivered" | "failed" | "played" | "read" | "sent" | null
        `timestamp`     : "<unix timestamp>"
        `conversation`  : WhatsAppConversation | null
        `pricing`       : WhatsAppPricing | null
        `errors`        : tuple[ WhatsAppStatusError, ...] | null
    """
    model_config = ConfigDict( frozen = True)
    
    id           : NE_str
    recipient_id : NE_str
    status       : Literal[
        "delivered",
        "failed",
        "played",
        "read",
        "sent",
    ]
    timestamp    : NE_str
    conversation : WhatsAppConversation             | None = None
    pricing      : WhatsAppPricing                  | None = None
    errors       : tuple[ WhatsAppStatusError, ...] | None = None

class WhatsAppValue(BaseModel) :
    """
    WhatsApp change value payload
        `messaging_product` : "whatsapp"
        `metadata`          : WhatsAppMetaData
        `contacts`          : tuple[ WhatsAppContact, ...]
        `messages`          : tuple[ WhatsAppMsg, ...]
        `statuses`          : tuple[ WhatsAppStatus, ...]
    """
    
    model_config = ConfigDict( frozen = True)
    
    messaging_product : NE_str = "whatsapp"
    
    metadata : WhatsAppMetaData
    contacts : tuple[ WhatsAppContact, ...] = ()
    messages : tuple[ WhatsAppMsg,     ...] = ()
    statuses : tuple[ WhatsAppStatus,  ...] = ()
    
    @model_validator( mode = "after")
    def check_content(self) -> Self :
        
        if not ( self.messages or self.statuses ) :
            raise ValueError("WhatsApp value must include messages or statuses")
        
        return self

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
    
    def has_messages(self) -> bool :
        return any(
            change.value.messages
            for entry in self.entry
            for change in entry.changes
        )

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
        `error`   : false | true | null
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
    header  : WA_InteractiveHeaderFooter | None = None
    body    : WA_InteractiveBody
    footer  : WA_InteractiveHeaderFooter | None = None
    button  : WA_InteractiveButtonLabel  | None = None
    options : Annotated[ list[InteractiveOption],
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
# UTILITY FUNCTIONS
# =========================================================================================

def is_llm_readable( message : Message) -> bool :
    
    return not ( isinstance( message, ServerTextMsg) and message.user_eyes )

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
