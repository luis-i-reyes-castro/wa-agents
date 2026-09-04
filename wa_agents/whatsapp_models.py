"""
WhatsApp BaseModels \\
References:
* https://developers.facebook.com/docs/whatsapp/cloud-api/webhooks/reference/messages
& https://developers.facebook.com/documentation/business-messaging/whatsapp/business-scoped-user-ids/
"""

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)
from typing import (
    Annotated,
    Literal,
    Self,
)

from sofia_utils.pydantic import (
    HexHash,
    MIME_Type,
    NE_str,
    NumericID,
    UnixTS,
)


# -----------------------------------------------------------------------------------------
# BASE TYPES

type WhatsAppPayloadType = Literal[ "message", "status"]
""" WhatsApp Payload Type """

type WhatsAppUsername = Annotated[
                            str,
                            Field( pattern = r"^[A-Za-z0-9\.\_]{3,35}$"),
                        ]
""" WhatsApp Username """

type WhatsAppBSUID    = Annotated[
                            str, 
                            Field( pattern = r"^[A-Z]{2}\.[A-Za-z0-9]{1,128}$"),
                        ]
""" WhatsApp Business-scoped User ID (BSUID) """

type WhatsAppMessageID = Annotated[
                            str,
                            Field( pattern = r"^wamid\.[A-Za-z0-9\+\/\=]+$"),
                         ]
""" WhatsApp Message ID """

type WhatsAppTemplateLanguageCode = Annotated[
                                        str,
                                        Field( pattern = r"^[a-z]{2}\_[A-Z]{2}$"),
                                    ]

type WhatsAppTextBody                = Annotated[ str, Field( min_length = 1)]
""" WhatsApp inbound text body """

type WhatsAppInteractiveId           = Annotated[ str, Field( min_length = 1,
                                                              max_length = 200)]
""" WhatsApp interactive option id """

type WhatsAppInteractiveTitle        = Annotated[ str, Field( min_length = 1,
                                                              max_length = 24)]
""" WhatsApp interactive option title """

type WhatsAppInteractiveDescription  = Annotated[ str, Field( min_length = 1,
                                                              max_length = 72)]
""" WhatsApp interactive option description """

type WhatsAppInteractiveHeaderFooter = Annotated[ str, Field( min_length = 1,
                                                              max_length = 60)]
""" WhatsApp interactive header/footer text """

type WhatsAppInteractiveBody         = Annotated[ str, Field( min_length = 1,
                                                              max_length = 1024)]
""" WhatsApp interactive body text """

type WhatsAppInteractiveButtonLabel  = Annotated[ str, Field( min_length = 1,
                                                              max_length = 20), ]
""" WhatsApp interactive button label """


# -----------------------------------------------------------------------------------------
# MESSAGES

class WhatsAppMetaData(BaseModel) :
    """
    WhatsApp message or status recipient metadata.
        `display_phone_number` : "<receiver phone number>"
        `phone_number_id`      : "<receiver WhatsApp number ID>"
    """
    model_config = ConfigDict( frozen = True)
    
    display_phone_number : NumericID # Receiver phone number
    phone_number_id      : NumericID # Receiver phone number ID

class WhatsAppProfile(BaseModel) :
    """
    WhatsApp contact profile corresponding to message sender (NOT CONTACT CARD)
        `name`     : "<display name>"
        `username` : "<username>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    name     : NE_str
    username : WhatsAppUsername | None = None

class WhatsAppContact(BaseModel) :
    """
    WhatsApp contact corresponding to message sender (NOT CONTACT CARD)
        
        `profile` : WhatsAppProfile | null
        `wa_id`   : "<sender phone number>"
        `user_id` : "<BSUID>"       | null
    NOTE:
        * This class models contact data ASSOCIATED WITH an incoming WhatsAppMessage
        * Different from `WhatsAppContactPayload`
    """
    model_config = ConfigDict( frozen = True)
    
    profile : WhatsAppProfile | None = None
    wa_id   : NumericID       | None = None # Sender phone number
    user_id : WhatsAppBSUID   | None = None # Sender BSUID
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        if not ( self.wa_id or self.user_id ) :
            raise ValueError(
                "WhatsAppContact is missing both fields 'wa_id' and 'user_id'"
            )
        return self

class WhatsAppContext(BaseModel) :
    """
    WhatsApp message context
        `user`                 : "<sender phone number>" | null
        `user_id`              : "<sender BSUID>"        | null
        `id`                   : "<replied-to message ID>" | null
        `forwarded`            : true | false | null
        `frequently_forwarded` : true | false | null
        `referred_product`     : { "<key>": "<value>", ... } | null
    NOTE:
        * Since `from` is a reserved keyword in Python here we declare it as the dummy field `user` and then assign it the alias `from`.
        * Similarly `from_user_id` is declared as `user_id` and then aliased.
    """
    model_config = ConfigDict( frozen           = True,
                               populate_by_name = True)
    
    # Fields below present only if message is a reply
    user    : NumericID         | None = Field( alias = "from",         default = None)
    user_id : WhatsAppBSUID     | None = Field( alias = "from_user_id", default = None)
    id      : WhatsAppMessageID | None = None # ID of message being replied to
    
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
    
    body : WhatsAppTextBody

class WhatsAppInteractiveOption(BaseModel) :
    """
    Interactive Message Option
        `id`          : "<option ID>"
        `title`       : "<option title>"
        `description` : "<option detail line>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    id          : WhatsAppInteractiveId
    title       : WhatsAppInteractiveTitle
    description : WhatsAppInteractiveDescription | None = None

class WhatsAppInteractiveReply(BaseModel) :
    """
    WhatsApp interactive reply
        `type`         : "button_reply" | "list_reply"
        `button_reply` : InteractiveOption | null
        `list_reply`   : InteractiveOption | null
    """
    model_config = ConfigDict( frozen = True)
    
    type         : Literal[ "button_reply", "list_reply"]
    button_reply : WhatsAppInteractiveOption | None = None
    list_reply   : WhatsAppInteractiveOption | None = None
    
    @model_validator( mode = "after")
    def check_content(self) -> Self :
        
        type_attribute = getattr( self, self.type, None)
        if not type_attribute :
            e_msg = f"Interactive reply of type '{self.type}' " \
                  + f"must have nontrivial attribute '{self.type}'"
            raise ValueError(e_msg)
        
        return self
    
    @property
    def choice(self) -> WhatsAppInteractiveOption | None :
        
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
    
    id        : NumericID
    mime_type : MIME_Type
    sha256    : HexHash
    caption   : WhatsAppTextBody | None = None # image and video
    voice     : bool             | None = None # audio
    animated  : bool             | None = None # sticker
    
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
    
    message_id : WhatsAppMessageID
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
    WhatsApp incoming contact payload (a.k.a. CONTACT CARD)
        `name`   : `WhatsAppContactPayload_Name`
        `phones` : `tuple[ WhatsAppContactPayload_Phone, ...]`
        `org`    : `WhatsAppContactPayload_Org`                | null
        `emails` : `tuple[ WhatsAppContactPayload_Email, ...]` | null
    NOTE:
        * This class models contact data payload ATTACHED to a WhatsAppMessage
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

class WhatsAppMessage(BaseModel) :
    """
    WhatsApp message payload
        `from`         : "<sender phone number>"
        `from_user_id` : "<sender BSUID>"
        `id`           : "<message ID>"
        `timestamp`    : "<unix timestamp>"
        `type`         : "<message type>"
        `text`         : `WhatsAppText`             | null
        `interactive`  : `WhatsAppInteractiveReply` | null
        `image`        : `WhatsAppMediaData`        | null
        `video`        : `WhatsAppMediaData`        | null
        `audio`        : `WhatsAppMediaData`        | null
        `sticker`      : `WhatsAppMediaData`        | null
        `reaction`     : `WhatsAppReaction`         | null
        `contacts`     : `tuple[ WhatsAppContactPayload, ...]` | null
        `location`     : `WhatsAppLocation`                    | null
    NOTE:
        * Since `from` is a reserved keyword in Python here we declare it as the dummy field `user` and then assign it the alias `from`.
        * Similarly `from_user_id` is declared as `user_id` and then aliased.
    """
    model_config = ConfigDict( frozen           = True,
                               populate_by_name = True)
    
    context   : WhatsAppContext | None = None
    
    user      : NE_str | None = Field( alias   = "from",         # Sender phone number
                                       default = None)
    user_id   : NE_str | None = Field( alias   = "from_user_id", # Sender BSUID
                                       default = None)
    
    id        : WhatsAppMessageID
    timestamp : UnixTS
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
    def validate(self) -> Self :
        
        if not ( self.user or self.user_id ) :
            raise ValueError(
                "WhatsAppMessage is missing both fields 'from' and 'from_user_id'"
            )
        
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
# STATUSES OF SENT MESSAGES

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
        `id`                : "<WhatsApp message ID>"
        `recipient_id`      : "<user phone number>"
        `recipient_user_id` : "<user BSUID>"
        `status`            : "delivered" | "failed" | "played" | "read" | "sent" | null
        `timestamp`         : "<unix timestamp>"
        `conversation`      : WhatsAppConversation | null
        `pricing`           : WhatsAppPricing | null
        `errors`            : tuple[ WhatsAppStatusError, ...] | null
    """
    model_config = ConfigDict( frozen = True)
    
    id                : WhatsAppMessageID
    recipient_id      : NumericID     | None = None # Receiver phone number
    recipient_user_id : WhatsAppBSUID | None = None # Receiver BSUID
    status       : Literal[
        "delivered",
        "failed",
        "played",
        "read",
        "sent",
    ]
    timestamp    : UnixTS
    conversation : WhatsAppConversation             | None = None
    pricing      : WhatsAppPricing                  | None = None
    errors       : tuple[ WhatsAppStatusError, ...] | None = None
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if not ( self.recipient_id or self.recipient_user_id ) :
            raise ValueError(
                "WhatsAppStatus is missing both fields "
                "'recipient_id' and 'recipient_user_id'"
            )
        
        return self


# -----------------------------------------------------------------------------------------
# PAYLOADS

class WhatsAppValue(BaseModel) :
    """
    WhatsApp change value payload
        `messaging_product` : "whatsapp"
        `metadata`          : WhatsAppMetaData
        `contacts`          : tuple[ WhatsAppContact, ...]
        `messages`          : tuple[ WhatsAppMessage, ...]
        `statuses`          : tuple[ WhatsAppStatus, ...]
    """
    
    model_config = ConfigDict( frozen = True)
    
    messaging_product : NE_str = "whatsapp"
    
    metadata : WhatsAppMetaData
    contacts : tuple[ WhatsAppContact, ...] = ()
    messages : tuple[ WhatsAppMessage, ...] = ()
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
        `object` : "whatsapp_business_account"
        `entry`  : tuple[ WhatsAppChanges, ...]
    NOTE:
        Since `object` is a reserved keyword in Python here we declare it
        as the dummy field `object_field` and then assign it the alias `object`.
    """
    
    model_config = ConfigDict( frozen = True)
    
    object_field : NE_str = Field( alias   = "object",
                                   default = "whatsapp_business_account")
    entry        : tuple[ WhatsAppChanges, ...]
    
    def has_messages(self) -> bool :
        return any(
            change.value.messages
            for entry in self.entry
            for change in entry.changes
        )
