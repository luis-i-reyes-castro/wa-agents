"""
WhatsApp BaseModels \\
References:
* https://developers.facebook.com/docs/whatsapp/cloud-api/webhooks/reference/messages
* https://developers.facebook.com/documentation/business-messaging/whatsapp/business-scoped-user-ids/
"""

from abc import ABC
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_serializer,
    model_validator,
)
from typing import (
    Annotated,
    Any,
    Callable,
    Literal,
    Self,
)

from sofia_utils.pydantic import (
    HexHash,
    MIME_Type,
    NE_str,
    NE_var_name,
    NumericID,
    UnixTS,
    serialize_without_nones,
)


# =========================================================================================
# BASE TYPES

type WhatsAppBSUID                = Annotated[
    str, Field( pattern = r"^[A-Z]{2}\.[A-Za-z0-9]{1,128}$"),
]
""" WhatsApp Business-scoped User ID (BSUID) """

type WhatsAppMessageID            = Annotated[
    str, Field( pattern = r"^wamid\.[A-Za-z0-9\+\/\=]+$"),
]
""" WhatsApp Message ID """

type WhatsAppTemplateLanguageCode = Annotated[
    str, Field( pattern = r"^[a-z]{2}\_[A-Z]{2}$"),
]

type WhatsAppUsername             = Annotated[
    str, Field( pattern = r"^[A-Za-z0-9\.\_]{3,35}$"),
]
""" WhatsApp Username """

type WhatsAppTextBody                = Annotated[ str, Field( min_length = 1)]
""" WhatsApp text body """

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

type WhatsAppMessageType = Literal[
    "text",
    "interactive",
    "image",
    "video",
    "audio",
    "document",
    "sticker",
    "reaction",
    "contacts",
    "location",
    "unsupported",
]
""" WhatsApp Message Type """

type WhatsAppPayloadType = Literal[
    "message",
    "status",
]
""" WhatsApp Payload Type """

type WhatsApp_OB_MediaType = Literal[
    "image",
    "video",
    "audio",
    "document",
]
""" WhatsApp Outbound Media Type """


# =========================================================================================
# INBOUND & OUTBOUND: SHARED MODELS

class WhatsAppText (BaseModel) :
    """
    WhatsApp text payload
        `body` : "<message text>"
    """
    model_config = ConfigDict( frozen = True)
    
    body : WhatsAppTextBody

class WhatsAppInteractiveOption (BaseModel) :
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

class WhatsAppContactPayload_Name (BaseModel) :
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

class WhatsAppContactPayload_Phone (BaseModel) :
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

class WhatsAppContactPayload_Email (BaseModel) :
    """
    WhatsApp incoming contact email
        `email` : "<email>"
        `type`  : "Work" | "Personal" | str
    """
    model_config = ConfigDict( frozen = True)
    
    email : str
    type  : str

class WhatsAppContactPayload_Org (BaseModel) :
    """
    WhatsApp incoming contact organization
        `company` : "<company name>"
    """
    model_config = ConfigDict( frozen = True)
    
    company    : str
    department : str | None = None
    title      : str | None = None

class WhatsAppContactPayload_Address (BaseModel) :
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

class WhatsAppContactPayload_Url (BaseModel) :
    """
    WhatsApp incoming contact URL
        `type` : "HOME" | "WORK" | str | null
        `url`  : "<URL>"
    """
    model_config = ConfigDict( frozen = True)
    
    type : str | None = None
    url  : str

class WhatsAppContactPayload (BaseModel) :
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

class WhatsAppLocation (BaseModel) :
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


# =========================================================================================
# INBOUND: MESSAGES

class WhatsAppMetaData (BaseModel) :
    """
    WhatsApp message or status recipient metadata.
        `display_phone_number` : "<receiver phone number>"
        `phone_number_id`      : "<receiver WhatsApp number ID>"
    """
    model_config = ConfigDict( frozen = True)
    
    display_phone_number : NumericID # Receiver phone number
    phone_number_id      : NumericID # Receiver phone number ID

class WhatsAppProfile (BaseModel) :
    """
    WhatsApp contact profile corresponding to message sender (NOT CONTACT CARD)
        `name`     : "<display name>"
        `username` : "<username>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    name     : NE_str
    username : WhatsAppUsername | None = None

class WhatsAppContact (BaseModel) :
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
                f"{self.__class__.__name__} is missing both fields 'wa_id' and 'user_id'"
            )
        return self

class WhatsAppContext (BaseModel) :
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

class WhatsAppInteractiveReply (BaseModel) :
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

class WhatsAppMediaData (BaseModel) :
    """
    WhatsApp media descriptor
        `id`        : "<media ID>"
        `mime_type` : "<MIME type>"
        `sha256`    : "<sha256 checksum>"
        `caption`   : "<caption>" | null
        `filename`  : "<filename>" | null
        `voice`     : true | false | null
        `animated`  : true | false | null
    """
    model_config = ConfigDict( frozen = True)
    
    id        : NumericID
    mime_type : MIME_Type
    sha256    : HexHash
    caption   : WhatsAppTextBody | None = None # image, video, and document
    filename  : NE_str           | None = None # document
    voice     : bool             | None = None # audio
    animated  : bool             | None = None # sticker
    
    @property
    def extension(self) -> str :
        return self.mime_type.split("/")[1]
    
    @property
    def type(self) -> str :
        return self.mime_type.split("/")[0]

class WhatsAppReaction (BaseModel) :
    """
    WhatsApp reaction
        `message_id` : "<message ID>"
        `emoji`      : "<emoji>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    message_id : WhatsAppMessageID
    emoji      : str | None = None

class WhatsAppMessage (BaseModel) :
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
        `document`     : `WhatsAppMediaData`        | null
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
    
    user      : NumericID     | None = Field( alias   = "from",         default = None)
    user_id   : WhatsAppBSUID | None = Field( alias   = "from_user_id", default = None)
    
    id        : WhatsAppMessageID
    timestamp : UnixTS
    type      : WhatsAppMessageType
    
    # In a WhatsApp message only one of the fields below will be present
    # (more precisely, the field that matches the message `type`).
    text        : WhatsAppText             | None = None
    interactive : WhatsAppInteractiveReply | None = None
    image       : WhatsAppMediaData        | None = None
    video       : WhatsAppMediaData        | None = None
    audio       : WhatsAppMediaData        | None = None
    document    : WhatsAppMediaData        | None = None
    sticker     : WhatsAppMediaData        | None = None
    reaction    : WhatsAppReaction         | None = None
    contacts    : tuple[ WhatsAppContactPayload, ...] | None = None
    location    : WhatsAppLocation                    | None = None
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if not ( self.user or self.user_id ) :
            raise ValueError(
                f"Received {self.__class__.__name__} is missing both fields "
                f"'from' and 'from_user_id'"
            )
        
        if not (
            getattr( self, self.type, None) or ( self.type == "unsupported" )
        ) :
            raise ValueError(
                f"Received {self.__class__.__name__} has field 'type' = '{self.type}' "
                f"but field '{self.type}' is missing"
            )
        
        return self
    
    @property
    def media_data(self) -> WhatsAppMediaData | None :
        
        if self.type in { "audio", "document", "image", "sticker", "video" } :
            return getattr( self, self.type, None)
        
        return None

class WhatsAppMessageEcho (WhatsAppMessage) :
    """
    WhatsApp message echo payload
    
    Includes all the fields in `WhatsAppMessage` along with:
        `to`: "<receiver phone number>"
    """
    
    to         : NumericID     | None = None
    to_user_id : WhatsAppBSUID | None = None
    
    @model_validator( mode = "after")
    def validate_recipient(self) -> Self :
        
        if not ( self.to or self.to_user_id) :
            raise ValueError(
                f"Received {self.__class__.__name__} is missing both fields "
                f"'to' and 'to_user_id'"
            )
        
        return self

# =========================================================================================
# INBOUND: STATUSES OF SENT MESSAGES

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
    
    id                   : NumericID
    origin               : WhatsAppConversationOrigin | None = None
    expiration_timestamp : UnixTS                     | None = None

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


# =========================================================================================
# INBOUND: PAYLOADS

class WhatsAppValue (BaseModel) :
    """
    WhatsApp change value payload
        `messaging_product` : "whatsapp"
        `metadata`          : WhatsAppMetaData
        `contacts`          : tuple[ WhatsAppContact ]
        `messages`          : tuple[ WhatsAppMessage,     ...]
        `statuses`          : tuple[ WhatsAppStatus,      ...]
        `message_echoes`    : tuple[ WhatsAppMessageEcho, ...]
    """
    
    model_config = ConfigDict( frozen = True)
    
    messaging_product : Literal["whatsapp"] = "whatsapp"
    
    metadata       : WhatsAppMetaData
    contacts       : tuple[ WhatsAppContact ] # Exactly one item
    messages       : tuple[ WhatsAppMessage,     ...] = ()
    statuses       : tuple[ WhatsAppStatus,      ...] = ()
    message_echoes : tuple[ WhatsAppMessageEcho, ...] = ()
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if not ( self.messages or self.statuses or self.message_echoes ) :
            raise ValueError(
                f"Received {self.__class__.__name__} includes no messages, statuses, "
                f"or message echoes"
            )
        
        return self
    
    @model_serializer( mode = "wrap")
    def serialize_without_nones(
        self,
        handler: Callable[ [BaseModel], dict[ str, Any]],
    ) -> dict[ str, Any] :
        
        return serialize_without_nones( self, handler)

class WhatsAppPartnerWABAInfo (BaseModel) :
    """
    WhatsApp Business Account data included in partner updates
        `waba_id`           : "<WhatsApp Business Account ID>"
        `owner_business_id` : "<owner Meta Business Account ID>"
        `partner_app_id`    : "<partner app ID>" | null
    """
    
    model_config = ConfigDict( frozen = True)
    
    waba_id           : NumericID
    owner_business_id : NumericID
    partner_app_id    : NumericID | None = None

class WhatsAppPartnerUpdate (BaseModel) :
    """
    WhatsApp partner account update
        `event`     : "PARTNER_ADDED"   | "PARTNER_APP_INSTALLED" |
                      "PARTNER_REMOVED" | "PARTNER_APP_UNINSTALLED"
        `waba_info` : WhatsAppPartnerWABAInfo
    """
    
    model_config = ConfigDict( frozen = True)
    
    event : Literal[
        "PARTNER_ADDED",
        "PARTNER_APP_INSTALLED",
        "PARTNER_REMOVED",
        "PARTNER_APP_UNINSTALLED",
    ]
    waba_info : WhatsAppPartnerWABAInfo

class WhatsAppChange (BaseModel) :
    """
    WhatsApp change item
        `value` : WhatsAppValue | WhatsAppPartnerUpdate
        `field` : "<webhook_field>"
    Currently supported fields:
        `account_update`     : Partner account and app updates
        `messages`           : Regular inbound messages
        `smb_message_echoes` : Human-originated outbound messages (mobile app)
    """
    
    model_config = ConfigDict( frozen = True)
    
    value : WhatsAppValue | WhatsAppPartnerUpdate
    field : Literal[
                "account_update",
                "messages",
                "smb_message_echoes",
            ]
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if (
            ( self.field == "messages"                           ) and
            (
                ( not isinstance( self.value, WhatsAppValue) ) or
                ( not ( self.value.messages or self.value.statuses ) )
            )
        ) :
            raise ValueError(
                f"Received {self.__class__.__name__} has 'field' = 'messages' "
                f"but fields 'value.messages' and 'value.statuses' are both empty"
            )
        elif (
            ( self.field == "smb_message_echoes" ) and
            (
                ( not isinstance( self.value, WhatsAppValue) ) or
                ( not self.value.message_echoes )
            )
        ) :
            raise ValueError(
                f"Received {self.__class__.__name__} has 'field' = 'smb_message_echoes' "
                f"but field 'value.message_echoes'"
            )
        elif (
            ( self.field == "account_update" ) and
            ( not isinstance( self.value, WhatsAppPartnerUpdate) )
        ) :
            raise ValueError(
                f"Received {self.__class__.__name__} has 'field' = 'account_update' "
                f"but field 'value' is not a {WhatsAppPartnerUpdate.__name__}"
            )
        
        return self

class WhatsAppPayloadItem (BaseModel) :
    """
    WhatsApp payload item
        `id`      : "<receiver WABA number>"
        `time`    : "<unix timestamp>" | null
        `changes` : tuple[ WhatsAppChange, ...]
    """
    
    model_config = ConfigDict( frozen = True)
    
    id      : NumericID # Receiver WABA ID
    time    : int | None = None
    changes : Annotated[ tuple[ WhatsAppChange, ...], Field( min_length = 1)]

class WhatsAppPayload (BaseModel) :
    """
    Top-level WhatsApp webhook payload
        `object` : "whatsapp_business_account"
        `entry`  : tuple[ WhatsAppPayloadItem, ...]
    NOTE:
        Since `object` is a reserved keyword in Python here we declare it
        as the dummy field `object_field` and then assign it the alias `object`.
    """
    
    model_config = ConfigDict( frozen = True)
    
    object_field : NE_str = Field(
                                alias   = "object",
                                default = "whatsapp_business_account",
                            )
    entry        : Annotated[
                       tuple[ WhatsAppPayloadItem, ...],
                       Field( min_length = 1),
                   ]
    
    def has_messages(self) -> bool :
        return any(
            isinstance( change.value, WhatsAppValue) and change.value.messages
            for entry in self.entry
            for change in entry.changes
        )


# =========================================================================================
# OUTBOUND

class WhatsApp_OB_PayloadHeader ( BaseModel, ABC) :
    
    messaging_product : Literal["whatsapp"]   = "whatsapp"
    recipient_type    : Literal["individual"] = "individual"
    to                : NumericID     | None  = None
    recipient         : WhatsAppBSUID | None  = None
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if not ( self.to or self.recipient ) :
            raise ValueError(
                f"{self.__class__.__name__} is missing both fields 'to' and 'recipient'"
            )
        
        if not hasattr( self, "type") :
            raise ValueError(
                f"{self.__class__.__name__} is missing field 'type'"
            )
        
        return self
    
    @model_serializer( mode = "wrap")
    def serialize_without_nones(
        self,
        handler: Callable[ [BaseModel], dict[ str, Any]],
    ) -> dict[ str, Any] :
        
        return serialize_without_nones( self, handler)

# -----------------------------------------------------------------------------------------
# OUTBOUND: Text

class WhatsApp_OB_TextMessage (WhatsApp_OB_PayloadHeader) :
    
    type : Literal["text"] = "text"
    text : WhatsAppText

# -----------------------------------------------------------------------------------------
# OUTBOUND: Interactive Messages

class WhatsApp_OB_InteractiveOptionsHeaderObject (BaseModel) :
    
    type : Literal["text"] = "text"
    text : WhatsAppInteractiveHeaderFooter

class WhatsApp_OB_InteractiveOptionsBodyObject (BaseModel) :
    
    text : WhatsAppInteractiveBody

class WhatsApp_OB_InteractiveOptionsFooterObject (BaseModel) :
    
    text : WhatsAppInteractiveHeaderFooter

class WhatsApp_OB_InteractiveOptionsButtonEntry (BaseModel) :
    
    type  : Literal["reply"] = "reply"
    reply : WhatsAppInteractiveOption
    
    @model_serializer( mode = "wrap")
    def serialize_without_option_descriptions(
        self,
        handler: Callable[ [BaseModel], dict[ str, Any]],
    ) -> dict[ str, Any] :
        
        payload = handler(self)
        payload["reply"].pop( "description", None)
        
        return payload

class WhatsApp_OB_InteractiveOptionsButtons (BaseModel) :
    
    buttons : Annotated[
                list[WhatsApp_OB_InteractiveOptionsButtonEntry],
                Field( min_length = 1, max_length = 3),
              ]

class WhatsApp_OB_InteractiveOptionsListEntries (BaseModel) :
    
    rows : Annotated[
                list[WhatsAppInteractiveOption],
                Field( min_length = 1, max_length = 10),
           ]

class WhatsApp_OB_InteractiveOptionsList (BaseModel) :
    
    button   : WhatsAppInteractiveButtonLabel
    sections : Annotated[
                    list[WhatsApp_OB_InteractiveOptionsListEntries],
                    Field( min_length = 1, max_length = 1)
               ]

class WhatsApp_OB_InteractiveOptionsData (BaseModel) :
    
    type   : Literal[ "button", "list"]
    
    header : WhatsApp_OB_InteractiveOptionsHeaderObject | None = None
    body   : WhatsApp_OB_InteractiveOptionsBodyObject
    footer : WhatsApp_OB_InteractiveOptionsFooterObject | None = None
    action : (
        WhatsApp_OB_InteractiveOptionsButtons |
        WhatsApp_OB_InteractiveOptionsList
    )
    
    @model_serializer( mode = "wrap")
    def serialize_without_nones(
        self,
        handler: Callable[ [BaseModel], dict[ str, Any]],
    ) -> dict[ str, Any] :
        
        return serialize_without_nones( self, handler)
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if (
            (
                ( self.type == "button" ) and
                ( not isinstance( self.action, WhatsApp_OB_InteractiveOptionsButtons) )
            )
            or
            (
                ( self.type == "list" ) and
                ( not isinstance( self.action, WhatsApp_OB_InteractiveOptionsList) )
            )
        ) :
            
            expected_type = {
                "button" : WhatsApp_OB_InteractiveOptionsButtons.__name__,
                "list"   : WhatsApp_OB_InteractiveOptionsList.__name__,
            }.get(self.type)
            
            raise ValueError(
                f"{self.__class__.__name__} has field 'type' = '{self.type}' yet "
                f"its field 'action' is of type '{type(self.action).__name__}'; "
                f"the expected field type for 'action' is '{expected_type}'."
            )
        
        return self

class WhatsApp_OB_InteractiveOptionsMessage (WhatsApp_OB_PayloadHeader) :
    
    type        : Literal["interactive"] = "interactive"
    interactive : WhatsApp_OB_InteractiveOptionsData

# -----------------------------------------------------------------------------------------
# OUTBOUND: Template Messages

class WhatsApp_OB_TemplateLanguageObject (BaseModel) :
    
    code : WhatsAppTemplateLanguageCode

class WhatsApp_OB_TemplateTextParameter (BaseModel) :
    
    type           : Literal["text"]    = "text"
    parameter_name : NE_var_name | None = None
    text           : WhatsAppTextBody
    
    @model_serializer( mode = "wrap")
    def serialize_without_nones(
        self,
        handler: Callable[ [BaseModel], dict[ str, Any]],
    ) -> dict[ str, Any] :
        
        return serialize_without_nones( self, handler)

class WhatsApp_OB_TemplateBodyComponent (BaseModel) :
    
    type       : Literal["body"] = "body"
    parameters : Annotated[
        list[WhatsApp_OB_TemplateTextParameter],
        Field( min_length = 1),
    ]
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        has_named = any( par.parameter_name for par in self.parameters )
        if (
            has_named and
            ( not all( par.parameter_name for par in self.parameters ) )
        ):
            raise ValueError(
                f"{self.__class__.__name__} parameters must be all named or all positional"
            )
        
        return self

class WhatsApp_OB_TemplateData (BaseModel) :
    
    name       : NE_str
    language   : WhatsApp_OB_TemplateLanguageObject
    components : Annotated[
        list[WhatsApp_OB_TemplateBodyComponent],
        Field( min_length = 1, max_length = 1),
    ] | None = None
    
    @model_serializer( mode = "wrap")
    def serialize_without_nones(
        self,
        handler: Callable[ [BaseModel], dict[ str, Any]],
    ) -> dict[ str, Any] :
        
        return serialize_without_nones( self, handler)

class WhatsApp_OB_TemplateMessage (WhatsApp_OB_PayloadHeader) :
    
    type     : Literal["template"] = "template"
    template : WhatsApp_OB_TemplateData


# -----------------------------------------------------------------------------------------
# OUTBOUND: Media

class WhatsApp_OB_MediaData (BaseModel) :
    
    id       : NumericID
    caption  : NE_str | None = None
    filename : NE_str | None = None
    
    @model_serializer( mode = "wrap")
    def serialize_without_nones(
        self,
        handler: Callable[ [BaseModel], dict[ str, Any]],
    ) -> dict[ str, Any] :
        
        return serialize_without_nones( self, handler)

class WhatsApp_OB_MediaMessage (WhatsApp_OB_PayloadHeader) :
    
    type     : WhatsApp_OB_MediaType
    image    : WhatsApp_OB_MediaData | None = None
    video    : WhatsApp_OB_MediaData | None = None
    audio    : WhatsApp_OB_MediaData | None = None
    document : WhatsApp_OB_MediaData | None = None
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if not getattr( self, self.type, None) :
            raise ValueError(
                f"In {self.__class__.__name__}: Field 'type' has value '{self.type}' "
                f"but field '{self.type}' is missing"
            )
        
        if (
            ( 1 if self.image    else 0 ) +
            ( 1 if self.video    else 0 ) +
            ( 1 if self.audio    else 0 ) +
            ( 1 if self.document else 0 )
        ) > 1 :
            raise ValueError(
                f"In {self.__class__.__name__}: Conflicting fields"
            )
        
        if (
            ( not ( self.type == "document" )         ) and
            ( media_data := getattr( self, self.type) ) and
            ( getattr( media_data, "filename", None)  )
        ) :
            raise ValueError(
                f"In {self.__class__.__name__}: Field 'type' has value '{self.type}' "
                f"but field '{self.type}' has non-trivial field 'filename'"
            )
        
        return self
    
    @model_serializer( mode = "wrap")
    def serialize_without_nones(
        self,
        handler: Callable[ [BaseModel], dict[ str, Any]],
    ) -> dict[ str, Any] :
        
        return serialize_without_nones( self, handler)
    
    @property
    def media_data(self) -> WhatsApp_OB_MediaData | None :
        
        return getattr( self, self.type, None)


# -----------------------------------------------------------------------------------------
# OUTBOUND: Contacts & Locations

class WhatsApp_OB_ContactsMessage (WhatsApp_OB_PayloadHeader) :
    
    type     : Literal["contacts"] = "contacts"
    contacts : Annotated[ list[WhatsAppContactPayload], Field( min_length = 1)]

class WhatsApp_OB_LocationMessage (WhatsApp_OB_PayloadHeader) :
    
    type     : Literal["location"] = "location"
    location : WhatsAppLocation
