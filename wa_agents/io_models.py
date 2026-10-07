"""
WhatsApp BaseModels \\
References:
* https://developers.facebook.com/docs/whatsapp/cloud-api/webhooks/reference/messages
* https://developers.facebook.com/documentation/business-messaging/whatsapp/business-scoped-user-ids/
"""

import json

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
    Base64_str,
    MIME_Type,
    NE_str,
    NE_var_name,
    NO_WS_str,
    NumericID,
    UnixTS,
    serialize_without_nones,
)


# =========================================================================================
# BASE TYPES

# Strings constrained by regex pattern and/or length specs

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

# Literal-constrained strings

type WhatsAppFlowAction = Literal[
    "ping",
    "INIT",
    "BACK",
    "data_exchange",
]
""" WhatsApp Flow endpoint request action """

type WhatsAppMediaSHA256 = Annotated[
    str, Field( pattern = r"^[A-Za-z0-9+/]{43}=$"),
]
""" Base64-encoded SHA-256 digest supplied by Meta for inbound media. """

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

class WhatsAppContactCard_Name (BaseModel) :
    """
    WhatsApp contact-card name
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

class WhatsAppContactCard_Phone (BaseModel) :
    """
    WhatsApp contact-card phone
        `phone` : "<phone number starting with plus sign>"
        `type`  : "CELL" | "Mobile" | "Landline" | str
        `wa_id` : "<WhatsApp phone number ID>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    phone : str
    type  : str
    wa_id : str | None = None

class WhatsAppContactCard_Email (BaseModel) :
    """
    WhatsApp contact-card email
        `email` : "<email>"
        `type`  : "Work" | "Personal" | str
    """
    model_config = ConfigDict( frozen = True)
    
    email : str
    type  : str

class WhatsAppContactCard_Org (BaseModel) :
    """
    WhatsApp contact-card organization
        `company` : "<company name>"
    """
    model_config = ConfigDict( frozen = True)
    
    company    : str
    department : str | None = None
    title      : str | None = None

class WhatsAppContactCard_Address (BaseModel) :
    """
    WhatsApp contact-card address
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

class WhatsAppContactCard_Url (BaseModel) :
    """
    WhatsApp contact-card URL
        `type` : "HOME" | "WORK" | str | null
        `url`  : "<URL>"
    """
    model_config = ConfigDict( frozen = True)
    
    type : str | None = None
    url  : str

class WhatsAppContactCard (BaseModel) :
    """
    WhatsApp contact card
        `name`   : `WhatsAppContactCard_Name`
        `phones` : `tuple[ WhatsAppContactCard_Phone, ...]`
        `org`    : `WhatsAppContactCard_Org`                | null
        `emails` : `tuple[ WhatsAppContactCard_Email, ...]` | null
    NOTE:
        * This class models a contact card in inbound and outbound WhatsApp messages
        * Different from `WhatsApp_IB_Contact`
    """
    model_config = ConfigDict( frozen = True)
    
    name   : WhatsAppContactCard_Name
    phones : tuple[ WhatsAppContactCard_Phone, ...]
    org    : WhatsAppContactCard_Org                | None = None
    emails : tuple[ WhatsAppContactCard_Email, ...] | None = None
    birthday  : str                                         | None = None
    addresses : tuple[ WhatsAppContactCard_Address, ...] | None = None
    urls      : tuple[ WhatsAppContactCard_Url, ...]     | None = None

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

class WhatsApp_IB_MetaData (BaseModel) :
    """
    WhatsApp message or status recipient metadata.
        `display_phone_number` : "<receiver phone number>"
        `phone_number_id`      : "<receiver WhatsApp number ID>"
    """
    model_config = ConfigDict( frozen = True)
    
    display_phone_number : NumericID # Receiver phone number
    phone_number_id      : NumericID # Receiver phone number ID

class WhatsApp_IB_Profile (BaseModel) :
    """
    WhatsApp contact profile corresponding to message sender (NOT CONTACT CARD)
        `name`     : "<display name>"
        `username` : "<username>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    name     : NE_str
    username : WhatsAppUsername | None = None

class WhatsApp_IB_Contact (BaseModel) :
    """
    WhatsApp contact corresponding to message sender (NOT CONTACT CARD)
        
        `profile` : WhatsApp_IB_Profile | null
        `wa_id`   : "<sender phone number>"
        `user_id` : "<BSUID>"       | null
    NOTE:
        * This class models contact data ASSOCIATED WITH an incoming WhatsApp_IB_Message
        * Different from `WhatsAppContactCard`
    """
    model_config = ConfigDict( frozen = True)
    
    profile : WhatsApp_IB_Profile | None = None
    wa_id   : NumericID       | None = None # Sender phone number
    user_id : WhatsAppBSUID   | None = None # Sender BSUID
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        if not ( self.wa_id or self.user_id ) :
            raise ValueError(
                f"{self.__class__.__name__} is missing both fields 'wa_id' and 'user_id'"
            )
        return self

class WhatsApp_IB_Context (BaseModel) :
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

class WhatsApp_IB_FlowReply (BaseModel) :
    """
    Terminal Flow reply delivered through the messages webhook
        `name`          : "flow"
        `body`          : Meta's human-readable completion status | null
        `response_json` : JSON-encoded completion payload defined by the Flow
    """
    model_config = ConfigDict( frozen = True)
    
    name          : Literal["flow"] = "flow"
    body          : str | None      = None
    response_json : str
    
    @property
    def response(self) -> dict[str, Any] :
        """
        Parse and return the Flow-defined completion payload
        """
        data = json.loads(self.response_json)
        if not isinstance( data, dict) :
            raise ValueError("Flow response_json must contain an object")
        
        return data

class WhatsApp_IB_InteractiveReply (BaseModel) :
    """
    WhatsApp interactive reply
        `type`         : "button_reply" | "list_reply" | "nfm_reply"
        `button_reply` : InteractiveOption | null
        `list_reply`   : InteractiveOption | null
        `nfm_reply`    : WhatsApp_IB_FlowReply | null
    """
    model_config = ConfigDict( frozen = True)
    
    type         : Literal[ "button_reply", "list_reply", "nfm_reply"]
    button_reply : WhatsAppInteractiveOption | None = None
    list_reply   : WhatsAppInteractiveOption | None = None
    nfm_reply    : WhatsApp_IB_FlowReply     | None = None
    
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

class WhatsApp_IB_MediaData (BaseModel) :
    """
    WhatsApp media descriptor
        `id`        : "<media ID>"
        `mime_type` : "<MIME type>"
        `sha256`    : "<Base64-encoded sha256 checksum>"
        `caption`   : "<caption>" | null
        `filename`  : "<filename>" | null
        `voice`     : true | false | null
        `animated`  : true | false | null
    """
    model_config = ConfigDict( frozen = True)
    
    id        : NumericID
    mime_type : MIME_Type
    sha256    : WhatsAppMediaSHA256
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

class WhatsApp_IB_Reaction (BaseModel) :
    """
    WhatsApp reaction
        `message_id` : "<message ID>"
        `emoji`      : "<emoji>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    message_id : WhatsAppMessageID
    emoji      : str | None = None

class WhatsApp_IB_MessageErrorData (BaseModel) :
    """
    WhatsApp inbound message error details
        `details` : "<error details>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    details : NE_str | None = None

class WhatsApp_IB_MessageError (BaseModel) :
    """
    WhatsApp inbound message error
        `code`       : <error code>
        `title`      : "<error title>"
        `message`    : "<error message>" | null
        `error_data` : WhatsApp_IB_MessageErrorData | null
        `href`       : "<error code URL>" | null
    Reference:
        https://developers.facebook.com/documentation/business-messaging/whatsapp/webhooks/reference/messages/errors
    """
    model_config = ConfigDict( frozen = True)
    
    code       : int
    title      : NE_str
    message    : NE_str                       | None = None
    error_data : WhatsApp_IB_MessageErrorData | None = None
    href       : NE_str                       | None = None

class WhatsApp_IB_UnsupportedData (BaseModel) :
    """
    WhatsApp unsupported message data
        `type`     : "<unsupported message type>"
        `raw_type` : "<raw unsupported message type>"
    Reference:
        https://developers.facebook.com/documentation/business-messaging/whatsapp/webhooks/reference/messages/unsupported
    NOTE:
        Field `raw_type` was observed in a received payload on 2026-10-07,
        yet it does not seem to be included in the Meta docs.
    """
    model_config = ConfigDict( frozen = True)
    
    type     : NE_str
    raw_type : NE_str

class WhatsApp_IB_Message (BaseModel) :
    """
    WhatsApp message payload
        `from`         : "<sender phone number>"
        `from_user_id` : "<sender BSUID>"
        `id`           : "<message ID>"
        `timestamp`    : "<unix timestamp>"
        `type`         : "<message type>"
        
        `contacts`    : `tuple[ WhatsAppContactCard, ...]`      | null
        `errors`      : `tuple[ WhatsApp_IB_MessageError, ...]` | null
        `interactive` : `WhatsApp_IB_InteractiveReply`          | null
        `location`    : `WhatsAppLocation`      | null
        `reaction`    : `WhatsApp_IB_Reaction`  | null
        `text`        : `WhatsAppText`          | null
        `unsupported` : `WhatsApp_IB_UnsupportedData` | null
        
        `audio`       : `WhatsApp_IB_MediaData` | null
        `document`    : `WhatsApp_IB_MediaData` | null
        `image`       : `WhatsApp_IB_MediaData` | null
        `sticker`     : `WhatsApp_IB_MediaData` | null
        `video`       : `WhatsApp_IB_MediaData` | null
    
    NOTE:
        * Since `from` is a reserved keyword in Python here we declare it as the dummy field `user` and then assign it the alias `from`.
        * Similarly `from_user_id` is declared as `user_id` and then aliased.
    """
    model_config = ConfigDict( frozen           = True,
                               populate_by_name = True)
    
    context   : WhatsApp_IB_Context | None = None
    
    user      : NumericID     | None = Field( alias   = "from",         default = None)
    user_id   : WhatsAppBSUID | None = Field( alias   = "from_user_id", default = None)
    
    id        : WhatsAppMessageID
    timestamp : UnixTS
    type      : WhatsAppMessageType
    
    # In a WhatsApp message only one of the fields below will be present
    # (more precisely, the field that matches the message `type`).
    
    contacts    : tuple[ WhatsAppContactCard, ...]      | None = None
    errors      : tuple[ WhatsApp_IB_MessageError, ...] | None = None
    interactive : WhatsApp_IB_InteractiveReply          | None = None
    location    : WhatsAppLocation      | None = None
    reaction    : WhatsApp_IB_Reaction  | None = None
    text        : WhatsAppText          | None = None
    unsupported : WhatsApp_IB_UnsupportedData | None = None
    
    audio       : WhatsApp_IB_MediaData | None = None
    document    : WhatsApp_IB_MediaData | None = None
    image       : WhatsApp_IB_MediaData | None = None
    sticker     : WhatsApp_IB_MediaData | None = None
    video       : WhatsApp_IB_MediaData | None = None
    
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
    def media_data(self) -> WhatsApp_IB_MediaData | None :
        
        if self.type in { "audio", "document", "image", "sticker", "video" } :
            return getattr( self, self.type, None)
        
        return None

class WhatsApp_IB_MessageEcho (WhatsApp_IB_Message) :
    """
    WhatsApp message echo payload
    
    Includes all the fields in `WhatsApp_IB_Message` along with:
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

class WhatsApp_IB_ConversationOrigin (BaseModel) :
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

class WhatsApp_IB_Conversation (BaseModel) :
    """
    WhatsApp status conversation data
        `id`                   : "<conversation ID>"
        `origin`               : WhatsApp_IB_ConversationOrigin | null
        `expiration_timestamp` : "<unix timestamp>"             | null
    """
    model_config = ConfigDict( frozen = True)
    
    id                   : NumericID
    origin               : WhatsApp_IB_ConversationOrigin | None = None
    expiration_timestamp : UnixTS                         | None = None

class WhatsApp_IB_Pricing (BaseModel) :
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

class WhatsApp_IB_StatusErrorData (BaseModel) :
    """
    WhatsApp status error details
        `details` : "<error details>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    details : str | None = None

class WhatsApp_IB_StatusError (BaseModel) :
    """
    WhatsApp status error
        `code`       : <error code>
        `title`      : "<error title>"
        `message`    : "<error message>" | null
        `error_data` : WhatsApp_IB_StatusErrorData | null
        `href`       : "<error code URL>" | null
    """
    model_config = ConfigDict( frozen = True)
    
    code       : int
    title      : NE_str
    message    : NE_str                      | None = None
    error_data : WhatsApp_IB_StatusErrorData | None = None
    href       : NE_str                      | None = None

class WhatsApp_IB_Status (BaseModel) :
    """
    WhatsApp outbound message status update
        `id`                : "<WhatsApp message ID>"
        `recipient_id`      : "<user phone number>"
        `recipient_user_id` : "<user BSUID>"
        `status`            : "delivered" | "failed" | "played" | "read" | "sent" | null
        `timestamp`         : "<unix timestamp>"
        `conversation`      : WhatsApp_IB_Conversation | null
        `pricing`           : WhatsApp_IB_Pricing | null
        `errors`            : tuple[ WhatsApp_IB_StatusError, ...] | null
    """
    model_config = ConfigDict( frozen = True)
    
    id                : WhatsAppMessageID
    recipient_id      : NumericID     | None = None # Receiver phone number
    recipient_user_id : WhatsAppBSUID | None = None # Receiver BSUID
    status            : Literal[
        "delivered",
        "failed",
        "played",
        "read",
        "sent",
    ]
    
    timestamp    : UnixTS
    conversation : WhatsApp_IB_Conversation             | None = None
    pricing      : WhatsApp_IB_Pricing                  | None = None
    errors       : tuple[ WhatsApp_IB_StatusError, ...] | None = None
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if not ( self.recipient_id or self.recipient_user_id ) :
            raise ValueError(
                "WhatsApp_IB_Status is missing both fields "
                "'recipient_id' and 'recipient_user_id'"
            )
        
        return self


# =========================================================================================
# INBOUND: PAYLOADS

class WhatsApp_IB_FlowEvent (BaseModel) :
    """
    Lifecycle, endpoint-health, or client-error event for a Meta Flow
        `event`   : Meta Flow event name
        `flow_id` : Meta Flow ID | null
    Event-specific fields are preserved as extra model fields.
    """
    model_config = ConfigDict( extra = "allow", frozen = True)
    
    event   : NE_str
    flow_id : NumericID | None = None

class WhatsApp_IB_PartnerWABAInfo (BaseModel) :
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

class WhatsApp_IB_PartnerUpdate (BaseModel) :
    """
    WhatsApp partner account update
        `event`     : "PARTNER_ADDED"   | "PARTNER_APP_INSTALLED" |
                      "PARTNER_REMOVED" | "PARTNER_APP_UNINSTALLED"
        `waba_info` : WhatsApp_IB_PartnerWABAInfo
    """
    
    model_config = ConfigDict( frozen = True)
    
    event : Literal[
        "PARTNER_ADDED",
        "PARTNER_APP_INSTALLED",
        "PARTNER_REMOVED",
        "PARTNER_APP_UNINSTALLED",
    ]
    waba_info : WhatsApp_IB_PartnerWABAInfo

class WhatsApp_IB_Value (BaseModel) :
    """
    WhatsApp change value payload
        `messaging_product` : "whatsapp"
        `metadata`          : WhatsApp_IB_MetaData
        `contacts`          : tuple[ WhatsApp_IB_Contact ]
        `messages`          : tuple[ WhatsApp_IB_Message,     ...]
        `statuses`          : tuple[ WhatsApp_IB_Status,      ...]
        `message_echoes`    : tuple[ WhatsApp_IB_MessageEcho, ...]
    """
    
    model_config = ConfigDict( frozen = True)
    
    messaging_product : Literal["whatsapp"] = "whatsapp"
    
    metadata       : WhatsApp_IB_MetaData
    contacts       : tuple[ WhatsApp_IB_Contact ] # Exactly one item
    messages       : tuple[ WhatsApp_IB_Message,     ...] = ()
    statuses       : tuple[ WhatsApp_IB_Status,      ...] = ()
    message_echoes : tuple[ WhatsApp_IB_MessageEcho, ...] = ()
    
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

class WhatsApp_IB_Change (BaseModel) :
    """
    WhatsApp change item
        `value` :
            `WhatsApp_IB_FlowEvent`     |
            `WhatsApp_IB_PartnerUpdate` |
            `WhatsApp_IB_Value`
        `field` :
            `account_update`     : Partner account and app updates
            `flows`              : Flow status, endpoint-health, and client-error events
            `messages`           : Regular inbound messages
            `smb_message_echoes` : Human-originated outbound messages (mobile app)
    """
    
    model_config = ConfigDict( frozen = True)
    
    value : (
        WhatsApp_IB_FlowEvent     |
        WhatsApp_IB_PartnerUpdate |
        WhatsApp_IB_Value
    )
    field : Literal[
                "account_update",
                "flows",
                "messages",
                "smb_message_echoes",
            ]
    
    @model_validator( mode = "after")
    def validate(self) -> Self :
        
        if (
            ( self.field == "messages"                           ) and
            (
                ( not isinstance( self.value, WhatsApp_IB_Value) ) or
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
                ( not isinstance( self.value, WhatsApp_IB_Value) ) or
                ( not self.value.message_echoes )
            )
        ) :
            raise ValueError(
                f"Received {self.__class__.__name__} has 'field' = 'smb_message_echoes' "
                f"but field 'value.message_echoes'"
            )
        elif (
            ( self.field == "account_update" ) and
            ( not isinstance( self.value, WhatsApp_IB_PartnerUpdate) )
        ) :
            raise ValueError(
                f"Received {self.__class__.__name__} has 'field' = 'account_update' "
                f"but field 'value' is not a {WhatsApp_IB_PartnerUpdate.__name__}"
            )
        
        return self

class WhatsApp_IB_PayloadItem (BaseModel) :
    """
    WhatsApp payload item
        `id`      : "<receiver WABA number>"
        `time`    : "<unix timestamp>" | null
        `changes` : tuple[ WhatsApp_IB_Change, ...]
    """
    
    model_config = ConfigDict( frozen = True)
    
    id      : NumericID # Receiver WABA ID
    time    : int | None = None
    changes : Annotated[ tuple[ WhatsApp_IB_Change, ...], Field( min_length = 1)]

class WhatsApp_IB_Payload (BaseModel) :
    """
    Top-level WhatsApp webhook payload
        `object` : "whatsapp_business_account"
        `entry`  : tuple[ WhatsApp_IB_PayloadItem, ...]
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
                       tuple[ WhatsApp_IB_PayloadItem, ...],
                       Field( min_length = 1),
                   ]
    
    def has_messages(self) -> bool :
        return any(
            isinstance( change.value, WhatsApp_IB_Value) and change.value.messages
            for entry in self.entry
            for change in entry.changes
        )


# =========================================================================================
# INBOUND: FLOW DATA ENDPOINT

class WhatsApp_IB_Encrypted_FlowRequest (BaseModel) :
    """
    Encrypted request envelope posted by Meta to a Flow data endpoint
        `encrypted_flow_data` : Base64-encoded AES-GCM ciphertext and authentication tag
        `encrypted_aes_key`   : Base64-encoded RSA-OAEP encrypted AES key
        `initial_vector`      : Base64-encoded AES-GCM initialization vector
    """
    model_config = ConfigDict( frozen = True)
    
    encrypted_flow_data : Base64_str
    encrypted_aes_key   : Base64_str
    initial_vector      : Base64_str

class WhatsApp_IB_Decrypted_FlowRequest (BaseModel) :
    """
    Decrypted request received from the Flow client
        `version`    : Data-channel protocol version
        `action`     : Requested endpoint operation
        `flow_token` : Application-issued Flow session token | null
        `screen`     : Screen that initiated the exchange | null
        `data`       : Flow-defined request payload
    """
    model_config = ConfigDict( extra = "allow", frozen = True)
    
    version    : NO_WS_str
    action     : WhatsAppFlowAction
    flow_token : NO_WS_str | None = None
    screen     : NO_WS_str | None = None
    data       : dict[ str, Any] = Field( default_factory = dict)

    @property
    def is_client_error(self) -> bool :
        """
        Return whether the Flow client reported an execution error
        """
        return bool(self.data.get("error"))


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
    contacts : Annotated[ list[WhatsAppContactCard], Field( min_length = 1)]

class WhatsApp_OB_LocationMessage (WhatsApp_OB_PayloadHeader) :
    
    type     : Literal["location"] = "location"
    location : WhatsAppLocation

# -----------------------------------------------------------------------------------------
# OUTBOUND: Flow Data Endpoint

class WhatsApp_OB_FlowCompletionParams (BaseModel) :
    """
    Values returned through the terminal Flow reply webhook
        `flow_token` : Application-issued Flow session token
    NOTE:
        Flow-specific completion values are preserved as extra model fields.
    """
    model_config = ConfigDict( extra = "allow", frozen = True)
    
    flow_token : NO_WS_str

class WhatsApp_OB_FlowResponse (BaseModel) :
    """
    Logical endpoint response to encrypt and return to the Flow client
        `version` : Data-channel protocol version
        `screen`  : Next screen to display | null
        `data`    : Flow-defined response payload
    """
    model_config = ConfigDict( extra = "allow", frozen = True)
    
    version : NO_WS_str        = "3.0"
    screen  : NO_WS_str | None = None
    data    : dict[ str, Any]  = Field( default_factory = dict)
    
    @classmethod
    def health_check(cls) -> Self :
        """
        Return the response expected by Meta's Flow endpoint health check
        """
        return cls( data = { "status" : "active" })
    
    @classmethod
    def acknowledge_error(cls) -> Self :
        """
        Acknowledge an execution error reported by the Flow client
        """
        return cls( data = { "acknowledged" : True })
    
    @classmethod
    def next_screen(
        cls,
        screen : NO_WS_str,
        data   : dict[ str, Any] | None = None,
    ) -> Self :
        """
        Return the data required to render the next Flow screen \\
        Args:
            screen : Destination screen ID
            data   : Flow-defined data for the destination screen
        """
        return cls( screen = screen, data = data or {})
    
    @classmethod
    def complete(
        cls,
        flow_token : NO_WS_str,
        **params   : Any,
    ) -> Self :
        """
        Complete the Flow and return values through the reply webhook \\
        Args:
            flow_token : Application-issued Flow session token
            **params   : Flow-specific completion values
        """
        completion_params = WhatsApp_OB_FlowCompletionParams.model_validate(
            { **params, "flow_token" : flow_token}
        )
        return cls(
            screen = "SUCCESS",
            data   = {
                "extension_message_response" : {
                    "params" : completion_params.model_dump( mode = "json"),
                },
            },
        )
