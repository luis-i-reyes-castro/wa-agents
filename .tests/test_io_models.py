import pytest

from pydantic import (
    TypeAdapter,
    ValidationError,
)
from sofia_utils.pydantic import NE_str

from wa_agents.io_models import (
    WhatsAppCountryCode,
    WhatsAppInteractiveOption,
    WhatsApp_IB_Message,
    WhatsApp_IB_MessageEcho,
    WhatsApp_IB_Payload,
    WhatsApp_OB_InteractiveOptionsBodyObject,
    WhatsApp_OB_InteractiveOptionsButtonEntry,
    WhatsApp_OB_InteractiveOptionsButtons,
    WhatsApp_OB_InteractiveOptionsData,
    WhatsApp_OB_InteractiveOptionsMessage,
    WhatsApp_OB_InteractiveOptionsList,
    WhatsApp_OB_InteractiveOptionsListEntries,
    WhatsApp_OB_MediaData,
    WhatsApp_OB_TextMessage,
    WhatsApp_OB_TemplateBodyComponent,
    WhatsApp_OB_TemplateData,
    WhatsApp_OB_TemplateLanguageObject,
    WhatsApp_OB_TemplateMessage,
    WhatsApp_OB_TemplateTextParameter,
    WhatsAppText,
)
from wa_agents.supabase import (
    WhatsAppDatabaseRecord_Business,
    WhatsAppDatabaseRecord_Contact,
)


DOCUMENT_DATA = {
    "id"        : "123456789",
    "mime_type" : "application/pdf",
    "sha256"    : "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
    "filename"  : "invoice.pdf",
    "caption"   : "Invoice",
}


def test_nonempty_string_accepts_internal_newlines() -> None :
    value = "First line\nSecond line"

    assert TypeAdapter(NE_str).validate_python(value) == value


@pytest.mark.parametrize( "country_code", [ "EC", "ec" ] )
def test_country_code_accepts_two_letters(country_code : str) -> None :
    assert TypeAdapter(WhatsAppCountryCode).validate_python(country_code) == country_code


@pytest.mark.parametrize(
    "country_code",
    [ "E", "ECU", "EC-extra", "extra-ec", "E1", "Ec", "eC" ],
)
def test_country_code_rejects_invalid_values(country_code : str) -> None :
    with pytest.raises(ValidationError) :
        TypeAdapter(WhatsAppCountryCode).validate_python(country_code)


def test_media_captions_accept_internal_newlines() -> None :
    caption = "First line\nSecond line"
    inbound = WhatsApp_IB_Message.model_validate({
        "from"      : "593995341161",
        "id"        : "wamid.document",
        "timestamp" : "1788724265",
        "type"      : "document",
        "document"  : { **DOCUMENT_DATA, "caption" : caption },
    })
    outbound = WhatsApp_OB_MediaData(
        id      = "123456789",
        caption = caption,
    )

    assert inbound.document
    assert inbound.document.caption == caption
    assert outbound.caption == caption


def test_media_captions_reject_oversized_text() -> None :
    with pytest.raises(ValidationError) :
        WhatsApp_OB_MediaData(
            id      = "123456789",
            caption = "x" * 1025,
        )


@pytest.mark.parametrize(
    ( "identity", "contact_identity"),
    [
        ( "987654321", "593995341161"),
        ( "IECED_Autocomplete", "IECED_Request_ciphertext"),
    ],
)
def test_business_and_contact_identities_accept_meta_and_synthetic_values(
    identity         : str,
    contact_identity : str,
) -> None :
    business = WhatsAppDatabaseRecord_Business(
        row_id              = 5,
        display_phone_number = identity,
        phone_number_id      = identity,
        waba_id              = identity,
    )
    contact = WhatsAppDatabaseRecord_Contact(
        row_id  = 35,
        user_id = contact_identity,
    )

    assert business.api_id == identity
    assert contact.api_id == contact_identity


def test_synthetic_identities_parse_inbound_webhook() -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "object" : "whatsapp_business_account",
        "entry"  : [
            {
                "id"      : "IECED_Autocomplete",
                "changes" : [
                    {
                        "field" : "messages",
                        "value" : {
                            "metadata" : {
                                "display_phone_number" : "IECED_Autocomplete",
                                "phone_number_id"      : "IECED_Autocomplete",
                            },
                            "contacts" : [
                                { "user_id" : "IECED_Request_ciphertext" },
                            ],
                            "messages" : [
                                {
                                    "from_user_id" : "IECED_Request_ciphertext",
                                    "id"           : "IECED_Message_1",
                                    "timestamp"    : "1788724265",
                                    "type"         : "text",
                                    "text"         : { "body" : "Hello" },
                                },
                            ],
                        },
                    },
                ],
            },
        ],
    })

    assert payload.entry[0].id == "IECED_Autocomplete"
    assert payload.entry[0].changes[0].value.messages[0].id == "IECED_Message_1"


def test_history_webhook_parses_messages_and_placeholders() -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "object" : "whatsapp_business_account",
        "entry"  : [
            {
                "id"      : "123456789",
                "changes" : [
                    {
                        "field" : "history",
                        "value" : {
                            "messaging_product" : "whatsapp",
                            "metadata"          : {
                                "display_phone_number" : "593962939146",
                                "phone_number_id"      : "987654321",
                            },
                            "history" : [
                                {
                                    "metadata" : {
                                        "phase"       : 1,
                                        "chunk_order" : 4,
                                        "progress"    : 60,
                                    },
                                    "threads" : [
                                        {
                                            "id"       : "593995341161",
                                            "messages" : [
                                                {
                                                    "from"      : "593995341161",
                                                    "id"        : "wamid.history-text",
                                                    "timestamp" : "1788724265",
                                                    "type"      : "text",
                                                    "text"      : {
                                                        "body" : "Previous message"
                                                    },
                                                    "history_context" : {
                                                        "status" : "read"
                                                    },
                                                },
                                                {
                                                    "from"      : "593962939146",
                                                    "to"        : "593995341161",
                                                    "id"        : "wamid.history-media",
                                                    "timestamp" : "1788724270",
                                                    "type"      : "media_placeholder",
                                                    "history_context" : {
                                                        "status" : "delivered"
                                                    },
                                                },
                                            ],
                                        },
                                    ],
                                },
                            ],
                        },
                    },
                ],
            },
        ],
    })

    value   = payload.entry[0].changes[0].value
    history = value.history[0]
    assert history.metadata
    assert history.metadata.progress == 60
    assert history.threads[0].messages[0].text
    assert history.threads[0].messages[0].text.body == "Previous message"
    assert history.threads[0].messages[1].type == "media_placeholder"
    assert history.threads[0].messages[1].to == "593995341161"


def test_history_webhook_parses_error_message_and_completion_marker() -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "object" : "whatsapp_business_account",
        "entry"  : [
            {
                "id"      : "123456789",
                "changes" : [
                    {
                        "field" : "history",
                        "value" : {
                            "metadata" : {
                                "display_phone_number" : "593962939146",
                                "phone_number_id"      : "987654321",
                            },
                            "history" : [
                                {
                                    "metadata" : {
                                        "phase"       : 1,
                                        "chunk_order" : 1,
                                        "progress"    : 100,
                                    },
                                    "threads" : [
                                        {
                                            "id"       : "447710173736",
                                            "messages" : [
                                                {
                                                    "from"      : "447710173736",
                                                    "id"        : "wamid.history-error",
                                                    "timestamp" : "1792822797",
                                                    "errors" : [
                                                        {
                                                            "code" : 131051,
                                                            "title" : (
                                                                "Message type unknown"
                                                            ),
                                                        },
                                                    ],
                                                    "type" : "errors",
                                                    "history_context" : {
                                                        "status" : "pending"
                                                    },
                                                },
                                            ],
                                        },
                                    ],
                                },
                                {
                                    "metadata" : {
                                        "phase"       : 2,
                                        "chunk_order" : 1,
                                        "progress"    : 100,
                                    },
                                },
                            ],
                        },
                    },
                ],
            },
        ],
    })

    history = payload.entry[0].changes[0].value.history
    assert history[0].threads[0].messages[0].type == "errors"
    assert history[0].threads[0].messages[0].history_context.status == "pending"
    assert history[1].metadata and history[1].metadata.phase == 2
    assert history[1].threads == ()


def test_history_webhook_parses_declined_sharing() -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "object" : "whatsapp_business_account",
        "entry"  : [
            {
                "id"      : "123456789",
                "changes" : [
                    {
                        "field" : "history",
                        "value" : {
                            "metadata" : {
                                "display_phone_number" : "593962939146",
                                "phone_number_id"      : "987654321",
                            },
                            "history" : [
                                {
                                    "errors" : [
                                        {
                                            "code"    : 2593109,
                                            "title"   : "History sync is turned off",
                                            "message" : "History sharing was declined",
                                            "error_data" : {
                                                "details" : "Disabled in the app"
                                            },
                                        },
                                    ],
                                },
                            ],
                        },
                    },
                ],
            },
        ],
    })

    history = payload.entry[0].changes[0].value.history[0]
    assert history.metadata is None
    assert history.errors[0].code == 2593109


def test_history_webhook_parses_later_media_details() -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "object" : "whatsapp_business_account",
        "entry"  : [
            {
                "id"      : "123456789",
                "changes" : [
                    {
                        "field" : "history",
                        "value" : {
                            "messaging_product" : "whatsapp",
                            "metadata" : {
                                "display_phone_number" : "593962939146",
                                "phone_number_id"      : "987654321",
                            },
                            "messages" : [
                                {
                                    "from"      : "593995341161",
                                    "id"        : "wamid.history-media",
                                    "timestamp" : "1788724270",
                                    "type"      : "image",
                                    "image" : {
                                        "id"        : "24230790383178626",
                                        "mime_type" : "image/jpeg",
                                        "sha256" : (
                                            "P52U05n6YckRvBvEccgTpdA1zbG5tR8J"
                                            "6WOkFcbLDQw="
                                        ),
                                    },
                                },
                            ],
                        },
                    },
                ],
            },
        ],
    })

    value = payload.entry[0].changes[0].value
    assert not value.history
    assert value.messages[0].image
    assert value.messages[0].image.id == "24230790383178626"


def test_smb_app_state_sync_parses_contact_changes() -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "object" : "whatsapp_business_account",
        "entry"  : [
            {
                "id"      : "123456789",
                "changes" : [
                    {
                        "field" : "smb_app_state_sync",
                        "value" : {
                            "messaging_product" : "whatsapp",
                            "metadata" : {
                                "display_phone_number" : "593962939146",
                                "phone_number_id"      : "987654321",
                            },
                            "state_sync" : [
                                {
                                    "type"    : "contact",
                                    "action"  : "add",
                                    "contact" : {
                                        "full_name"    : "Pablo Morales",
                                        "first_name"   : "Pablo",
                                        "phone_number" : "16505551234",
                                    },
                                    "metadata" : { "timestamp" : "1738346006" },
                                },
                                {
                                    "type"    : "contact",
                                    "action"  : "remove",
                                    "contact" : {
                                        "phone_number" : "16505559876",
                                    },
                                    "metadata" : { "timestamp" : "1738346010" },
                                },
                            ],
                        },
                    },
                ],
            },
        ],
    })

    state_sync = payload.entry[0].changes[0].value.state_sync
    assert state_sync[0].contact.full_name == "Pablo Morales"
    assert state_sync[0].action == "add"
    assert state_sync[1].contact.full_name is None
    assert state_sync[1].action == "remove"


def test_account_updates_parse_observed_variants() -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "object" : "whatsapp_business_account",
        "entry"  : [
            {
                "id"      : "123456789",
                "changes" : [
                    {
                        "field" : "account_update",
                        "value" : {
                            "event"     : "PARTNER_REMOVED",
                            "waba_info" : {
                                "waba_id"           : "123456789",
                                "owner_business_id" : "987654321",
                            },
                            "disconnection_info" : {
                                "reason"       : "ACCOUNT_DISCONNECTED",
                                "initiated_by" : "USER",
                            },
                        },
                    },
                    {
                        "field" : "account_update",
                        "value" : {
                            "event"      : "PHONE_LINK_CHANGED",
                            "event_time" : "2026-10-24T01:35:02+0000",
                            "actor" : {
                                "business_id"   : "987654321",
                                "business_name" : "Example Business",
                            },
                            "details" : {
                                "change_action"   : "UNLINK",
                                "phone_number_id" : "111222333",
                            },
                        },
                    },
                    {
                        "field" : "account_update",
                        "value" : {
                            "event"      : "PHONE_LINK_CHANGED",
                            "event_time" : "2026-10-24T01:37:05+0000",
                            "actor"      : { "description" : "System actor" },
                            "details" : {
                                "change_action"   : "UNLINK",
                                "phone_number_id" : "111222333",
                            },
                        },
                    },
                    {
                        "field" : "account_update",
                        "value" : {
                            "event"        : "PHONE_NUMBER_REMOVED",
                            "phone_number" : "15551234567",
                        },
                    },
                    {
                        "field" : "account_update",
                        "value" : {
                            "event" : "PARTNER_APP_INSTALLED",
                            "waba_info" : {
                                "waba_id"           : "1078065061686317",
                                "owner_business_id" : "2748286302233297",
                                "partner_app_id"    : "868440515682488",
                            },
                        },
                    },
                    {
                        "field" : "account_update",
                        "value" : {
                            "event"      : "WEBHOOK_CONFIG_UPDATED",
                            "event_time" : "2026-10-24T06:21:48+0000",
                            "actor" : {
                                "business_id"   : "2748286302233297",
                                "business_name" : "Vibecoding SAS",
                            },
                            "details" : {
                                "change_action" : "subscribe",
                                "field"         : "subscribed_apps",
                            },
                        },
                    },
                ],
            },
        ],
    })

    changes = payload.entry[0].changes
    assert changes[0].value.disconnection_info
    assert changes[0].value.disconnection_info.initiated_by == "USER"
    assert changes[1].value.actor.business_name == "Example Business"
    assert changes[1].value.details.change_action == "UNLINK"
    assert changes[2].value.actor.description == "System actor"
    assert changes[3].value.phone_number == "15551234567"
    assert changes[4].value.waba_info.partner_app_id == "868440515682488"
    assert changes[5].value.details.change_action == "subscribe"
    assert changes[5].value.details.field == "subscribed_apps"


def test_flow_endpoint_availability_parses_observed_variant() -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "object" : "whatsapp_business_account",
        "entry"  : [
            {
                "id"      : "856535583972492",
                "time"    : 1792823003,
                "changes" : [
                    {
                        "field" : "flows",
                        "value" : {
                            "event"        : "ENDPOINT_AVAILABILITY",
                            "message" : (
                                "Flow endpoint availability successfully recovered."
                            ),
                            "flow_id"      : "1667950261438147",
                            "availability" : 100,
                            "threshold"    : 90,
                            "alert_state"  : "DEACTIVATED",
                        },
                    },
                ],
            },
        ],
    })

    value = payload.entry[0].changes[0].value
    assert value.event == "ENDPOINT_AVAILABILITY"
    assert value.availability == 100
    assert value.threshold == 90
    assert value.alert_state == "DEACTIVATED"


@pytest.mark.parametrize(
    "value",
    [
        { "event" : "ACCOUNT_DELETED" },
        { "event" : "ACCOUNT_OFFBOARDED" },
        { "event" : "ACCOUNT_RECONNECTED" },
        {
            "event" : "ACCOUNT_RESTRICTION",
            "phone_number" : "15550783881",
            "restriction_info" : [
                {
                    "restriction_type" : "RESTRICTED_BIZ_INITIATED_MESSAGING",
                    "expiration"       : 1641330498,
                },
                {
                    "restriction_type" : "RESTRICTED_ADD_PHONE_NUMBER_ACTION",
                    "expiration"       : 1641330498,
                    "remediation"      : "Review messaging practices.",
                },
            ],
        },
        {
            "event"          : "ACCOUNT_VIOLATION",
            "violation_info" : { "violation_type" : "ADULT" },
        },
        {
            "event" : "AD_ACCOUNT_LINKED",
            "waba_info" : {
                "waba_id"           : "980198427658004",
                "owner_business_id" : "2329417887457253",
                "ad_account_linked" : "980198427534243",
            },
        },
        {
            "event" : "AUTH_INTL_PRICE_ELIGIBILITY_UPDATE",
            "auth_international_rate_eligibility" : {
                "exception_countries" : [
                    { "country_code" : "ID", "start_time" : 1751347424 },
                ],
                "start_time" : 1748780624,
            },
        },
        {
            "event"   : "BUSINESS_PRIMARY_LOCATION_COUNTRY_UPDATE",
            "country" : "IN",
        },
        {
            "event"    : "DISABLED_UPDATE",
            "ban_info" : {
                "waba_ban_state" : "REINSTATE",
                "waba_ban_date"  : "April 17, 2025",
            },
        },
        {
            "event" : "MM_LITE_TERMS_SIGNED",
            "waba_info" : {
                "waba_id"           : "980198427658004",
                "owner_business_id" : "2329417887457253",
            },
        },
        {
            "event" : "PARTNER_CLIENT_CERTIFICATION_STATUS_UPDATE",
            "partner_client_certification_info" : {
                "client_business_id" : "2729063490586005",
                "status"             : "APPROVED",
                "rejection_reasons"  : [ "NONE" ],
            },
        },
        {
            "event" : "VOLUME_BASED_PRICING_TIER_UPDATE",
            "volume_tier_info" : {
                "tier_update_time" : 1743451903,
                "pricing_category" : "UTILITY",
                "tier"             : "25000001:50000000",
                "effective_month"  : "2025-11",
                "region"           : "India",
            },
        },
    ],
)
def test_account_updates_parse_documented_variants(
    value : dict[str, object],
) -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "entry" : [
            {
                "id"      : "123456789",
                "changes" : [
                    { "field" : "account_update", "value" : value },
                ],
            },
        ],
    })

    assert payload.entry[0].changes[0].value.event == value["event"]


def test_partner_update_parses_documented_multi_partner_fields() -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "entry" : [
            {
                "id"      : "123456789",
                "changes" : [
                    {
                        "field" : "account_update",
                        "value" : {
                            "event" : "PARTNER_ADDED",
                            "waba_info" : {
                                "waba_id"           : "980198427658004",
                                "owner_business_id" : "2329417887457253",
                                "solution_id"       : "1715120619246906",
                                "solution_partner_business_ids" : [
                                    "2949482758682047",
                                    "520744086200222",
                                ],
                            },
                        },
                    },
                ],
            },
        ],
    })

    value = payload.entry[0].changes[0].value
    assert value.waba_info
    assert value.waba_info.solution_id == "1715120619246906"
    assert value.waba_info.solution_partner_business_ids == (
        "2949482758682047",
        "520744086200222",
    )


def test_partner_removed_parses_documented_phone_number_variant() -> None :
    payload = WhatsApp_IB_Payload.model_validate({
        "entry" : [
            {
                "id"      : "123456789",
                "changes" : [
                    {
                        "field" : "account_update",
                        "value" : {
                            "event"        : "PARTNER_REMOVED",
                            "phone_number" : "15550783881",
                            "disconnection_info" : {
                                "reason"       : "PRIMARY_INACTIVITY",
                                "initiated_by" : "SYSTEM",
                            },
                        },
                    },
                ],
            },
        ],
    })

    value = payload.entry[0].changes[0].value
    assert value.waba_info is None
    assert value.phone_number == "15550783881"
    assert value.disconnection_info
    assert value.disconnection_info.reason == "PRIMARY_INACTIVITY"


def test_account_update_rejects_unknown_variant() -> None :
    with pytest.raises(ValidationError) :
        WhatsApp_IB_Payload.model_validate({
            "entry" : [
                {
                    "id"      : "123456789",
                    "changes" : [
                        {
                            "field" : "account_update",
                            "value" : { "event" : "UNKNOWN_EVENT" },
                        },
                    ],
                },
            ],
        })


def test_documented_account_update_requires_event_details() -> None :
    with pytest.raises(ValidationError) :
        WhatsApp_IB_Payload.model_validate({
            "entry" : [
                {
                    "id"      : "123456789",
                    "changes" : [
                        {
                            "field" : "account_update",
                            "value" : { "event" : "ACCOUNT_VIOLATION" },
                        },
                    ],
                },
            ],
        })


def test_revoke_message_parses_original_message_id() -> None :
    message = WhatsApp_IB_Message.model_validate({
        "from"      : "593995341161",
        "id"        : "wamid.revoke",
        "timestamp" : "1788724265",
        "type"      : "revoke",
        "revoke"    : { "original_message_id" : "wamid.original" },
    })

    assert message.revoke
    assert message.revoke.original_message_id == "wamid.original"


def test_history_chunk_rejects_invalid_progress() -> None :
    with pytest.raises(ValidationError) :
        WhatsApp_IB_Payload.model_validate({
            "entry" : [
                {
                    "id"      : "123456789",
                    "changes" : [
                        {
                            "field" : "history",
                            "value" : {
                                "metadata" : {
                                    "display_phone_number" : "593962939146",
                                    "phone_number_id"      : "987654321",
                                },
                                "history" : [
                                    {
                                        "metadata" : {
                                            "phase"       : 0,
                                            "chunk_order" : 0,
                                            "progress"    : 101,
                                        },
                                        "threads" : [{ "id" : "593995341161" }],
                                    },
                                ],
                            },
                        },
                    ],
                },
            ],
        })


def test_synthetic_identity_builds_outbound_message() -> None :
    message = WhatsApp_OB_TextMessage(
        to   = "IECED_Request_ciphertext",
        text = WhatsAppText( body = "Hello" ),
    )

    assert message.to == "IECED_Request_ciphertext"


@pytest.mark.parametrize( "identity", [ "", "contains whitespace" ] )
def test_identity_values_must_be_nonempty_without_whitespace(identity : str) -> None :
    with pytest.raises( ValidationError ) :
        WhatsApp_OB_TextMessage(
            to   = identity,
            text = WhatsAppText( body = "Hello" ),
        )


def test_contact_still_requires_an_identity() -> None :
    with pytest.raises( ValidationError ) :
        WhatsAppDatabaseRecord_Contact( row_id = 35)


def test_inbound_document_message() -> None :
    
    message = WhatsApp_IB_Message.model_validate({
        "from"     : "593995341161",
        "id"       : "wamid.document",
        "timestamp" : "1788724265",
        "type"     : "document",
        "document" : DOCUMENT_DATA,
    })
    
    assert message.media_data == message.document
    assert message.document
    assert message.document.filename == "invoice.pdf"
    assert message.document.extension == "pdf"


def test_inbound_unsupported_message() -> None :
    
    message = WhatsApp_IB_Message.model_validate({
        "from"         : "593995168398",
        "from_user_id" : "EC.1147177764911282",
        "id"           : "wamid.dW5zdXBwb3J0ZWQ=",
        "timestamp"    : "1791390490",
        "errors"       : [
            {
                "code"       : 131051,
                "title"      : "Message type unknown",
                "message"    : "Message type unknown",
                "error_data" : {
                    "details" : "Message type is currently not supported.",
                },
            }
        ],
        "type"        : "unsupported",
        "unsupported" : {
            "type"     : "unknown",
            "raw_type" : "unknown",
        },
    })
    
    assert message.unsupported
    assert message.unsupported.type == "unknown"
    assert message.unsupported.raw_type == "unknown"
    assert message.errors
    assert message.errors[0].code == 131051
    assert message.errors[0].error_data
    assert (
        message.errors[0].error_data.details ==
        "Message type is currently not supported."
    )


def test_document_message_echo() -> None :
    
    message = WhatsApp_IB_MessageEcho.model_validate({
        "from"       : "593964204854",
        "to_user_id" : "EC.1286082552926188",
        "id"         : "wamid.documentecho",
        "timestamp"  : "1788724265",
        "type"       : "document",
        "document"   : DOCUMENT_DATA,
    })
    
    assert message.media_data == message.document
    assert message.to is None
    assert message.to_user_id == "EC.1286082552926188"


def test_outbound_text_message_serializes_without_recipient() -> None :
    
    message = WhatsApp_OB_TextMessage(
        to   = "593995341161",
        text = WhatsAppText( body = "Hello" ),
    )
    
    assert message.model_dump() == {
        "messaging_product" : "whatsapp",
        "recipient_type"    : "individual",
        "to"                : "593995341161",
        "type"              : "text",
        "text"              : { "body" : "Hello" },
    }


def test_outbound_buttons_omit_option_descriptions() -> None :
    
    message = WhatsApp_OB_InteractiveOptionsMessage(
        to = "593995341161",
        interactive = WhatsApp_OB_InteractiveOptionsData(
            type = "button",
            body = WhatsApp_OB_InteractiveOptionsBodyObject( text = "Pick one" ),
            action = WhatsApp_OB_InteractiveOptionsButtons(
                buttons = [
                    WhatsApp_OB_InteractiveOptionsButtonEntry(
                        reply = WhatsAppInteractiveOption(
                            id          = "one",
                            title       = "One",
                            description = "Not sent for reply buttons",
                        )
                    )
                ]
            ),
        ),
    )
    
    assert message.model_dump()["interactive"] == {
        "type"   : "button",
        "body"   : { "text" : "Pick one" },
        "action" : {
            "buttons" : [
                {
                    "type"  : "reply",
                    "reply" : { "id" : "one", "title" : "One" },
                }
            ]
        },
    }


def test_outbound_interactive_type_matches_action() -> None :
    
    with pytest.raises( ValidationError ) :
        WhatsApp_OB_InteractiveOptionsData(
            type = "button",
            body = WhatsApp_OB_InteractiveOptionsBodyObject( text = "Pick one" ),
            action = WhatsApp_OB_InteractiveOptionsList(
                button   = "Choose",
                sections = [
                    WhatsApp_OB_InteractiveOptionsListEntries(
                        rows = [ WhatsAppInteractiveOption( id = "one", title = "One" ) ]
                    )
                ],
            ),
        )


def test_outbound_template_serializes_positional_parameters() -> None :
    
    message = WhatsApp_OB_TemplateMessage(
        to = "593995341161",
        template = WhatsApp_OB_TemplateData(
            name     = "order_confirmed",
            language = WhatsApp_OB_TemplateLanguageObject( code = "en_US" ),
            components = [
                WhatsApp_OB_TemplateBodyComponent(
                    parameters = [
                        WhatsApp_OB_TemplateTextParameter( text = "Luis" ),
                        WhatsApp_OB_TemplateTextParameter( text = "ORD-1234" ),
                    ]
                )
            ],
        ),
    )
    
    assert message.model_dump() == {
        "messaging_product" : "whatsapp",
        "recipient_type"    : "individual",
        "to"                : "593995341161",
        "type"              : "template",
        "template"          : {
            "name"       : "order_confirmed",
            "language"   : { "code" : "en_US" },
            "components" : [
                {
                    "type"       : "body",
                    "parameters" : [
                        { "type" : "text", "text" : "Luis" },
                        { "type" : "text", "text" : "ORD-1234" },
                    ],
                }
            ],
        },
    }


def test_outbound_template_serializes_named_parameters() -> None :
    
    message = WhatsApp_OB_TemplateMessage(
        to = "593995341161",
        template = WhatsApp_OB_TemplateData(
            name     = "order_confirmed",
            language = WhatsApp_OB_TemplateLanguageObject( code = "en_US" ),
            components = [
                WhatsApp_OB_TemplateBodyComponent(
                    parameters = [
                        WhatsApp_OB_TemplateTextParameter(
                            parameter_name = "name",
                            text           = "Luis",
                        )
                    ]
                )
            ],
        ),
    )
    
    parameters = message.model_dump()["template"]["components"][0]["parameters"]
    assert parameters == [
        { "type" : "text", "parameter_name" : "name", "text" : "Luis" }
    ]


def test_outbound_template_omits_absent_components() -> None :
    
    message = WhatsApp_OB_TemplateMessage(
        to = "593995341161",
        template = WhatsApp_OB_TemplateData(
            name     = "hello_world",
            language = WhatsApp_OB_TemplateLanguageObject( code = "en_US" ),
        ),
    )
    
    assert message.model_dump()["template"] == {
        "name"     : "hello_world",
        "language" : { "code" : "en_US" },
    }


def test_outbound_template_rejects_mixed_parameter_modes() -> None :
    
    with pytest.raises( ValidationError ) :
        WhatsApp_OB_TemplateBodyComponent(
            parameters = [
                WhatsApp_OB_TemplateTextParameter(
                    parameter_name = "name",
                    text           = "Luis",
                ),
                WhatsApp_OB_TemplateTextParameter( text = "ORD-1234" ),
            ]
        )
