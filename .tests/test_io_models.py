import pytest

from pydantic import (
    TypeAdapter,
    ValidationError,
)
from sofia_utils.pydantic import NE_str

from wa_agents.io_models import (
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
                                                        "status" : "READ"
                                                    },
                                                },
                                                {
                                                    "from"      : "593962939146",
                                                    "to"        : "593995341161",
                                                    "id"        : "wamid.history-media",
                                                    "timestamp" : "1788724270",
                                                    "type"      : "media_placeholder",
                                                    "history_context" : {
                                                        "status" : "DELIVERED"
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
