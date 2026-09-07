from wa_agents.case_handler_models import (
    ServerDocumentMsg,
    ServerMediaMsg,
    ServerInteractiveOptsMsg,
)
from wa_agents.whatsapp_functions import write_payload
from wa_agents.whatsapp_models import (
    WhatsAppContactPayload,
    WhatsAppContactPayload_Name,
    WhatsAppContactPayload_Phone,
    WhatsAppInteractiveOption,
    WhatsAppLocation,
)


TO_NUMBER = "593999000111"


def test_write_text_payload_uses_outbound_model() -> None :
    
    assert write_payload( TO_NUMBER, "Hello") == {
        "messaging_product" : "whatsapp",
        "recipient_type"    : "individual",
        "to"                : TO_NUMBER,
        "type"              : "text",
        "text"              : { "body" : "Hello" },
    }


def test_write_button_payload_uses_outbound_model() -> None :
    
    message = ServerInteractiveOptsMsg(
        type    = "button",
        header  = "Question",
        body    = "Pick one",
        footer  = "Required",
        options = [
            WhatsAppInteractiveOption( id = "one", title = "One"),
        ],
    )
    
    assert write_payload( TO_NUMBER, message) == {
        "messaging_product" : "whatsapp",
        "recipient_type"    : "individual",
        "to"                : TO_NUMBER,
        "type"              : "interactive",
        "interactive"       : {
            "type"   : "button",
            "header" : { "type" : "text", "text" : "Question" },
            "body"   : { "text" : "Pick one" },
            "footer" : { "text" : "Required" },
            "action" : {
                "buttons" : [
                    {
                        "type"  : "reply",
                        "reply" : { "id" : "one", "title" : "One" },
                    },
                ],
            },
        },
    }


def test_write_list_payload_uses_outbound_model() -> None :
    
    message = ServerInteractiveOptsMsg(
        type    = "list",
        body    = "Pick one",
        button  = "Choose",
        options = [
            WhatsAppInteractiveOption(
                id          = "one",
                title       = "One",
                description = "First choice",
            ),
        ],
    )
    
    assert write_payload( TO_NUMBER, message)["interactive"] == {
        "type"   : "list",
        "body"   : { "text" : "Pick one" },
        "action" : {
            "button"   : "Choose",
            "sections" : [
                {
                    "rows" : [
                        {
                            "id"          : "one",
                            "title"       : "One",
                            "description" : "First choice",
                        },
                    ],
                },
            ],
        },
    }


def test_write_image_payload_uses_outbound_model() -> None :
    
    message = ServerMediaMsg(
        mime      = "image/jpeg",
        filepath  = "image.jpg",
        content   = b"\xff\xd8\xff",
        caption   = "An image",
        upload_id = "123456789",
    )
    
    assert message.user_eyes is True
    serialized = message.model_dump( mode = "json")
    assert "content" not in serialized
    assert ServerMediaMsg.model_validate(serialized).content is None
    
    assert write_payload( TO_NUMBER, message) == {
        "messaging_product" : "whatsapp",
        "recipient_type"    : "individual",
        "to"                : TO_NUMBER,
        "type"              : "image",
        "image"             : {
            "id"      : "123456789",
            "caption" : "An image",
        },
    }


def test_write_document_payload_uses_outbound_model() -> None :
    
    message = ServerDocumentMsg(
        filepath  = "report.pdf",
        content   = b"%PDF-\xff",
        upload_id = "987654321",
    )
    
    assert message.basemodel == "ServerDocumentMsg"
    assert message.id
    serialized = message.model_dump( mode = "json")
    assert "content" not in serialized
    assert ServerDocumentMsg.model_validate(serialized).content is None
    
    assert write_payload( TO_NUMBER, message) == {
        "messaging_product" : "whatsapp",
        "recipient_type"    : "individual",
        "to"                : TO_NUMBER,
        "type"              : "document",
        "document"          : {
            "id"       : "987654321",
            "filename" : "report.pdf",
        },
    }


def test_write_contacts_payload_uses_outbound_model() -> None :
    
    contact = WhatsAppContactPayload(
        name   = WhatsAppContactPayload_Name(
            formatted_name = "Luis Reyes",
            first_name     = "Luis",
        ),
        phones = (
            WhatsAppContactPayload_Phone(
                phone = "+593999000111",
                type  = "CELL",
            ),
        ),
    )
    
    assert write_payload( TO_NUMBER, contact) == {
        "messaging_product" : "whatsapp",
        "recipient_type"    : "individual",
        "to"                : TO_NUMBER,
        "type"              : "contacts",
        "contacts"          : [
            {
                "name" : {
                    "formatted_name" : "Luis Reyes",
                    "first_name"     : "Luis",
                },
                "phones" : (
                    {
                        "phone" : "+593999000111",
                        "type"  : "CELL",
                    },
                ),
            },
        ],
    }


def test_write_location_payload_uses_outbound_model() -> None :
    
    location = WhatsAppLocation(
        latitude  = -0.180653,
        longitude = -78.467834,
        name      = "Quito",
    )
    
    assert write_payload( TO_NUMBER, location) == {
        "messaging_product" : "whatsapp",
        "recipient_type"    : "individual",
        "to"                : TO_NUMBER,
        "type"              : "location",
        "location"          : {
            "latitude"  : -0.180653,
            "longitude" : -78.467834,
            "name"      : "Quito",
        },
    }
