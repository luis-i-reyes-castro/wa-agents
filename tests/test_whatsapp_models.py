import pytest

from pydantic import ValidationError

from wa_agents.whatsapp_models import (
    WhatsAppInteractiveOption,
    WhatsApp_OB_InteractiveOptionsBodyObject,
    WhatsApp_OB_InteractiveOptionsButtonEntry,
    WhatsApp_OB_InteractiveOptionsButtons,
    WhatsApp_OB_InteractiveOptionsData,
    WhatsApp_OB_InteractiveOptionsMessage,
    WhatsApp_OB_InteractiveOptionsList,
    WhatsApp_OB_InteractiveOptionsListEntries,
    WhatsApp_OB_TextMessage,
    WhatsApp_OB_TemplateBodyComponent,
    WhatsApp_OB_TemplateData,
    WhatsApp_OB_TemplateLanguageObject,
    WhatsApp_OB_TemplateMessage,
    WhatsApp_OB_TemplateTextParameter,
    WhatsAppText,
)


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
