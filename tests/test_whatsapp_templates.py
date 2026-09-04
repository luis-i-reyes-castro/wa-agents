from __future__ import annotations

import asyncio

import pytest

from pydantic import ValidationError

from wa_agents.case_handler_base import (
    AsyncCaseHandlerBase,
    CaseHandlerBase,
)
from wa_agents.case_handler_models import ServerTemplateMsg
from wa_agents.whatsapp_functions import write_payload


class _TemplateHandler(CaseHandlerBase) :
    
    def process_message( self, _message, _media_content = None ) -> bool :
        return False
    
    def generate_response( self, max_tokens = None ) -> bool :
        return False


class _AsyncTemplateHandler(AsyncCaseHandlerBase) :
    
    async def process_message( self, _message, _media_content = None ) -> bool :
        return False
    
    async def generate_response( self, max_tokens = None ) -> bool :
        return False


def _template_with_body(
    parameters : list[str] | dict[str, str],
) -> ServerTemplateMsg :
    
    return ServerTemplateMsg(
        name       = "order_confirmed",
        language   = "en_US",
        parameters = parameters,
    )


def test_template_message_allows_positional_parameters() -> None :
    
    message = _template_with_body([ "Luis", "ORD-1234" ])
    
    assert message.parameters == [
        "Luis",
        "ORD-1234",
    ]


def test_template_message_allows_named_parameters() -> None :
    
    message = _template_with_body({
        "nombre" : "Luis Reyes",
        "email"  : "admin@sofia-systems.com",
    })
    
    assert message.parameters == {
        "nombre" : "Luis Reyes",
        "email"  : "admin@sofia-systems.com",
    }


def test_template_message_rejects_empty_parameter_list() -> None :
    
    with pytest.raises( ValidationError ) :
        _template_with_body([])


def test_template_message_rejects_empty_parameter_dict() -> None :
    
    with pytest.raises( ValidationError ) :
        _template_with_body({})


def test_template_payload_includes_recipient_type() -> None :
    
    payload = write_payload(
        "593999000111",
        ServerTemplateMsg(
            name       = "hello_world",
            language   = "en_US",
            parameters = [ "Luis" ],
        ),
    )
    
    assert payload["recipient_type"] == "individual"
    assert payload["type"] == "template"
    assert payload["template"]["language"] == { "code" : "en_US" }


def test_template_payload_includes_components() -> None :
    
    payload = write_payload(
        "593999000111",
        ServerTemplateMsg(
            name       = "hello_world",
            language   = "en_US",
            parameters = [ "Luis" ],
        ),
    )
    
    assert payload["template"]["components"] == [
        {
            "type"       : "body",
            "parameters" : [
                { "type" : "text", "text" : "Luis" },
            ],
        }
    ]


def test_template_payload_serializes_positional_parameters() -> None :
    
    payload = write_payload(
        "593999000111",
        _template_with_body([ "Luis", "ORD-1234" ]),
    )
    
    assert payload["template"]["components"] == [
        {
            "type"       : "body",
            "parameters" : [
                { "type" : "text", "text" : "Luis" },
                { "type" : "text", "text" : "ORD-1234" },
            ],
        }
    ]


def test_template_payload_serializes_named_parameters() -> None :
    
    payload = write_payload(
        "593999000111",
        _template_with_body({
            "nombre" : "Luis Reyes",
            "email"  : "admin@sofia-systems.com",
        }),
    )
    
    assert payload["template"]["components"] == [
        {
            "type"       : "body",
            "parameters" : [
                {
                    "type"           : "text",
                    "parameter_name" : "nombre",
                    "text"           : "Luis Reyes",
                },
                {
                    "type"           : "text",
                    "parameter_name" : "email",
                    "text"           : "admin@sofia-systems.com",
                },
            ],
        }
    ]


def test_case_handler_send_template_dispatches_helper( monkeypatch ) -> None :
    
    sent = []
    
    def _send( operator_id : str, user_id : str, message : ServerTemplateMsg ) -> None :
        sent.append(( operator_id, user_id, message ))
    
    monkeypatch.setattr( "wa_agents.case_handler_base.send_whatsapp_template", _send )
    
    handler             = object.__new__(_TemplateHandler)
    handler.operator_id = "op-1"
    handler.user_id     = "user-1"
    handler.debug       = False
    
    message = ServerTemplateMsg(
        name       = "hello_world",
        language   = "en_US",
        parameters = [ "Luis" ],
    )
    
    assert handler.send_template(message) is True
    assert sent == [ ( "op-1", "user-1", message ) ]


def test_async_case_handler_send_template_dispatches_helper( monkeypatch ) -> None :
    
    sent = []
    
    async def _send(
        operator_id : str,
        user_id     : str,
        message     : ServerTemplateMsg,
    ) -> None :
        sent.append(( operator_id, user_id, message ))
    
    monkeypatch.setattr(
        "wa_agents.case_handler_base.async_send_whatsapp_template",
        _send,
    )
    
    handler             = object.__new__(_AsyncTemplateHandler)
    handler.operator_id = "op-1"
    handler.user_id     = "user-1"
    handler.debug       = True
    
    message = ServerTemplateMsg(
        name       = "hello_world",
        language   = "en_US",
        parameters = [ "Luis" ],
    )
    
    assert asyncio.run( handler.send_template(message) ) is True
    assert sent == [ ( "op-1", "user-1", message ) ]
