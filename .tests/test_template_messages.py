from __future__ import annotations

import asyncio
import httpx
import json

import pytest

from itertools import combinations
from pydantic import ValidationError
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from wa_agents.case_handler_base import (
    AsyncWhatsAppCaseHandler,
    WhatsAppCaseHandler,
)
from wa_agents.case_handler_models import ServerTemplateMsg
from wa_agents import io_functions
from wa_agents.io_functions import write_payload
from wa_agents.io_models import (
    WhatsApp_OB_TemplateDocument,
    WhatsApp_OB_TemplateHeaderComponent,
    WhatsApp_OB_TemplateImage,
    WhatsApp_OB_TemplateLocation,
    WhatsApp_OB_TemplateVideo,
)
from wa_agents.supabase import _message_data, _message_from_row


HEADERS = [
    ( "document", { "id" : "123", "filename" : "invoice.pdf" } ),
    ( "image", { "link" : "https://example.com/photo.jpg" } ),
    ( "video", { "id" : "456" } ),
    ( "location", {
        "latitude" : "-2.17", "longitude" : "-79.92",
        "name" : "Pickup point", "address" : "Main street 123",
    } ),
]


class _TemplateHandler(WhatsAppCaseHandler) :
    
    def process_message( self, _message, _media_content = None ) -> bool :
        return False
    
    def run_while_in_action( self, max_tokens = None ) -> bool :
        return False


class _AsyncTemplateHandler(AsyncWhatsAppCaseHandler) :
    
    async def process_message( self, _message, _media_content = None ) -> bool :
        return False
    
    async def run_while_in_action( self, max_tokens = None ) -> bool :
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


@pytest.mark.parametrize( "header_type,header", HEADERS)
def test_case_handler_send_template_dispatches_helper(
    monkeypatch, header_type, header,
) -> None :
    
    sent = []
    
    def _send(
        operator_id : str,
        to_number   : str,
        message     : ServerTemplateMsg,
        debug       : bool,
        database_url : str,
    ) -> list :
        sent.append(( operator_id, to_number, message, debug ))
        assert database_url == "test-db"
        return []
    
    monkeypatch.setattr( "wa_agents.case_handler_base.send_whatsapp_template", _send )
    
    handler             = object.__new__(_TemplateHandler)
    handler.operator_id = "op-1"
    handler.user_id     = "user-1"
    handler.debug       = False
    handler.storage     = SimpleNamespace( database_url = "test-db")
    handler._persist_outbound_messages = Mock()
    
    message = ServerTemplateMsg(
        **{ header_type : header },
        id         = 1,
        name       = "hello_world",
        language   = "en_US",
        parameters = [ "Luis" ],
    )
    
    assert handler.send_template(message) is True
    assert sent == [ ( "op-1", "user-1", message, False ) ]
    handler._persist_outbound_messages.assert_called_once_with( 1, [])


@pytest.mark.parametrize( "header_type,header", HEADERS)
def test_async_case_handler_send_template_dispatches_helper(
    monkeypatch, header_type, header,
) -> None :
    
    sent = []
    
    async def _send(
        operator_id : str,
        to_number   : str,
        message     : ServerTemplateMsg,
        debug       : bool,
        database_url : str,
    ) -> list :
        sent.append(( operator_id, to_number, message, debug ))
        assert database_url == "test-db"
        return []
    
    monkeypatch.setattr(
        "wa_agents.case_handler_base.async_send_whatsapp_template",
        _send,
    )
    
    handler             = object.__new__(_AsyncTemplateHandler)
    handler.operator_id = "op-1"
    handler.user_id     = "user-1"
    handler.debug       = True
    handler.storage     = SimpleNamespace( database_url = "test-db")
    handler._persist_outbound_messages = AsyncMock()
    
    message = ServerTemplateMsg(
        **{ header_type : header },
        id         = 1,
        name       = "hello_world",
        language   = "en_US",
        parameters = [ "Luis" ],
    )
    
    assert asyncio.run( handler.send_template(message) ) is True
    assert sent == [ ( "op-1", "user-1", message, True ) ]
    handler._persist_outbound_messages.assert_awaited_once_with( 1, [])


@pytest.mark.parametrize( "header_type,header", HEADERS)
@pytest.mark.parametrize( "parameters", [ None, [ "Luis" ], { "customer" : "Luis" } ])
def test_template_header_survives_persistence_and_payload(
    header_type, header, parameters,
) -> None :
    
    message = ServerTemplateMsg(
        name = "approved_template", language = "es_EC", parameters = parameters,
        **{ header_type : header },
    )
    restored = _message_from_row({
        "id" : 1, "message_index" : 1, "ts" : message.ts,
        "basemodel" : "ServerTemplateMsg", "data" : _message_data(message).obj,
    })
    assert getattr( restored, header_type) == getattr( message, header_type)
    assert json.loads(restored.as_text())[header_type] == header
    components = write_payload( "123456789", restored)["template"]["components"]
    expected = { "type" : header_type, header_type : header }
    assert components[0] == { "type" : "header", "parameters" : [ expected ] }
    assert len(components) == ( 2 if parameters else 1 )
    assert WhatsApp_OB_TemplateHeaderComponent.model_validate(
        components[0],
    ).model_dump( mode = "json") == components[0]


@pytest.mark.parametrize( "first,second", list(combinations( HEADERS, 2)))
def test_template_rejects_multiple_headers( first, second) -> None :
    
    with pytest.raises( ValidationError, match = "at most one header") :
        ServerTemplateMsg(
            name = "approved_template", language = "en_US",
            **dict([ first, second ]),
        )


@pytest.mark.parametrize( "media_class", [
    WhatsApp_OB_TemplateDocument, WhatsApp_OB_TemplateImage, WhatsApp_OB_TemplateVideo,
])
@pytest.mark.parametrize( "reference", [
    { "id" : "123" }, { "link" : "https://example.com/media" },
])
def test_template_media_reference( media_class, reference) -> None :
    
    assert media_class(**reference).model_dump( mode = "json") == reference


@pytest.mark.parametrize( "media_class", [
    WhatsApp_OB_TemplateDocument, WhatsApp_OB_TemplateImage, WhatsApp_OB_TemplateVideo,
])
@pytest.mark.parametrize( "reference", [
    {}, { "id" : "123", "link" : "https://example.com/media" },
    { "id" : "not-a-media-id" }, { "link" : "file:///tmp/media" },
    { "id" : "123", "caption" : "Unsupported template caption" },
])
def test_template_rejects_invalid_media_reference( media_class, reference) -> None :
    
    with pytest.raises(ValidationError) :
        media_class(**reference)


@pytest.mark.parametrize( "field,value", [
    ( "latitude", -91 ), ( "latitude", 91 ),
    ( "longitude", -181 ), ( "longitude", 181 ),
    ( "latitude", float("nan") ), ( "longitude", float("inf") ),
    ( "name", "" ), ( "address", "" ),
])
def test_template_rejects_invalid_location( field, value) -> None :
    
    location = dict(HEADERS[-1][1])
    location[field] = value
    with pytest.raises(ValidationError) :
        WhatsApp_OB_TemplateLocation(**location)


@pytest.mark.parametrize( "header_type,header", HEADERS)
@pytest.mark.parametrize( "asynchronous", [ False, True ])
def test_template_send_preserves_header_in_request_and_audit(
    monkeypatch, header_type, header, asynchronous,
) -> None :
    
    sent = []
    message = ServerTemplateMsg(
        name = "approved_template", language = "en_US",
        **{ header_type : header },
    )
    def respond( request) :
        
        assert request.headers["Authorization"] == "Bearer test-business-token"
        assert request.url.path.endswith("/123/messages")
        sent.append(json.loads(request.content))
        return httpx.Response( 200, json = { "messages" : [ { "id" : "wamid.test" } ] })
    
    transport = httpx.MockTransport(respond)
    if asynchronous :
        resolver = AsyncMock( return_value = "test-business-token")
        monkeypatch.setattr(
            io_functions.AsyncSupabaseStorage, "get_business_token", resolver,
        )
        client_class = httpx.AsyncClient
        monkeypatch.setattr(
            httpx, "AsyncClient", lambda : client_class( transport = transport),
        )
        results = asyncio.run(io_functions.async_send_whatsapp_template(
            "123", "123456789", message, database_url = "test-db",
        ))
        resolver.assert_awaited_once_with( phone_number_id = "123")
    else :
        resolver = Mock( return_value = "test-business-token")
        monkeypatch.setattr(
            io_functions.SyncSupabaseStorage, "get_business_token", resolver,
        )
        with httpx.Client( transport = transport) as client :
            monkeypatch.setattr( httpx, "post", client.post)
            results = io_functions.send_whatsapp_template(
                "123", "123456789", message, database_url = "test-db",
            )
        resolver.assert_called_once_with( phone_number_id = "123")
    
    assert len(sent) == 1
    assert sent[0]["template"]["components"][0]["parameters"] == [
        { "type" : header_type, header_type : header },
    ]
    assert results == [{
        "msg_id" : "wamid.test", "msg_type" : "template",
        "msg_data" : { "template" : sent[0]["template"] },
    }]
