import asyncio
import base64
import hashlib

import httpx
import pytest

from wa_agents import io_functions
from wa_agents.case_handler_models import (
    ServerDocumentMsg,
    ServerMediaMsg,
    ServerInteractiveOptsMsg,
)
from wa_agents.io_functions import (
    async_fetch_media,
    fetch_media,
    write_payload,
)
from wa_agents.io_models import (
    WhatsAppContactCard,
    WhatsAppContactCard_Name,
    WhatsAppContactCard_Phone,
    WhatsAppInteractiveOption,
    WhatsAppLocation,
    WhatsApp_IB_MediaData,
)


TO_NUMBER = "593999000111"
MEDIA_URL = "https://media.example/file"


def _media_data( content : bytes) -> WhatsApp_IB_MediaData :
    return WhatsApp_IB_MediaData(
        id        = "123456789",
        mime_type = "image/jpeg",
        sha256    = base64.b64encode(
            hashlib.sha256(content).digest()
        ).decode(),
    )


def _response(
    status_code : int,
    url         : str,
    *,
    content : bytes = b"",
    json    : dict | None = None,
) -> httpx.Response :
    kwargs = { "content" : content }
    if json is not None :
        kwargs = { "json" : json }
    return httpx.Response(
        status_code,
        request = httpx.Request( "GET", url),
        **kwargs,
    )


def _run_fetch(
    monkeypatch,
    async_mode : bool,
    media_data : WhatsApp_IB_MediaData,
    responses  : list[httpx.Response],
) -> bytes :
    monkeypatch.setenv( "WA_TOKEN", "test-token")
    pending = iter(responses)

    if not async_mode :
        monkeypatch.setattr(
            io_functions.httpx,
            "get",
            lambda *_args, **_kwargs: next(pending),
        )
        return fetch_media(media_data)

    class AsyncClient :

        async def __aenter__(self) :
            return self

        async def __aexit__( self, *_args) -> None :
            return

        async def get( self, *_args, **_kwargs) -> httpx.Response :
            return next(pending)

    monkeypatch.setattr( io_functions.httpx, "AsyncClient", AsyncClient)
    return asyncio.run(async_fetch_media(media_data))


@pytest.mark.parametrize( "async_mode", [ False, True ] )
def test_fetch_media_verifies_downloaded_content(
    monkeypatch,
    async_mode : bool,
) -> None :
    content    = b"verified media"
    media_data = _media_data(content)
    responses  = [
        _response(
            200,
            f"{io_functions.API_URL}{media_data.id}",
            json = { "url" : MEDIA_URL },
        ),
        _response( 200, MEDIA_URL, content = content),
    ]

    assert _run_fetch( monkeypatch, async_mode, media_data, responses) == content


@pytest.mark.parametrize( "async_mode", [ False, True ] )
@pytest.mark.parametrize( "failure_phase", [ "lookup", "content" ] )
def test_fetch_media_raises_on_http_errors(
    monkeypatch,
    async_mode    : bool,
    failure_phase : str,
) -> None :
    media_data = _media_data(b"media")
    responses  = (
        [ _response( 401, f"{io_functions.API_URL}{media_data.id}") ]
        if failure_phase == "lookup" else
        [
            _response(
                200,
                f"{io_functions.API_URL}{media_data.id}",
                json = { "url" : MEDIA_URL },
            ),
            _response( 404, MEDIA_URL),
        ]
    )

    with pytest.raises(httpx.HTTPStatusError) :
        _run_fetch( monkeypatch, async_mode, media_data, responses)


@pytest.mark.parametrize( "async_mode", [ False, True ] )
def test_fetch_media_raises_when_graph_omits_url(
    monkeypatch,
    async_mode : bool,
) -> None :
    media_data = _media_data(b"media")
    responses  = [
        _response(
            200,
            f"{io_functions.API_URL}{media_data.id}",
            json = {},
        )
    ]

    with pytest.raises( RuntimeError, match = "did not contain a URL") :
        _run_fetch( monkeypatch, async_mode, media_data, responses)


@pytest.mark.parametrize( "async_mode", [ False, True ] )
def test_fetch_media_rejects_checksum_mismatch(
    monkeypatch,
    async_mode : bool,
) -> None :
    media_data = _media_data(b"expected")
    responses  = [
        _response(
            200,
            f"{io_functions.API_URL}{media_data.id}",
            json = { "url" : MEDIA_URL },
        ),
        _response( 200, MEDIA_URL, content = b"tampered"),
    ]

    with pytest.raises( ValueError, match = "checksum mismatch") :
        _run_fetch( monkeypatch, async_mode, media_data, responses)


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
    assert message.id is None
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
    
    contact = WhatsAppContactCard(
        name   = WhatsAppContactCard_Name(
            formatted_name = "Luis Reyes",
            first_name     = "Luis",
        ),
        phones = (
            WhatsAppContactCard_Phone(
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
