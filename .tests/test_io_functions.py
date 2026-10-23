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
    WHATSAPP_MEDIA_TIMEOUT,
    async_send_whatsapp_media,
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
            f"{io_functions.META_GRAPH_API_URL}/{media_data.id}",
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
        [
            _response(
                401,
                f"{io_functions.META_GRAPH_API_URL}/{media_data.id}",
            )
        ]
        if failure_phase == "lookup" else
        [
            _response(
                200,
                f"{io_functions.META_GRAPH_API_URL}/{media_data.id}",
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
            f"{io_functions.META_GRAPH_API_URL}/{media_data.id}",
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
            f"{io_functions.META_GRAPH_API_URL}/{media_data.id}",
            json = { "url" : MEDIA_URL },
        ),
        _response( 200, MEDIA_URL, content = b"tampered"),
    ]

    with pytest.raises( ValueError, match = "checksum mismatch") :
        _run_fetch( monkeypatch, async_mode, media_data, responses)


def test_async_send_text_uses_message_helper(monkeypatch) -> None :
    monkeypatch.setenv( "WA_TOKEN", "test-token")
    requests = []

    class AsyncClient :

        async def __aenter__(self) :
            return self

        async def __aexit__( self, *_args) -> None :
            return

        async def post( self, **kwargs) -> httpx.Response :
            requests.append(kwargs)
            return httpx.Response(
                200,
                request = httpx.Request( "POST", kwargs["url"]),
                json    = { "messages" : [{ "id" : "wamid.text" }] },
            )

    monkeypatch.setattr( io_functions.httpx, "AsyncClient", AsyncClient)

    results = asyncio.run(io_functions.async_send_whatsapp_text(
        operator_id = "123456789",
        to_number   = TO_NUMBER,
        text        = "Hello",
    ))

    assert results == [
        {
            "msg_id"   : "wamid.text",
            "msg_type" : "text",
            "msg_data" : { "text" : { "body" : "Hello" } },
        }
    ]
    assert requests[0]["url"] == (
        f"{io_functions.META_GRAPH_API_URL}/123456789/messages"
    )


def test_async_embedded_signup_graph_helpers(monkeypatch) -> None :
    responses = iter([
        _response(
            200,
            f"{io_functions.META_GRAPH_API_URL}/oauth/access_token",
            json = { "access_token" : "business-token" },
        ),
        _response(
            200,
            f"{io_functions.META_GRAPH_API_URL}/waba-id/subscribed_apps",
            json = { "success" : True },
        ),
        _response(
            200,
            f"{io_functions.META_GRAPH_API_URL}/waba-id/phone_numbers",
            json = { "data" : [{ "id" : "phone-id" }] },
        ),
    ])
    requests = []

    class AsyncClient :

        def __init__( self, *, timeout) -> None :
            assert timeout == io_functions.META_GRAPH_API_REQUEST_TIMEOUT

        async def __aenter__(self) :
            return self

        async def __aexit__( self, *_args) -> None :
            return

        async def request( self, **kwargs) -> httpx.Response :
            requests.append(kwargs)
            return next(responses)

    async def run_helpers() -> tuple[str, bool, list[dict]] :
        token = await io_functions.async_exchange_code_for_business_token(
            code         = "signup-code",
            app_id       = "app-id",
            app_secret   = "app-secret",
            redirect_uri = "https://example.test/callback",
        )
        subscribed = await io_functions.async_subscribe_to_waba(
            "waba-id",
            token,
        )
        phone_numbers = await io_functions.async_list_phone_numbers_for_waba(
            "waba-id",
            token,
        )
        return token, subscribed, phone_numbers

    monkeypatch.setattr( io_functions.httpx, "AsyncClient", AsyncClient)

    assert asyncio.run(run_helpers()) == (
        "business-token",
        True,
        [{ "id" : "phone-id" }],
    )
    assert requests[0] == {
        "method"  : "GET",
        "url"     : f"{io_functions.META_GRAPH_API_URL}/oauth/access_token",
        "headers" : { },
        "params"  : {
            "client_id"     : "app-id",
            "client_secret" : "app-secret",
            "code"          : "signup-code",
            "redirect_uri"  : "https://example.test/callback",
        },
    }
    assert requests[1]["headers"] == {
        "Authorization" : "Bearer business-token"
    }
    assert requests[2]["params"] == {
        "fields" : ",".join(io_functions.WABA_PHONE_NUMBER_FIELDS)
    }


def test_graph_error_preserves_meta_message_and_status(monkeypatch) -> None :
    response = _response(
        401,
        f"{io_functions.META_GRAPH_API_URL}/waba-id/subscribed_apps",
        json = { "error" : { "message" : "Invalid OAuth access token" } },
    )
    monkeypatch.setattr(
        io_functions.httpx,
        "request",
        lambda **_kwargs: response,
    )

    with pytest.raises(
        io_functions.MetaGraphError,
        match = "Invalid OAuth access token",
    ) as error :
        io_functions.subscribe_to_waba( "waba-id", "business-token")

    assert error.value.status_code == 401


def test_subscribe_to_waba_requires_success_confirmation(monkeypatch) -> None :
    response = _response(
        200,
        f"{io_functions.META_GRAPH_API_URL}/waba-id/subscribed_apps",
        json = { "success" : False },
    )
    monkeypatch.setattr(
        io_functions.httpx,
        "request",
        lambda **_kwargs: response,
    )

    with pytest.raises(
        io_functions.MetaGraphError,
        match = "did not confirm",
    ) :
        io_functions.subscribe_to_waba( "waba-id", "business-token")


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
    
    caption = "Pago de contado por transferencia a:\nBanco Guayaquil"
    message = ServerDocumentMsg(
        filepath  = "report.pdf",
        content   = b"%PDF-\xff",
        caption   = caption,
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
            "caption"  : caption,
            "filename" : "report.pdf",
        },
    }


def test_async_send_media_uses_extended_timeout(monkeypatch) -> None :
    monkeypatch.setenv( "WA_TOKEN", "test-token")
    responses = iter([
        httpx.Response(
            200,
            request = httpx.Request( "POST", "https://example.test/media"),
            json    = { "id" : "987654321" },
        ),
        httpx.Response(
            200,
            request = httpx.Request( "POST", "https://example.test/messages"),
            json    = { "messages" : [{ "id" : "wamid.sent" }] },
        ),
    ])

    class AsyncClient :

        def __init__( self, *, timeout) -> None :
            assert timeout is WHATSAPP_MEDIA_TIMEOUT

        async def __aenter__(self) :
            return self

        async def __aexit__( self, *_args) -> None :
            return

        async def post( self, *_args, **_kwargs) -> httpx.Response :
            return next(responses)

    monkeypatch.setattr( io_functions.httpx, "AsyncClient", AsyncClient)
    media = ServerDocumentMsg(
        filepath = "Cotizacion.pdf",
        content  = b"%PDF",
        caption  = "Line one\nLine two",
    )

    assert asyncio.run(async_send_whatsapp_media(
        operator_id = "123456789",
        to_number   = TO_NUMBER,
        media       = media,
    )) is True
    assert media.upload_id == "987654321"


def test_async_send_media_rejects_failed_message_response(
    monkeypatch,
) -> None :
    monkeypatch.setenv( "WA_TOKEN", "test-token")
    responses = iter([
        httpx.Response(
            200,
            request = httpx.Request( "POST", "https://example.test/media"),
            json    = { "id" : "987654321" },
        ),
        httpx.Response(
            400,
            request = httpx.Request( "POST", "https://example.test/messages"),
            json    = { "error" : { "message" : "Invalid caption" } },
        ),
    ])

    class AsyncClient :

        def __init__( self, *, timeout) -> None :
            assert timeout is WHATSAPP_MEDIA_TIMEOUT

        async def __aenter__(self) :
            return self

        async def __aexit__( self, *_args) -> None :
            return

        async def post( self, *_args, **_kwargs) -> httpx.Response :
            return next(responses)

    monkeypatch.setattr( io_functions.httpx, "AsyncClient", AsyncClient)
    media = ServerDocumentMsg(
        filepath = "Cotizacion.pdf",
        content  = b"%PDF",
    )

    assert asyncio.run(async_send_whatsapp_media(
        operator_id = "123456789",
        to_number   = TO_NUMBER,
        media       = media,
    )) is False


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
