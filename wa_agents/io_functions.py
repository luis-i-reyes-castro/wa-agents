#!/usr/bin/env python3

import base64
import hashlib
import hmac
import httpx
import json
import os
import re

from copy import deepcopy
from typing import (
    Any,
    TypedDict,
)

from sofia_utils.io import JSON_INDENT
from sofia_utils.printing import (
    get_qualname as here,
    print_sep,
)

from .case_handler_models import (
    ServerDocumentMsg,
    ServerFlowMsg,
    ServerInteractiveOptsMsg,
    ServerMediaMsg,
    ServerTemplateMsg,
    ServerTextMsg,
)
from .io_models import (
    WhatsAppContactCard,
    WhatsAppAppDataSyncType,
    WhatsAppLocation,
    WhatsAppText,
    WhatsApp_IB_MediaData,
    WhatsApp_OB_ContactsMessage,
    WhatsApp_OB_InteractiveOptionsBodyObject,
    WhatsApp_OB_InteractiveOptionsButtonEntry,
    WhatsApp_OB_InteractiveOptionsButtons,
    WhatsApp_OB_InteractiveOptionsData,
    WhatsApp_OB_InteractiveOptionsFooterObject,
    WhatsApp_OB_InteractiveOptionsHeaderObject,
    WhatsApp_OB_InteractiveOptionsList,
    WhatsApp_OB_InteractiveOptionsListEntries,
    WhatsApp_OB_InteractiveOptionsMessage,
    WhatsApp_OB_LocationMessage,
    WhatsApp_OB_MediaData,
    WhatsApp_OB_MediaMessage,
    WhatsApp_OB_TemplateBodyComponent,
    WhatsApp_OB_TemplateData,
    WhatsApp_OB_TemplateLanguageObject,
    WhatsApp_OB_TemplateMessage,
    WhatsApp_OB_TemplateTextParameter,
    WhatsApp_OB_TextMessage,
)


# =========================================================================================
# CONSTANTS

META_GRAPH_API_URL             = "https://graph.facebook.com/v26.0"
META_GRAPH_API_REQUEST_TIMEOUT = 30.0

WABA_PHONE_NUMBER_FIELDS = (
    "id",
    "display_phone_number",
    "verified_name",
    "quality_rating",
    "code_verification_status",
    "name_status",
    "platform_type",
    "is_on_biz_app",
)
WHATSAPP_MEDIA_TIMEOUT = httpx.Timeout( 30.0, connect = 10.0)


# =========================================================================================
# CLASSES

class MetaGraphError (RuntimeError) :
    """
    Error returned by the Meta Graph API
    """
    
    def __init__(
        self,
        message     : str,
        status_code : int | None = None,
    ) -> None :
        
        super().__init__(message)
        self.status_code = status_code
        
        return


class WhatsAppSendResult (TypedDict) :
    """
    One accepted outbound message and the data required to persist it.
    """
    msg_id   : str
    msg_type : str
    msg_data : dict[ str, Any]


# =========================================================================================
# INFRASTRUCTURE

def collect_send_results(
    payload  : dict[str, Any],
    response : httpx.Response,
    debug    : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Validate a Graph send response and pair its message IDs with the request data.
    """
    response_data = response.json()
    
    if debug :
        print_sep()
        print("[INFO] WhatsApp API response to sent message:")
        print(json.dumps( response_data, indent = JSON_INDENT))
    
    response.raise_for_status()
    
    msg_type = str(payload["type"])
    msg_body = deepcopy(payload[msg_type])
    if (
        ( msg_type             == "interactive" ) and
        ( msg_body.get("type") == "flow"        )
    ) :
        parameters = msg_body.get( "action", {}).get( "parameters", {})
        if "flow_token" in parameters :
            parameters["flow_token"] = "[REDACTED]"
    
    results  = [
        WhatsAppSendResult(
            msg_id   = str(message["id"]),
            msg_type = msg_type,
            msg_data = { msg_type : msg_body },
        )
        for message in response_data.get( "messages", [])
        if message.get("id")
    ]
    if not results :
        raise RuntimeError("Graph send response did not contain a message ID")
    
    return results


def graph_request(
    method       : str,
    path         : str,
    access_token : str | None = None,
    params       : dict[str, str] | None = None,
    json_data    : dict[str, Any] | None = None,
) -> dict[str, Any] :
    """
    Make one synchronous request to the configured Meta Graph API version
    """
    
    headers = (
        write_headers(
            content_type   = False,
            business_token = access_token,
        )
        if access_token else
        { }
    )
    request_data : dict[str, Any] = {
        "method"  : method,
        "url"     : f"{META_GRAPH_API_URL}/{path.lstrip('/')}",
        "headers" : headers,
        "params"  : params,
        "timeout" : META_GRAPH_API_REQUEST_TIMEOUT,
    }
    if json_data is not None :
        request_data["json"] = json_data
    
    response = httpx.request(**request_data)
    
    return parse_graph_response(response)


async def async_graph_request(
    method       : str,
    path         : str,
    access_token : str | None = None,
    params       : dict[str, str] | None = None,
    json_data    : dict[str, Any] | None = None,
) -> dict[str, Any] :
    """
    Make one asynchronous request to the configured Meta Graph API version
    """
    
    headers = (
        write_headers(
            content_type   = False,
            business_token = access_token,
        )
        if access_token else
        { }
    )
    async with httpx.AsyncClient(
        timeout = META_GRAPH_API_REQUEST_TIMEOUT
    ) as client :
        request_data : dict[str, Any] = {
            "method"  : method,
            "url"     : f"{META_GRAPH_API_URL}/{path.lstrip('/')}",
            "headers" : headers,
            "params"  : params,
        }
        if json_data is not None :
            request_data["json"] = json_data
        
        response = await client.request(**request_data)
    
    return parse_graph_response(response)


def parse_graph_response( response : httpx.Response) -> dict[str, Any] :
    """
    Parse one Graph response and raise an error containing Meta's message on failure
    """
    
    try :
        payload = response.json()
    except ValueError :
        payload = {
            "error" : {
                "message" :
                    response.text.strip() or "Meta returned an invalid response"
            }
        }
    
    if response.is_success and isinstance( payload, dict) :
        return payload
    
    error   = payload.get("error") if isinstance( payload, dict) else None
    message = None
    if isinstance( error, dict) :
        message = str( error.get("message") or "" ).strip() or None
    
    raise MetaGraphError(
        message     = message or f"Meta request failed with status {response.status_code}",
        status_code = response.status_code,
    )


def write_headers(
    content_type : bool = True,
    *,
    business_token : str | None = None,
) -> dict :
    """
    Compose Graph API headers with auth and optional JSON content-type \\
    Args:
        content_type : When True, include `Content-Type: application/json`
    Returns:
        Headers dictionary for Graph API HTTP calls
    """
    
    if not ( WA_TOKEN := business_token or os.getenv("WA_TOKEN") ) :
        raise RuntimeError("Environment variable 'WA_TOKEN' was not found")
    
    headers = { "Authorization": f"Bearer {WA_TOKEN}" }
    if content_type :
        headers["Content-Type"] = "application/json"
    
    return headers


# =========================================================================================
# INBOUND MEDIA

def fetch_media( media_data : WhatsApp_IB_MediaData) -> bytes :
    """
    Download WhatsApp media content using its media id \\
    Args:
        media_data : WhatsApp media metadata payload
    Returns:
        Raw media bytes
    """
    req_url  = f"{META_GRAPH_API_URL}/{media_data.id}"
    req_head = write_headers( content_type = False)
    response = httpx.get( req_url, headers = req_head)
    response.raise_for_status()

    response_data = response.json()
    media_url     = (
        response_data.get("url")
        if isinstance( response_data, dict) else None
    )
    if not media_url :
        raise RuntimeError(
            f"In {here()}: Graph media response did not contain a URL"
        )

    response = httpx.get( media_url, headers = req_head)
    response.raise_for_status()

    return verify_media_content( media_data, response.content)

async def async_fetch_media( media_data : WhatsApp_IB_MediaData) -> bytes :
    """
    Download WhatsApp media content using its media id \\
    Args:
        media_data : WhatsApp media metadata payload
    Returns:
        Raw media bytes
    """
    req_url  = f"{META_GRAPH_API_URL}/{media_data.id}"
    req_head = write_headers( content_type = False)
    
    async with httpx.AsyncClient() as client :
        response = await client.get( req_url, headers = req_head)
        response.raise_for_status()
        
        response_data = response.json()
        media_url     = (
            response_data.get("url")
            if isinstance( response_data, dict) else None
        )
        if not media_url :
            raise RuntimeError(
                f"In {here()}: Graph media response did not contain a URL"
            )
        
        response = await client.get( media_url, headers = req_head)
        response.raise_for_status()
    
    return verify_media_content( media_data, response.content)

def verify_app_secret(
    payload    : bytes | None,
    signature  : str   | None,
    app_secret : str   | None = None,
) -> bool :
    
    if not ( signature and signature.startswith("sha256=") ) :
        return False
    
    app_secret = app_secret or os.getenv("WA_APP_SECRET")
    if not app_secret :
        raise RuntimeError("Environment variable 'WA_APP_SECRET' was not found")
    
    expected = hmac.new(
        key       = app_secret.encode("utf-8"),
        msg       = payload,
        digestmod = hashlib.sha256,
    ).hexdigest()
    
    return hmac.compare_digest( signature, f"sha256={expected}")


def verify_media_content(
    media_data : WhatsApp_IB_MediaData,
    content    : bytes,
) -> bytes :
    """
    Verify downloaded media against Meta's declared SHA-256 digest
    """
    expected = base64.b64decode( media_data.sha256, validate = True)
    actual   = hashlib.sha256(content).digest()
    
    if not hmac.compare_digest( actual, expected) :
        raise ValueError(f"In {here()}: WhatsApp media checksum mismatch")
    
    return content


# =========================================================================================
# OUTBOUND: Payloads

def write_payload(
    to_number : str,
    content   : str
              | ServerTextMsg
              | ServerInteractiveOptsMsg
              | ServerFlowMsg
              | ServerTemplateMsg
              | ServerMediaMsg
              | WhatsAppContactCard
              | WhatsAppLocation,
) -> dict[ str, Any] :
    """
    Serialize outgoing text/interactive/media messages \\
    Args:
        to_number : WhatsApp recipient phone number
        content   : Text, interactive options, or media payload
    Returns:
        Dictionary ready to send to the Graph API.
    """
    
    if isinstance( content, ( str, ServerTextMsg)) :
        
        # Reference: https://developers.facebook.com/docs/whatsapp/cloud-api/messages/text-messages
        
        return WhatsApp_OB_TextMessage(
            to   = to_number,
            text = WhatsAppText(
                body = content if isinstance( content, str) else str(content.text)
            ),
        ).model_dump()
    
    elif isinstance( content, ServerInteractiveOptsMsg) :
        
        # References:
        # Interactive Reply Buttons: https://developers.facebook.com/docs/whatsapp/cloud-api/messages/interactive-reply-buttons-messages
        # Interactive Lists: https://developers.facebook.com/docs/whatsapp/cloud-api/messages/interactive-list-messages/
        
        match content.type :
            case "button" :
                action = WhatsApp_OB_InteractiveOptionsButtons(
                    buttons = [
                        WhatsApp_OB_InteractiveOptionsButtonEntry( reply = option)
                        for option in content.options
                    ],
                )
            case "list" :
                action = WhatsApp_OB_InteractiveOptionsList(
                    button   = str(content.button),
                    sections = [
                        WhatsApp_OB_InteractiveOptionsListEntries(
                            rows = content.options,
                        ),
                    ],
                )
        
        return WhatsApp_OB_InteractiveOptionsMessage(
            to          = to_number,
            interactive = WhatsApp_OB_InteractiveOptionsData(
                type   = content.type,
                header = (
                    WhatsApp_OB_InteractiveOptionsHeaderObject(
                        text = content.header,
                    )
                    if content.header else None
                ),
                body   = WhatsApp_OB_InteractiveOptionsBodyObject(
                    text = content.body,
                ),
                footer = (
                    WhatsApp_OB_InteractiveOptionsFooterObject(
                        text = content.footer,
                    )
                    if content.footer else None
                ),
                action = action,
            ),
        ).model_dump()
    
    elif isinstance( content, ServerFlowMsg) :
        
        action_parameters : dict[str, Any] = {
            "flow_message_version" : "3",
            "flow_id"              : content.flow_id,
            "flow_token"           : content.flow_token,
            "flow_cta"             : content.flow_cta,
            "mode"                 : content.flow_mode,
            "flow_action"          : content.flow_action,
        }
        if content.screen or content.data :
            action_parameters["flow_action_payload"] = {
                **( { "screen" : content.screen } if content.screen else {} ),
                **( { "data"   : content.data   } if content.data   else {} ),
            }
        
        interactive : dict[str, Any] = {
            "type"   : "flow",
            "body"   : { "text" : content.body },
            "action" : {
                "name"       : "flow",
                "parameters" : action_parameters,
            },
        }
        if content.header :
            interactive["header"] = {
                "type" : "text",
                "text" : content.header,
            }
        if content.footer :
            interactive["footer"] = { "text" : content.footer }
        
        return {
            "messaging_product" : "whatsapp",
            "recipient_type"    : "individual",
            "to"                : to_number,
            "type"              : "interactive",
            "interactive"       : interactive,
        }
    
    elif isinstance( content, ServerTemplateMsg) :
        
        # Reference: https://developers.facebook.com/documentation/business-messaging/whatsapp/templates/overview
        
        if isinstance( content.parameters, list) :
            parameters = [
                WhatsApp_OB_TemplateTextParameter( text = param)
                for param in content.parameters
            ]
        else :
            parameters = [
                WhatsApp_OB_TemplateTextParameter(
                    parameter_name = param_name,
                    text           = param_val,
                )
                for param_name, param_val in content.parameters.items()
            ]
        
        return WhatsApp_OB_TemplateMessage(
            to       = to_number,
            template = WhatsApp_OB_TemplateData(
                name       = content.name,
                language   = WhatsApp_OB_TemplateLanguageObject(
                    code = content.language,
                ),
                components = [
                    WhatsApp_OB_TemplateBodyComponent(
                        parameters = parameters,
                    ),
                ],
            ),
        ).model_dump()
    
    elif isinstance( content, ServerMediaMsg) :
        
        # Reference: https://developers.facebook.com/docs/whatsapp/cloud-api/messages/image-messages
        
        media_data = WhatsApp_OB_MediaData(
            id       = content.upload_id,
            caption  = content.caption,
            filename = (
                content.filename
                if isinstance( content, ServerDocumentMsg) else None
            ),
        )
        return WhatsApp_OB_MediaMessage(
            to   = to_number,
            type = content.type,
            **{ content.type : media_data },
        ).model_dump()
    
    elif isinstance( content, WhatsAppContactCard) :
        
        # Reference: https://developers.facebook.com/documentation/business-messaging/whatsapp/messages/contacts-messages
        
        return WhatsApp_OB_ContactsMessage(
            to       = to_number,
            contacts = [ content ],
        ).model_dump( exclude_none = True)
    
    elif isinstance( content, WhatsAppLocation) :
        
        # Reference: https://developers.facebook.com/documentation/business-messaging/whatsapp/webhooks/reference/messages/location
        
        return WhatsApp_OB_LocationMessage(
            to       = to_number,
            location = content,
        ).model_dump( exclude_none = True)
    
    else :
        raise ValueError(
            f"In function 'write_payload': Invalid content type '{type(content)}'"
        )


# =========================================================================================
# OUTBOUND: Text Messages

def send_whatsapp_text(
    operator_id : str,
    to_number   : str,
    text        : str,
    debug       : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Send a text-only WhatsApp message \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        text        : Message body
        debug       : Whether to emit response diagnostics
    """
    
    results : list[WhatsAppSendResult] = []
    
    for _text_ in chunk_text(text) :
        
        payload  = write_payload( to_number, _text_)
        response = httpx.post(
            url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
            headers = write_headers(),
            json    = payload,
        )
        
        results.extend( collect_send_results( payload, response, debug) )
    
    return results

async def async_send_whatsapp_text(
    operator_id : str,
    to_number   : str,
    text        : str,
    debug       : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Send a text-only WhatsApp message asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        text        : Message body
        debug       : Whether to emit response diagnostics
    """
    async with httpx.AsyncClient() as client :
        
        results : list[WhatsAppSendResult] = []
        
        for _text_ in chunk_text(text) :
            payload  = write_payload( to_number, _text_)
            response = await client.post(
                url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
                headers = write_headers(),
                json    = payload,
            )
            results.extend( collect_send_results( payload, response, debug) )
    
    return results


# -----------------------------------------------------------------------------------------
# OUTBOUND: Interactive Messages

def send_whatsapp_interactive(
    operator_id : str,
    to_number   : str,
    message     : ServerInteractiveOptsMsg,
    debug       : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Send WhatsApp interactive responses (buttons/lists) \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        message     : Interactive options payload
        debug       : Whether to emit response diagnostics
    """
    payload  = write_payload( to_number, message)
    response = httpx.post(
        url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
        headers = write_headers(),
        json    = payload,
    )
    return collect_send_results( payload, response, debug)

async def async_send_whatsapp_interactive(
    operator_id : str,
    to_number   : str,
    message     : ServerInteractiveOptsMsg,
    debug       : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Send WhatsApp interactive responses asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        message     : Interactive options payload
        debug       : Whether to emit response diagnostics
    """
    payload = write_payload( to_number, message)
    
    async with httpx.AsyncClient() as client :
        response = await client.post(
            url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
            headers = write_headers(),
            json    = payload,
        )
    
    return collect_send_results( payload, response, debug)


# -----------------------------------------------------------------------------------------
# OUTBOUND: Flow Messages

def send_whatsapp_flow(
    operator_id : str,
    to_number   : str,
    message     : ServerFlowMsg,
    debug       : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Send a WhatsApp Flow launch message.
    """
    payload  = write_payload( to_number, message)
    response = httpx.post(
        url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
        headers = write_headers(),
        json    = payload,
    )
    return collect_send_results( payload, response, debug)

async def async_send_whatsapp_flow(
    operator_id : str,
    to_number   : str,
    message     : ServerFlowMsg,
    debug       : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Send a WhatsApp Flow launch message asynchronously.
    """
    payload = write_payload( to_number, message)
    async with httpx.AsyncClient() as client :
        response = await client.post(
            url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
            headers = write_headers(),
            json    = payload,
        )
    return collect_send_results( payload, response, debug)


# -----------------------------------------------------------------------------------------
# OUTBOUND: Template Messages

def send_whatsapp_template(
    operator_id : str,
    to_number   : str,
    message     : ServerTemplateMsg,
    debug       : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Send WhatsApp template messages \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        message     : Approved template payload
        debug       : Whether to emit response diagnostics
    """
    payload     = write_payload( to_number, message)
    response    = httpx.post(
        url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
        headers = write_headers(),
        json    = payload,
    )
    return collect_send_results( payload, response, debug)

async def async_send_whatsapp_template(
    operator_id : str,
    to_number   : str,
    message     : ServerTemplateMsg,
    debug       : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Send WhatsApp template messages asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        message     : Approved template payload
        debug       : Whether to emit response diagnostics
    """
    payload = write_payload( to_number, message)
    
    async with httpx.AsyncClient() as client :
        response = await client.post(
            url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
            headers = write_headers(),
            json    = payload,
        )
    
    return collect_send_results( payload, response, debug)


# -----------------------------------------------------------------------------------------
# OUTBOUND: Contacts & Locations

def send_whatsapp_content(
    operator_id : str,
    to_number   : str,
    content     : WhatsAppContactCard | WhatsAppLocation,
    debug       : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Send a WhatsApp content message \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        content     : WhatsApp contact or location
        debug       : Whether to emit response diagnostics
    """
    payload  = write_payload( to_number, content)
    response = httpx.post(
        url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
        headers = write_headers(),
        json    = payload,
    )
    return collect_send_results( payload, response, debug)

async def async_send_whatsapp_content(
    operator_id : str,
    to_number   : str,
    content     : WhatsAppContactCard | WhatsAppLocation,
    debug       : bool = False,
) -> list[WhatsAppSendResult] :
    """
    Send a WhatsApp content message asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        content     : WhatsApp contact or location
        debug       : Whether to emit response diagnostics
    """
    payload = write_payload( to_number, content)
    
    async with httpx.AsyncClient() as client :
        response = await client.post(
            url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
            headers = write_headers(),
            json    = payload,
        )
    
    return collect_send_results( payload, response, debug)


# -----------------------------------------------------------------------------------------
# OUTBOUND: Media

def send_whatsapp_media(
    operator_id : str,
    to_number   : str,
    media       : ServerMediaMsg,
    debug       : bool = False,
) -> bool :
    """
    Upload media and send it to the given WhatsApp number \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        media       : ServerMediaMsg describing the file to send
        debug       : Whether to emit response diagnostics
    Returns:
        True if upload/send succeeded; else False.
    """
    
    # Reference: https://developers.facebook.com/docs/whatsapp/cloud-api/reference/media/
    
    stage = "validation"
    try :
        if not media.content :
            raise ValueError("Missing media content")
        
        if debug :
            print_sep()
            print(f"Uploading media: {media.filepath}")
        
        stage           = "upload"
        upload_response = httpx.post(
            url     = f"{META_GRAPH_API_URL}/{operator_id}/media",
            headers = write_headers( content_type = False),
            data    = {
                "messaging_product": "whatsapp"
            },
            files   = {
                "file" : ( media.filepath, media.content, media.mime)
            },
            timeout = WHATSAPP_MEDIA_TIMEOUT,
        )
        
        if debug :
            print(f"Upload response status: {upload_response.status_code}")
            print(f"Upload response: {upload_response.text}")
        
        if upload_response.status_code != 200 :
            print(f"Upload failed with status {upload_response.status_code}")
            return False
        
        upload_data = upload_response.json()
        if "id" not in upload_data :
            print(f"No 'id' in upload response: {upload_data}")
            return False
        
        media.upload_id = upload_data["id"]
        if debug :
            print(
                f"Uploaded media with ID: {media.upload_id}\n"
                f"Sending media message..."
            )
        
        stage    = "send"
        payload  = write_payload( to_number, media)
        response = httpx.post(
            url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
            headers = write_headers(),
            json    = payload,
            timeout = WHATSAPP_MEDIA_TIMEOUT,
        )
        collect_send_results( payload, response, debug)
        
        return True
    
    except Exception as ex:
        print(
            f"Error sending media during {stage}: "
            f"{type(ex).__name__}: {ex!r}"
        )
    
    return False

async def async_send_whatsapp_media(
    operator_id : str,
    to_number   : str,
    media       : ServerMediaMsg,
    debug       : bool = False,
) -> bool :
    """
    Upload media and send it to the given WhatsApp number asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        media       : ServerMediaMsg describing the file to send
        debug       : Whether to emit response diagnostics
    Returns:
        True if upload/send succeeded; else False.
    """
    
    stage = "validation"
    try :
        if not media.content :
            raise ValueError("Missing media content")
        
        if debug :
            print_sep()
            print(f"Uploading media: {media.filepath}")
        
        async with httpx.AsyncClient( timeout = WHATSAPP_MEDIA_TIMEOUT) as client :
            
            stage           = "upload"
            upload_response = await client.post(
                url     = f"{META_GRAPH_API_URL}/{operator_id}/media",
                headers = write_headers( content_type = False),
                data    = {
                    "messaging_product": "whatsapp"
                },
                files   = {
                    "file" : ( media.filepath, media.content, media.mime)
                },
            )
            
            if debug :
                print(f"Upload response status: {upload_response.status_code}")
                print(f"Upload response: {upload_response.text}")
            
            if upload_response.status_code != 200 :
                print(f"Upload failed with status {upload_response.status_code}")
                return False
            
            upload_data = upload_response.json()
            if "id" not in upload_data :
                print(f"No 'id' in upload response: {upload_data}")
                return False
            
            media.upload_id = upload_data["id"]
            if debug :
                print(f"Uploaded media with ID: {media.upload_id}")
                print("Sending media message...")
            
            stage       = "send"
            payload     = write_payload( to_number, media)
            response    = await client.post(
                url     = f"{META_GRAPH_API_URL}/{operator_id}/messages",
                headers = write_headers(),
                json    = payload,
            )
        
        collect_send_results( payload, response, debug)
        
        return True
    
    except Exception as ex:
        print(
            f"Error sending media during {stage}: "
            f"{type(ex).__name__}: {ex!r}"
        )
    
    return False


# -----------------------------------------------------------------------------------------
# APP SUBSCRIPTIONS

def _has_waba_app_subscription(
    payload : dict[str, Any],
    app_id  : str,
) -> bool :
    """
    Check a subscribed-apps Graph response for one Meta app
    """
    subscriptions = payload.get("data")
    if not isinstance( subscriptions, list) :
        return False
    
    return any(
        isinstance( subscription, dict) and
        isinstance(
            app_data := subscription.get("whatsapp_business_api_data"),
            dict,
        ) and
        str( app_data.get("id") or "" ) == str(app_id)
        for subscription in subscriptions
    )


def is_app_subscribed_to_waba(
    waba_id        : str,
    business_token : str,
    app_id         : str,
) -> bool :
    """
    Check whether a Meta app is subscribed to a WhatsApp Business Account
    """
    payload = graph_request(
        method       = "GET",
        path         = f"{waba_id}/subscribed_apps",
        access_token = business_token,
    )
    
    return _has_waba_app_subscription( payload, app_id)


async def async_is_app_subscribed_to_waba(
    waba_id        : str,
    business_token : str,
    app_id         : str,
) -> bool :
    """
    Check asynchronously whether a Meta app is subscribed to a WABA
    """
    payload = await async_graph_request(
        method       = "GET",
        path         = f"{waba_id}/subscribed_apps",
        access_token = business_token,
    )
    
    return _has_waba_app_subscription( payload, app_id)


def subscribe_to_waba(
    waba_id : str,
    business_token : str,
) -> bool :
    """
    Subscribe the configured Meta app to one WhatsApp Business Account
    """
    payload = graph_request(
        method       = "POST",
        path         = f"{waba_id}/subscribed_apps",
        access_token = business_token,
    )
    if payload.get("success") is True :
        return True
    
    raise MetaGraphError(
        "Meta did not confirm the WhatsApp Business Account subscription"
    )

async def async_subscribe_to_waba(
    waba_id : str,
    business_token : str,
) -> bool :
    """
    Subscribe the configured Meta app to one WhatsApp Business Account asynchronously
    """
    payload = await async_graph_request(
        method       = "POST",
        path         = f"{waba_id}/subscribed_apps",
        access_token = business_token,
    )
    if payload.get("success") is True :
        return True
    
    raise MetaGraphError(
        "Meta did not confirm the WhatsApp Business Account subscription"
    )


def exchange_code_for_business_token(
    code         : str,
    app_id       : str,
    app_secret   : str,
    redirect_uri : str | None = None,
) -> str :
    """
    Exchange an Embedded Signup authorization code for a business access token
    """
    
    params = {
        "client_id"     : app_id,
        "client_secret" : app_secret,
        "code"          : code,
    }
    if redirect_uri :
        params["redirect_uri"] = redirect_uri
    
    payload      = graph_request( "GET", "oauth/access_token", params = params)
    access_token = str( payload.get("access_token") or "" ).strip()
    if access_token :
        return access_token
    
    raise MetaGraphError("Meta did not return an access token")


async def async_exchange_code_for_business_token(
    code         : str,
    app_id       : str,
    app_secret   : str,
    redirect_uri : str | None = None,
) -> str :
    """
    Exchange an Embedded Signup code for a business access token asynchronously
    """
    
    params = {
        "client_id"     : app_id,
        "client_secret" : app_secret,
        "code"          : code,
    }
    if redirect_uri :
        params["redirect_uri"] = redirect_uri
    
    payload = await async_graph_request(
        "GET",
        "oauth/access_token",
        params = params,
    )
    access_token = str( payload.get("access_token") or "" ).strip()
    if access_token :
        return access_token
    
    raise MetaGraphError("Meta did not return an access token")


def list_phone_numbers_for_waba(
    waba_id        : str,
    business_token : str,
) -> list[dict[str, Any]] :
    """
    List the phone numbers authorized for one WhatsApp Business Account
    """
    
    payload = graph_request(
        method       = "GET",
        path         = f"{waba_id}/phone_numbers",
        access_token = business_token,
        params       = { "fields" : ",".join(WABA_PHONE_NUMBER_FIELDS) },
    )
    data = payload.get("data")
    
    return data if isinstance( data, list) else [ ]


async def async_list_phone_numbers_for_waba(
    waba_id        : str,
    business_token : str,
) -> list[dict[str, Any]] :
    """
    List a WABA's authorized phone numbers asynchronously
    """
    
    payload = await async_graph_request(
        method       = "GET",
        path         = f"{waba_id}/phone_numbers",
        access_token = business_token,
        params       = { "fields" : ",".join(WABA_PHONE_NUMBER_FIELDS) },
    )
    data = payload.get("data")
    
    return data if isinstance( data, list) else [ ]


def request_app_data_sync(
    phone_number_id : str,
    business_token  : str,
    sync_type       : WhatsAppAppDataSyncType,
) -> str :
    """
    Request one WhatsApp Business app contact or history synchronization
    """
    payload = graph_request(
        method       = "POST",
        path         = f"{phone_number_id}/smb_app_data",
        access_token = business_token,
        json_data    = {
            "messaging_product" : "whatsapp",
            "sync_type"         : sync_type,
        },
    )
    request_id = str( payload.get("request_id") or "" ).strip()
    if request_id :
        return request_id
    
    raise MetaGraphError("Meta did not return an app-data synchronization request ID")


async def async_request_app_data_sync(
    phone_number_id : str,
    business_token  : str,
    sync_type       : WhatsAppAppDataSyncType,
) -> str :
    """
    Request one WhatsApp Business app synchronization asynchronously
    """
    payload = await async_graph_request(
        method       = "POST",
        path         = f"{phone_number_id}/smb_app_data",
        access_token = business_token,
        json_data    = {
            "messaging_product" : "whatsapp",
            "sync_type"         : sync_type,
        },
    )
    request_id = str( payload.get("request_id") or "" ).strip()
    if request_id :
        return request_id
    
    raise MetaGraphError("Meta did not return an app-data synchronization request ID")


# -----------------------------------------------------------------------------------------
# FORMATTING

def chunk_text( text : str, max_len : int = 4096) -> list[str] :
    
    if len(text) <= max_len :
        return [text]
    
    i_mid  = len(text) // 2
    result = []
    
    result.extend( chunk_text( text[:i_mid ], max_len) )
    result.extend( chunk_text( text[ i_mid:], max_len) )
    
    return result

def markdown_to_whatsapp( markdown_text : str) -> str :
    
    """
    Convert markdown formatting to WhatsApp formatting \\
    Args:
        markdown_text : Markdown text to transform
    Returns:
        Text adjusted for WhatsApp-supported markup
    """
    
    # Convert markdown bold to WhatsApp bold
    text = re.sub( r'\*\*(.*?)\*\*', r'*\1*', markdown_text)
    
    # Convert markdown italic to WhatsApp italic
    text = re.sub( r'__(.*?)__', r'_\1_', text)
    
    # Remove heading markers (## or # at start of line)
    text = re.sub( r'^#+\s+', '', text, flags = re.MULTILINE)
    
    return text
