#!/usr/bin/env python3

import hashlib
import hmac
import httpx
import os
import re

from typing import Any

from sofia_utils.printing import print_sep

from .case_handler_models import (
    ServerDocumentMsg,
    ServerInteractiveOptsMsg,
    ServerMediaMsg,
    ServerTemplateMsg,
    ServerTextMsg,
)
from .whatsapp_models import (
    WhatsAppContactPayload,
    WhatsAppLocation,
    WhatsAppMediaData,
    WhatsAppText,
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


API_URL = "https://graph.facebook.com/v26.0/"


# =========================================================================================
# INBOUND

def fetch_media( media_data : WhatsAppMediaData) -> bytes :
    """
    Download WhatsApp media content using its media id \\
    Args:
        media_data : WhatsApp media metadata payload
    Returns:
        Raw media bytes
    """
    result = None
    
    # Request media URL
    req_url   = f"{API_URL}{media_data.id}"
    req_head  = write_headers()
    req_resp  = httpx.get( req_url, headers = req_head).json()
    media_url = req_resp.get("url")
    
    # If response contains media URL then download
    if media_url :
        req_resp = httpx.get( media_url, headers = req_head)
        result   = req_resp.content
    else :
        print("In download_media: No file URL received")
    
    return result

async def async_fetch_media( media_data : WhatsAppMediaData) -> bytes :
    """
    Download WhatsApp media content using its media id \\
    Args:
        media_data : WhatsApp media metadata payload
    Returns:
        Raw media bytes
    """
    result = None
    
    req_url  = f"{API_URL}{media_data.id}"
    req_head = write_headers()
    
    async with httpx.AsyncClient() as client :
        
        req_resp  = await client.get( req_url, headers = req_head)
        media_url = req_resp.json().get("url")
        
        if media_url :
            req_resp = await client.get( media_url, headers = req_head)
            result   = req_resp.content
        else :
            print("In download_media: No file URL received")
    
    return result

def verify_app_secret(
    payload   : bytes | None,
    signature : str   | None,
) -> bool :
    
    if not ( signature and signature.startswith("sha256=") ) :
        return False
    
    if not ( WA_APP_SECRET := os.getenv("WA_APP_SECRET") ) :
        raise RuntimeError("Environment variable 'WA_APP_SECRET' was not found")
    
    expected = hmac.new(
        key       = WA_APP_SECRET.encode("utf-8"),
        msg       = payload,
        digestmod = hashlib.sha256,
    ).hexdigest()
    
    return hmac.compare_digest( signature, f"sha256={expected}")


# -----------------------------------------------------------------------------------------
# OUTBOUND: Text Messages

def send_whatsapp_text(
    operator_id : str,
    to_number   : str,
    text        : str,
) -> None :
    """
    Send a text-only WhatsApp message \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        text        : Message body
    """
    
    # 1) Declare message URL and headers
    msg_url     = f"{API_URL}{operator_id}/messages"
    msg_headers = write_headers( content_type = True)
    
    # 2) Chunk the text
    for _text_ in chunk_text(text) :
        # 2-1) Write payload and post message
        payload  = write_payload( to_number, _text_)
        response = httpx.post( msg_url, headers = msg_headers, json = payload)
        # 2-2) Print response
        print_sep()
        print( "Reply response:", response.json())
    
    return

async def async_send_whatsapp_text(
    operator_id : str,
    to_number   : str,
    text        : str,
) -> None :
    """
    Send a text-only WhatsApp message asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        text        : Message body
    """
    
    msg_url     = f"{API_URL}{operator_id}/messages"
    msg_headers = write_headers( content_type = True)
    
    async with httpx.AsyncClient() as client :
        
        for _text_ in chunk_text(text) :
            payload  = write_payload( to_number, _text_)
            response = await client.post(
                url     = msg_url,
                headers = msg_headers,
                json    = payload,
            )
            print_sep()
            print( "Reply response:", response.json())
    
    return


# -----------------------------------------------------------------------------------------
# OUTBOUND: Interactive Messages

def send_whatsapp_interactive(
    operator_id : str,
    to_number   : str,
    message     : ServerInteractiveOptsMsg,
) -> None :
    """
    Send WhatsApp interactive responses (buttons/lists) \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        message     : Interactive options payload
    """
    
    # 1) Declare message URL and headers
    msg_url     = f"{API_URL}{operator_id}/messages"
    msg_headers = write_headers( content_type = True)
    # 2) Write payload and post message
    payload  = write_payload( to_number, message)
    response = httpx.post( msg_url, headers = msg_headers, json = payload)
    # 3) Print response
    print_sep()
    print( "Reply response:", response.json())
    
    return

async def async_send_whatsapp_interactive(
    operator_id : str,
    to_number   : str,
    message     : ServerInteractiveOptsMsg,
) -> None :
    """
    Send WhatsApp interactive responses asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        message     : Interactive options payload
    """
    
    msg_url     = f"{API_URL}{operator_id}/messages"
    msg_headers = write_headers( content_type = True)
    payload     = write_payload( to_number, message)
    
    async with httpx.AsyncClient() as client :
        response = await client.post( msg_url, headers = msg_headers, json = payload)
    
    print_sep()
    print( "Reply response:", response.json())
    
    return


# =========================================================================================
# OUTBOUND: Template Messages

def send_whatsapp_template(
    operator_id : str,
    to_number   : str,
    message     : ServerTemplateMsg,
) -> None :
    """
    Send WhatsApp template messages \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        message     : Approved template payload
    """
    
    msg_url     = f"{API_URL}{operator_id}/messages"
    msg_headers = write_headers( content_type = True)
    payload     = write_payload( to_number, message)
    response    = httpx.post( msg_url, headers = msg_headers, json = payload)
    
    print_sep()
    print( "Reply response:", response.json())
    
    return

async def async_send_whatsapp_template(
    operator_id : str,
    to_number   : str,
    message     : ServerTemplateMsg,
) -> None :
    """
    Send WhatsApp template messages asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        message     : Approved template payload
    """
    
    msg_url     = f"{API_URL}{operator_id}/messages"
    msg_headers = write_headers( content_type = True)
    payload     = write_payload( to_number, message)
    
    async with httpx.AsyncClient() as client :
        response = await client.post( msg_url, headers = msg_headers, json = payload)
    
    print_sep()
    print( "Reply response:", response.json())
    
    return


# -----------------------------------------------------------------------------------------
# OUTBOUND: Contacts & Locations

def send_whatsapp_content(
    operator_id : str,
    to_number   : str,
    content     : WhatsAppContactPayload | WhatsAppLocation,
) -> None :
    """
    Send a WhatsApp content message \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        content     : WhatsApp contact or location
    """
    
    # 1) Declare message URL and headers
    msg_url     = f"{API_URL}{operator_id}/messages"
    msg_headers = write_headers( content_type = True)
    
    # 2) Write payload and post message
    payload  = write_payload( to_number, content)
    response = httpx.post( msg_url, headers = msg_headers, json = payload)
    # 3) Print response
    print_sep()
    print( "Reply response:", response.json())
    
    return

async def async_send_whatsapp_content(
    operator_id : str,
    to_number   : str,
    content     : WhatsAppContactPayload | WhatsAppLocation,
) -> None :
    """
    Send a WhatsApp content message asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        content     : WhatsApp contact or location
    """
    
    msg_url     = f"{API_URL}{operator_id}/messages"
    msg_headers = write_headers( content_type = True)
    payload     = write_payload( to_number, content)
    
    async with httpx.AsyncClient() as client :
        response = await client.post( msg_url, headers = msg_headers, json = payload)
    
    print_sep()
    print( "Reply response:", response.json())
    
    return


# -----------------------------------------------------------------------------------------
# OUTBOUND: Media

def send_whatsapp_media(
    operator_id : str,
    to_number   : str,
    media       : ServerMediaMsg,
) -> bool :
    """
    Upload media and send it to the given WhatsApp number \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        media       : ServerMediaMsg describing the file to send
    Returns:
        True if upload/send succeeded; else False.
    """
    
    # Reference: https://developers.facebook.com/docs/whatsapp/cloud-api/reference/media/
    
    try:
        if not media.content :
            raise ValueError("Missing media content")
        
        # 1) UPLOAD THE MEDIA TO GET A MEDIA ID
        print_sep()
        print(f"Uploading media: {media.filepath}")
        
        # Post upload
        upload_url  = f"{API_URL}{operator_id}/media"
        upload_head = write_headers()
        files       = { "file" : ( media.filepath, media.content, media.mime) }
        data        = { "messaging_product": "whatsapp" }
        upload_response = httpx.post(
            url     = upload_url,
            headers = upload_head,
            files   = files,
            data    = data,
        )
        
        # Print response
        print(f"Upload response status: {upload_response.status_code}")
        print(f"Upload response: {upload_response.text}")
        
        # Handle response status failure
        if upload_response.status_code != 200 :
            print(f"Upload failed with status {upload_response.status_code}")
            return False
        
        # Handle response without media ID
        upload_data = upload_response.json()
        if "id" not in upload_data :
            print(f"No 'id' in upload response: {upload_data}")
            return False
        
        # Upload succeded so copy media ID
        media.upload_id = upload_data["id"]
        print(f"Uploaded media with ID: {media.upload_id}")
        
        # 2) SEND THE MEDIA MESSAGE WITH THE CORRESPONDING MEDIA ID
        print("Sending media message...")
        
        # Declare message URL and headers
        msg_url     = f"{API_URL}{operator_id}/messages"
        msg_headers = write_headers( content_type = True)
        
        # Write payload and post message
        payload  = write_payload( to_number, media)
        response = httpx.post( msg_url, headers = msg_headers, json = payload)
        
        # Print response
        print(f"Media message response status: {response.status_code}")
        print(f"Media reply response: {response.json()}")
        
        return True
    
    except Exception as ex:
        print(f"Error sending media: {ex}")
    
    return False

async def async_send_whatsapp_media(
    operator_id : str,
    to_number   : str,
    media       : ServerMediaMsg,
) -> bool :
    """
    Upload media and send it to the given WhatsApp number asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        media       : ServerMediaMsg describing the file to send
    Returns:
        True if upload/send succeeded; else False.
    """
    
    try:
        if not media.content :
            raise ValueError("Missing media content")
        
        print_sep()
        print(f"Uploading media: {media.filepath}")
        
        upload_url  = f"{API_URL}{operator_id}/media"
        upload_head = write_headers()
        files       = { "file" : ( media.filepath, media.content, media.mime) }
        data        = { "messaging_product": "whatsapp" }
        
        async with httpx.AsyncClient() as client :
            
            upload_response = await client.post(
                url     = upload_url,
                headers = upload_head,
                files   = files,
                data    = data,
            )
            
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
            print(f"Uploaded media with ID: {media.upload_id}")
            print("Sending media message...")
            
            msg_url     = f"{API_URL}{operator_id}/messages"
            msg_headers = write_headers( content_type = True)
            payload     = write_payload( to_number, media)
            response    = await client.post(
                url     = msg_url,
                headers = msg_headers,
                json    = payload,
            )
        
        print(f"Media message response status: {response.status_code}")
        print(f"Media reply response: {response.json()}")
        
        return True
    
    except Exception as ex:
        print(f"Error sending media: {ex}")
    
    return False


# -----------------------------------------------------------------------------------------
# OUTBOUND: Headers & Payloads

def write_headers( content_type : bool = False) -> dict :
    """
    Compose Graph API headers with auth and optional JSON content-type \\
    Args:
        content_type : When True, include `Content-Type: application/json`
    Returns:
        Headers dictionary for Graph API HTTP calls
    """
    
    if not ( WA_TOKEN := os.getenv("WA_TOKEN") ) :
        raise RuntimeError("Environment variable 'WA_TOKEN' was not found")
    
    headers = { "Authorization": f"Bearer {WA_TOKEN}" }
    if content_type :
        headers["Content-Type"] = "application/json"
    
    return headers

def write_payload(
    to_number : str,
    content   : str
              | ServerTextMsg
              | ServerInteractiveOptsMsg
              | ServerTemplateMsg
              | ServerMediaMsg
              | WhatsAppContactPayload
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
    
    elif isinstance( content, WhatsAppContactPayload) :
        
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
