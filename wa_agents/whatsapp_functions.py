#!/usr/bin/env python3

import hashlib
import hmac
import httpx
import os
import re

from typing import Any

from sofia_utils.printing import print_sep

from .basemodels import (
    OutgoingDocumentMsg,
    OutgoingMediaMsg,
    ServerInteractiveOptsMsg,
    WhatsAppContactPayload,
    WhatsAppLocation,
    WhatsAppMediaData,
)


API_URL = "https://graph.facebook.com/v25.0/"


# -----------------------------------------------------------------------------------------
# MESSAGES: INCOMING

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

def verify_payload_signature(
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
# MESSAGES: OUTGOING

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

def send_whatsapp_media(
    operator_id : str,
    to_number   : str,
    media       : OutgoingMediaMsg,
) -> bool :
    """
    Upload media and send it to the given WhatsApp number \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        media       : OutgoingMediaMsg describing the file to send
    Returns:
        True if upload/send succeeded; else False.
    """
    
    # Reference: https://developers.facebook.com/docs/whatsapp/cloud-api/reference/media/
    
    try:
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
    media       : OutgoingMediaMsg,
) -> bool :
    """
    Upload media and send it to the given WhatsApp number asynchronously \\
    Args:
        operator_id : Business phone-number id
        to_number   : Recipient phone number
        media       : OutgoingMediaMsg describing the file to send
    Returns:
        True if upload/send succeeded; else False.
    """
    
    try:
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
              | ServerInteractiveOptsMsg
              | WhatsAppContactPayload
              | WhatsAppLocation
              | OutgoingMediaMsg,
) -> dict[ str, Any] :
    """
    Serialize outgoing text/interactive/media messages \\
    Args:
        to_number : WhatsApp recipient phone number
        content   : Text, interactive options, or media payload
    Returns:
        Dictionary ready to send to the Graph API.
    """
    
    payload = { "messaging_product" : "whatsapp",
                "to"                : to_number }
    
    if isinstance( content, str) :
        
        # Reference: https://developers.facebook.com/docs/whatsapp/cloud-api/messages/text-messages
        
        payload["type"] = "text"
        payload["text"] = { "body" : content }
    
    elif isinstance( content, ServerInteractiveOptsMsg) :
        
        # References:
        # Interactive Reply Buttons: https://developers.facebook.com/docs/whatsapp/cloud-api/messages/interactive-reply-buttons-messages
        # Interactive Lists: https://developers.facebook.com/docs/whatsapp/cloud-api/messages/interactive-list-messages/
        
        payload["type"]        = "interactive"
        payload["interactive"] = { "type" : content.type }
        
        if ( text_header := content.header ) :
            payload["interactive"]["header"] = { "type" : "text",
                                                    "text" : text_header }
        
        if ( text_body := content.body ) :
            payload["interactive"]["body"] = { "text" : text_body }            
        
        if ( text_footer := content.footer ) :
            payload["interactive"]["footer"] = { "text" : text_footer }
        
        match content.type :
            case "button" :
                # Initialize action dict
                payload["interactive"]["action"] = \
                    {
                    "buttons" :
                        [
                            {
                                "type"  : "reply",
                                "reply" : { "id" : option.id, "title" : option.title },
                            }
                            for option in content.options
                        ],
                    }
            case "list" :
                # Initialize action dict
                payload["interactive"]["action"] = \
                    {
                    "button"   : str(content.button),
                    "sections" :
                        [ {
                            "rows"  : [
                                opt.model_dump( exclude_none = True)
                                for opt in content.options
                            ]
                        } ],
                    }
    
    elif isinstance( content, WhatsAppContactPayload) :
        
        # Reference: https://developers.facebook.com/documentation/business-messaging/whatsapp/messages/contacts-messages
        
        payload["type"]     = "contacts"
        payload["contacts"] = [ content.model_dump( exclude_none = True) ]
    
    elif isinstance( content, WhatsAppLocation) :
        
        # Reference: https://developers.facebook.com/documentation/business-messaging/whatsapp/webhooks/reference/messages/location
        
        payload["type"]     = "location"
        payload["location"] = content.model_dump( exclude_none = True)
    
    elif isinstance( content, OutgoingMediaMsg) :
        
        # Reference: https://developers.facebook.com/docs/whatsapp/cloud-api/messages/image-messages
        
        payload["type"]       = content.type
        payload[content.type] = { "id" : content.upload_id }
        if content.caption :
            payload[content.type]["caption"] = content.caption
        if (
            isinstance( content, OutgoingDocumentMsg) and
            ( filename := content.filename )
        ) :
            payload[content.type]["filename"] = filename
    
    else :
        raise ValueError(f"In write_payload: Invalid content type '{type(content)}'")
    
    return payload

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
