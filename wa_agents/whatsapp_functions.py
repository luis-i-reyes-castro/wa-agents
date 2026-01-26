#!/usr/bin/env python3

import os
import re
import requests

from typing import Any

from sofia_utils.printing import print_sep

from .basemodels import ( OutgoingMediaMsg,
                          ServerInteractiveOptsMsg,
                          WhatsAppMediaData )


# -----------------------------------------------------------------------------------------
# CONSTANTS AND ENVIRONMENT VARIABLES

API_URL  = "https://graph.facebook.com/v23.0/"
WA_TOKEN = os.getenv("WA_TOKEN")

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
    req_resp  = requests.get( req_url, headers = req_head).json()
    media_url = req_resp.get("url")
    
    # If response contains media URL then download
    if media_url :
        req_resp = requests.get( media_url, headers = req_head)
        result   = req_resp.content
    else :
        print("In download_media: No file URL received")
    
    return result

# -----------------------------------------------------------------------------------------
# MESSAGES: OUTGOING

def write_headers( content_type : bool = False) -> dict :
    """
    Compose Graph API headers with auth and optional JSON content-type \\
    Args:
        content_type : When True, include `Content-Type: application/json`
    Returns:
        Headers dictionary for requests
    """
    
    headers = { "Authorization": f"Bearer {WA_TOKEN}" }
    if content_type :
        headers["Content-Type"] = "application/json"
    
    return headers

def write_payload( to_number : str,
                   content   : str | ServerInteractiveOptsMsg | OutgoingMediaMsg
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
                        [ { "type" : "reply", "reply" : option.model_dump() }
                            for option in content.options ]
                    }
            case "list" :
                # Initialize action dict
                payload["interactive"]["action"] = \
                    {
                    "button"   : str(content.button),
                    "sections" :
                        [ {
                        "title" : str(None),
                        "rows"  : [ opt.model_dump() for opt in content.options ]
                        } ]
                    }
    
    elif isinstance( content, OutgoingMediaMsg) :
        
        # Reference: https://developers.facebook.com/docs/whatsapp/cloud-api/messages/image-messages
        
        payload["type"]       = content.type
        payload[content.type] = { "id" : content.upload_id }
        if content.caption :
            payload[content.type]["caption"] = content.caption
    
    else :
        raise ValueError(f"In write_payload: Invalid content type '{type(content)}'")
    
    return payload

def chunk_text( text : str, max_len : int = 4096) -> list[str] :
    
    if len(text) <= max_len :
        return [text]
    
    i_mid  = len(text) // 2
    result = []
    
    result.extend( chunk_text( text[:i_mid ], max_len) )
    result.extend( chunk_text( text[ i_mid:], max_len) )
    
    return result

def send_whatsapp_text( operator_id : str,
                        to_number   : str,
                        text        : str ) -> None :
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
        response = requests.post( msg_url, headers = msg_headers, json = payload)
        # 2-2) Print response
        print_sep()
        print( "Reply response:", response.json())
    
    return

def send_whatsapp_interactive( operator_id : str,
                               to_number   : str,
                               message     : ServerInteractiveOptsMsg ) -> None :
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
    response = requests.post( msg_url, headers = msg_headers, json = payload)
    # 3) Print response
    print_sep()
    print( "Reply response:", response.json())
    
    return

def send_whatsapp_media( operator_id : str,
                         to_number   : str,
                         media       : OutgoingMediaMsg ) -> bool :
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
        # 1) UPLOAD THE IMAGE TO GET A MEDIA ID
        print_sep()
        print(f"Uploading image: {media.filepath}")
        
        # Post upload
        upload_url  = f"{API_URL}{operator_id}/media"
        upload_head = write_headers()
        files       = { "file" : ( media.filepath, media.content, media.mime) }
        data        = { "messaging_product": "whatsapp" }
        upload_response = requests.post( upload_url,
                                         headers = upload_head,
                                         files   = files,
                                         data    = data )
        
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
        
        # 2) SEND THE IMAGE MESSAGE WITH THE CORRESPONDING MEDIA ID
        print(f"Sending image message...")
        
        # Declare message URL and headers
        msg_url     = f"{API_URL}{operator_id}/messages"
        msg_headers = write_headers( content_type = True)
        
        # Write payload and post message
        payload  = write_payload( to_number, media)
        response = requests.post( msg_url, headers = msg_headers, json = payload)
        
        # Print response
        print(f"Image message response status: {response.status_code}")
        print( "Image reply response:", response.json())
        
        return True
    
    except Exception as ex:
        print(f"Error sending image: {ex}")
    
    return False

# -----------------------------------------------------------------------------------------
# FORMATTING

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
