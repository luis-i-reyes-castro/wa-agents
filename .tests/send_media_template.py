#!/usr/bin/env python3
"""
Upload a local file and send it as a WhatsApp template header.

Usage:
    python .tests/send_media_template.py <config.json> <media-file>

Config JSON:
    wa_token               : Optional Graph API token. When omitted, the script
                             uses the sending number's business token, then WA_TOKEN.
    phone_number_id        : Sending phone-number ID
    recipient_phone_number : Recipient phone number
    template_name          : Approved template name (the send API uses the name)
    language               : Template language code, for example es_EC
    parameters             : Optional body parameters, named dict or positional list

PDF files become a document header. Image and video files use the matching header.
Omit `parameters`, or leave it empty, when the approved body has no variables.
"""

import argparse
import httpx
import json

from dotenv import (
    find_dotenv,
    load_dotenv,
)
from mimetypes import guess_type
from pathlib import Path

from sofia_utils.io import (
    JSON_INDENT,
    load_json_file,
)

from wa_agents.case_handler_models import (
    ServerDocumentMsg,
    ServerMediaMsg,
    ServerTemplateMsg,
)
from wa_agents.io_functions import (
    META_GRAPH_API_URL,
    WHATSAPP_MEDIA_TIMEOUT,
    collect_send_results,
    parse_graph_response,
    write_headers,
    write_payload,
)
from wa_agents.io_models import (
    WhatsApp_OB_TemplateDocument,
    WhatsApp_OB_TemplateImage,
    WhatsApp_OB_TemplateVideo,
)
from wa_agents.supabase import SyncSupabaseStorage


REQUIRED = (
    "phone_number_id",
    "recipient_phone_number",
    "template_name",
    "language",
)
""" Config fields required to upload media and send the template """

HEADER_MODELS = {
    "document" : WhatsApp_OB_TemplateDocument,
    "image"    : WhatsApp_OB_TemplateImage,
    "video"    : WhatsApp_OB_TemplateVideo,
}
""" Template header model for each supported media kind """


def media_header_kind( mime : str) -> str :
    """
    Map a file MIME type to a template header type \\
    Args:
        mime : MIME type guessed from the file name
    Returns:
        `document` for a PDF, otherwise `image` or `video`.
    Raises:
        ValueError: The MIME type is not a PDF, image, or video.
    """
    if mime == "application/pdf" :
        return "document"
    
    kind = mime.split("/")[0]
    if kind in { "image", "video" } :
        return kind
    
    raise ValueError(
        f"Unsupported media type '{mime}'; expected a PDF, image, or video"
    )


def load_media_file( path : Path) -> tuple[ str, ServerMediaMsg] :
    """
    Read a PDF, image, or video into a server media message \\
    Args:
        path : Filesystem path of the file to upload
    Returns:
        Header kind and the media message carrying the file bytes.
    """
    mime = guess_type(path.name)[0]
    if not mime :
        raise ValueError(f"Could not guess a MIME type for '{path.name}'")
    
    kind    = media_header_kind(mime)
    content = path.read_bytes()
    if not content :
        raise ValueError(f"Media file '{path}' is empty")
    
    fields = {
        "filepath" : path.name,
        "content"  : content,
    }
    media = (
        ServerDocumentMsg(**fields)
        if kind == "document" else
        ServerMediaMsg( mime = mime, **fields)
    )
    
    return kind, media


def resolve_token( config : dict, phone_number_id : str) -> str | None :
    """
    Choose the Graph API token for this send \\
    Args:
        config          : Script config. `wa_token` overrides stored credentials.
        phone_number_id : Sending phone-number ID used for the business-token lookup.
    Returns:
        The config token or the number's business token.
        None when the number has no Embedded Signup token, so the request uses WA_TOKEN.
    """
    supplied = str( config.get("wa_token") or "" ).strip()
    if supplied :
        return supplied
    
    return SyncSupabaseStorage().get_business_token(phone_number_id)


def upload_media(
    token           : str | None,
    phone_number_id : str,
    media           : ServerMediaMsg,
) -> str :
    """
    Upload media bytes and return the Graph media ID \\
    Args:
        token           : Graph API token, or None to use WA_TOKEN
        phone_number_id : Sending phone-number ID
        media           : File name, MIME type, and bytes
    Returns:
        Numeric media ID returned by Graph.
    """
    response  = httpx.post(
        url     = f"{META_GRAPH_API_URL}/{phone_number_id}/media",
        headers = write_headers(
            content_type   = False,
            business_token = token,
        ),
        data    = { "messaging_product" : "whatsapp" },
        files   = {
            "file" : ( media.filepath, media.content, media.mime),
        },
        timeout = WHATSAPP_MEDIA_TIMEOUT,
    )
    upload_id = parse_graph_response(response).get("id")
    if not ( isinstance( upload_id, str) and upload_id.isdecimal() ) :
        raise RuntimeError("Media upload did not return a numeric media ID")
    
    return upload_id


def main() -> None :
    """
    Upload the given file and send the configured template
    """
    parser = argparse.ArgumentParser(
        description = "Upload a file and send it as a WhatsApp template header",
    )
    parser.add_argument( "config", help = "JSON config path")
    parser.add_argument( "media",  help = "PDF, image, or video file")
    args   = parser.parse_args()
    config = load_json_file(args.config)
    if not isinstance( config, dict) :
        raise SystemExit(f"Config file '{args.config}' is missing or not a JSON object")
    
    missing = [ key for key in REQUIRED if not config.get(key) ]
    if missing :
        raise SystemExit( "Config is missing: " + ", ".join(missing) )
    
    media_path      = Path(args.media).expanduser()
    kind, media     = load_media_file(media_path)
    phone_number_id = str(config["phone_number_id"])
    token           = resolve_token( config, phone_number_id)
    media_id        = upload_media( token, phone_number_id, media)
    print(f"Uploaded media ID: {media_id}")
    
    header = { "id" : media_id }
    if kind == "document" :
        header["filename"] = media_path.name
    
    message  = ServerTemplateMsg(
        name       = str(config["template_name"]),
        language   = str(config["language"]),
        parameters = config.get("parameters") or None,
        **{ kind : HEADER_MODELS[kind](**header) },
    )
    payload  = write_payload( str(config["recipient_phone_number"]), message)
    response = httpx.post(
        url     = f"{META_GRAPH_API_URL}/{phone_number_id}/messages",
        headers = write_headers( business_token = token),
        json    = payload,
        timeout = WHATSAPP_MEDIA_TIMEOUT,
    )
    results  = collect_send_results( payload, response, debug = True)
    print(json.dumps( results, indent = JSON_INDENT))


if __name__ == "__main__" :
    load_dotenv(find_dotenv( usecwd = True))
    main()
