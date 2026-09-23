#!/usr/bin/env python3

# NOTE:
# This script is old; the new stack uses FastAPI instead of Flask.
# Since Flask is not longer included in `requirements.txt`,
# install it via `pip install -U flask`.

import os
from dotenv import load_dotenv
from flask import (
    Flask,
    request,
)
from pydantic import ValidationError

from sofia_utils.printing import (
    print_recursively,
    print_sep,
)
from wa_agents.whatsapp_models import WhatsAppPayload


load_dotenv()
VERIFY_TOKEN = os.getenv("WA_VERIFY_TOKEN")
print(f"WA_VERIFY TOKEN: {VERIFY_TOKEN}")


app = Flask(__name__)


@app.route( "/webhook", methods = ["GET"])
def verify() :
    
    token     = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")
    
    if token == VERIFY_TOKEN:
        return challenge
    
    return "Verification failed", 403


@app.route( "/webhook", methods = ["POST"])
def webhook() :
    
    data = request.get_json()
    try :
        payload = WhatsAppPayload.model_validate(data)
        print_sep()
        print("WHATSAPP PAYLOAD STRUCTURE:")
        print(payload.model_dump_json( indent = 2))
    
    except ValidationError :
        print_sep()
        print("RECEIVED DATA:")
        print_recursively(data)
    
    except Exception as e :
        print( "Error:", e)
    
    return "ok", 200


if __name__ == "__main__":
    app.run( port = 8080, debug = True)
