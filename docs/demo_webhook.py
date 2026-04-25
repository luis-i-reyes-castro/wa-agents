#!/usr/bin/env python3

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
from wa_agents.basemodels import (
    WhatsAppContactPayload,
    WhatsAppContactPayload_Name,
    WhatsAppContactPayload_Phone,
    WhatsAppLocation,
    WhatsAppPayload,
)
from wa_agents.whatsapp_functions import (
    send_whatsapp_content,
    send_whatsapp_text,
)


# -----------------------------------------------------------------------------------------
# Load WhatsApp Verify Token
load_dotenv()
VERIFY_TOKEN = os.getenv("WA_VERIFY_TOKEN")
print(f"WA_VERIFY TOKEN: {VERIFY_TOKEN}")


# -----------------------------------------------------------------------------------------
# Declare Flask App
app = Flask(__name__)

# Webhook verification (GET)
@app.route( "/webhook", methods = ["GET"])
def verify() :
    
    token     = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")
    
    if token == VERIFY_TOKEN:
        return challenge
    
    return "Verification failed", 403

# Webhook implementation (POST): Handle incoming messages
@app.route( "/webhook", methods = ["POST"])
def webhook() :
    
    data = request.get_json()
    try :
        wapl = WhatsAppPayload.model_validate(data)
        print_sep()
        print("WHATSAPP PAYLOAD STRUCTURE:")
        print( wapl.model_dump_json( indent = 2) )
        
        operator_id = wapl.entry[0].changes[0].value.metadata.phone_number_id
        user_id     = wapl.entry[0].changes[0].value.contacts[0].wa_id
        
        send_whatsapp_text( operator_id, user_id, "Hola mundo!")
        
        contact_card = WhatsAppContactPayload(
            name = WhatsAppContactPayload_Name(
                formatted_name = "Luis Reyes",
                first_name     = "Luis",
                last_name      = "Reyes",
            ),
            phones = (
                WhatsAppContactPayload_Phone(
                    phone = "+593995341161",
                    type  = "CELL",
                    wa_id = "593995341161",
                ),
            )
        )
        send_whatsapp_content( operator_id, user_id, contact_card)
        
        location_card = WhatsAppLocation(
            latitude  = -2.1859526634216,
            longitude = -79.988868713379,
            name      = "Reyes Castro Drones - Taller Principal",
            address   = "Urb. Laguna Club, Mz. 13 Sl. 83, Kilometro 12.5 Via a la Costa, Guayaquil - Ecuador",
        )
        send_whatsapp_content( operator_id, user_id, location_card)
    
    except ValidationError :
        print_sep()
        print("RECEIVED DATA:")
        print_recursively(data)
    
    except Exception as e :
        print( "Error:", e)
    
    return "ok", 200


# -----------------------------------------------------------------------------------------
# Run on port 8080 in debug mode

if __name__ == "__main__":
    app.run( port = 8080, debug = True)
