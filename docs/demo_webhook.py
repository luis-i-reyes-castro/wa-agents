#!/usr/bin/env python3

import os
import uvicorn

from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    Request,
)
from fastapi.responses import PlainTextResponse
from pydantic import ValidationError

from sofia_utils.printing import (
    print_recursively,
    print_sep,
)
from wa_agents.whatsapp_models import WhatsApp_IB_Payload


load_dotenv()
VERIFY_TOKEN = os.getenv("WA_VERIFY_TOKEN")
print(f"WA_VERIFY TOKEN: {VERIFY_TOKEN}")


app = FastAPI()


@app.get("/webhook")
def verify( request : Request) -> PlainTextResponse :
    
    token     = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")
    
    if token == VERIFY_TOKEN:
        return PlainTextResponse( challenge or "" )
    
    return PlainTextResponse( "Verification failed", status_code = 403)


@app.post("/webhook")
async def webhook( request : Request) -> PlainTextResponse :
    
    data = await request.json()
    try :
        payload = WhatsApp_IB_Payload.model_validate(data)
        print_sep()
        print("WHATSAPP PAYLOAD STRUCTURE:")
        print(payload.model_dump_json( indent = 2))
    
    except ValidationError :
        print_sep()
        print("RECEIVED DATA:")
        print_recursively(data)
    
    except Exception as e :
        print( "Error:", e)
    
    return PlainTextResponse("OK")


if __name__ == "__main__":
    uvicorn.run( app, port = 5000)
