"""
Listener App
"""

import logging
import os
from flask import ( Flask,
                    request )
from pydantic import ValidationError

from sofia_utils.printing import print_sep

from .basemodels import WhatsAppPayload
from .queue_db import QueueDB


class Listener(Flask) :
    """
    Minimal Flask app that validates and enqueues WhatsApp webhooks
    """
    
    def __init__( self, import_name : str, queue_db : QueueDB) -> None :
        """
        Initialize the listener with its import name and queue backend \\
        Args:
            import_name : Flask import name
            queue_db    : QueueDB instance used to enqueue payloads
        """
        
        super().__init__(import_name)
        self.queue_db = queue_db
        
        self.config["PROPAGATE_EXCEPTIONS"] = True
        self.register_routes()
        
        return
    
    def register_routes(self) -> None :
        """
        Register health, verification, and webhook endpoints
        """
        # ---------------------------------------------------------------------------------
        # Diagnostic functions
        
        @self.get("/")
        def root() -> tuple[ str, int] :
            return "root ok", 200
        
        @self.get("/debugz")
        def debugz() -> tuple[ dict[ str, str | bool], int] :
            
            expected = os.getenv( "WA_VERIFY_TOKEN", default = "")
            masked   = ("*"*(len(expected)-4) + expected[-4:] ) if expected else ""
            
            return { "verify_token_set"  : bool(expected),
                     "verify_token_tail" : masked          }, 200
        
        @self.route( "/healthz", methods = ["GET"])
        def healthz() -> tuple[ str, int] :
            return "ok", 200
        
        # ---------------------------------------------------------------------------------
        # Webhook functions
        
        @self.route( "/webhook", methods = ["GET"])
        def verify() -> tuple[ str, int] :
            """
            Webhook verification
            """
            token     = request.args.get( "hub.verify_token", default = "")
            challenge = request.args.get( "hub.challenge",    default = "")
            expected  =        os.getenv( "WA_VERIFY_TOKEN",  default = "")
            
            if token and challenge and expected and ( token == expected ) :
                return challenge, 200
            
            return "Verification failed", 403
        
        @self.route( "/webhook", methods = ["POST"])
        def webhook() -> tuple[ dict[ str, str | bool], int] :
            """
            Handle incoming messages
            """
            data = request.get_json( silent = True) or {}
            print_sep()
            print( "Incoming:", data)
            
            try :
                payload = WhatsAppPayload.model_validate(data)
            except ValidationError as ve :
                return { "status" : "error",
                         "error"  : f"Malformed payload: {ve}" }, 200
            
            stored        = False
            storage_error = None
            try :
                from .supabase_storage import webhook_payload_write
                
                stored = webhook_payload_write(payload)
            except Exception as ex :
                storage_error = str(ex)
                logging.error(
                    "Failed to store WhatsApp webhook payload: %s",
                    storage_error,
                )
            
            try :
                enqueue_result = self.queue_db.enqueue(payload)
            except Exception as ex :
                return { "status" : "error",
                         "error"  : str(ex) }, 200
            
            response = { "status"   : "ok",
                         "enqueued" : enqueue_result,
                         "stored"   : stored }
            if storage_error :
                response["storage_error"] = storage_error
            
            return response, 200
        
        # ---------------------------------------------------------------------------------
        return
