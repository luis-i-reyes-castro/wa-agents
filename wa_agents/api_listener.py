"""
FastAPI app that receives and enqueues WhatsApp webhooks.
"""

from __future__ import annotations

import json
import logging
import os

from contextlib import asynccontextmanager
from fastapi import (
    FastAPI,
    Request,
    status,
)
from fastapi.responses import (
    JSONResponse,
    PlainTextResponse,
)
from pydantic import ValidationError
from typing import (
    Any,
    AsyncIterator,
)

from sofia_utils.io import JSON_INDENT
from sofia_utils.printing import (
    get_qualname as here,
    print_sep,
)

from .database_queue import AsyncWhatsAppDatabaseQueue
from .io_functions import verify_app_secret


def _load_app_secrets() -> tuple[ str, ...] :
    """
    Return configured Meta app secrets for webhook signature verification.
    """
    configured = os.getenv("WA_APPS")
    if not configured :
        secret = os.getenv("WA_APP_SECRET")
        if secret :
            return ( secret, )
        raise RuntimeError(
            "Environment variables 'WA_APPS' and 'WA_APP_SECRET' were not found"
        )
    
    try :
        apps = json.loads(configured)
    except json.JSONDecodeError as ex :
        raise RuntimeError("Environment variable 'WA_APPS' is invalid JSON") from ex
    
    if not (
        isinstance( apps, list) and
        apps                    and
        all(
            isinstance( app, dict)              and
            isinstance( app.get("id"), str)     and
            bool(app["id"])                     and
            isinstance( app.get("secret"), str) and
            bool(app["secret"])
            for app in apps
        )
    ) :
        raise RuntimeError(
            "Environment variable 'WA_APPS' must be a non-empty JSON list of "
            "objects with non-empty string 'id' and 'secret' fields"
        )
    
    return tuple( dict.fromkeys( app["secret"] for app in apps) )


class WhatsAppAPIListener (FastAPI) :
    """
    FastAPI app with WhatsApp webhook routes and database enqueueing.
    """
    
    def __init__(
        self,
        *,
        queue             : AsyncWhatsAppDatabaseQueue | None = None,
        verify_app_secret : bool = True,
        webhook_path      : str  = "/webhook",
        **kwargs          : Any,
    ) -> None :
        """
        Initialize the WhatsApp API listener. \\
        Args:
            queue             : Optional async WhatsApp database queue
            webhook_path      : Webhook route path
            verify_app_secret : Whether to verify payload signatures
            kwargs            : Forwarded to FastAPI
        """
        self._init_listener(
            queue            = queue or AsyncWhatsAppDatabaseQueue(),
            webhook_path     = webhook_path,
            verify_signature = verify_app_secret,
        )
        kwargs.setdefault( "lifespan", self.listener_lifespan)
        FastAPI.__init__( self, **kwargs)
        self.register_listener_routes( include_diagnostics = True)
        
        return
    
    def _init_listener(
        self,
        *,
        queue            : AsyncWhatsAppDatabaseQueue,
        verify_signature : bool,
        webhook_path     : str,
    ) -> None :
        """
        Initialize listener state without initializing FastAPI.
        """
        self.queue             = queue
        self.verify_app_secret = verify_signature
        self.webhook_path      = webhook_path
        
        self._app_secrets = _load_app_secrets() if verify_signature else ()
        
        return
    
    @asynccontextmanager
    async def listener_lifespan( self, _app : FastAPI) -> AsyncIterator[None] :
        """
        Keep the queue's async database pool open for the app lifetime.
        """
        async with self.queue.connection_pool() :
            yield
        
        return
    
    def register_listener_routes( self, *, include_diagnostics : bool) -> None :
        """
        Register listener routes and optional standalone diagnostics.
        """
        if include_diagnostics :
            self.add_api_route(
                path     = "/",
                endpoint = self.root,
                methods  = ["GET"],
            )
            self.add_api_route(
                path     = "/healthz",
                endpoint = self.healthz,
                methods  = ["GET"],
            )
            self.add_api_route(
                path     = "/debugz",
                endpoint = self.listener_debugz,
                methods  = ["GET"],
            )
        
        self.add_api_route(
            path           = self.webhook_path,
            endpoint       = self.verify,
            methods        = ["GET"],
            response_class = PlainTextResponse,
        )
        self.add_api_route(
            path     = self.webhook_path,
            endpoint = self.webhook,
            methods  = ["POST"],
        )
        
        return
    
    async def root(self) -> JSONResponse :
        """
        Root diagnostic endpoint.
        """
        return JSONResponse(
            content     = { "root" : "OK" },
            status_code = status.HTTP_200_OK,
        )
    
    async def healthz(self) -> JSONResponse :
        """
        Health endpoint.
        """
        return JSONResponse(
            content     = { "healthy" : True },
            status_code = status.HTTP_200_OK,
        )
    
    def _listener_debug_data(self) -> dict[str, Any] :
        """
        Return masked webhook verification configuration.
        """
        expected = os.getenv( "WA_VERIFY_TOKEN", default = "")
        masked   = ("*"*(len(expected)-4) + expected[-4:] ) if expected else ""
        
        return {
            "verify_token_set"       : bool(expected),
            "verify_token_tail"      : masked,
            "verify_meta_app_secret" : self.verify_app_secret,
        }
    
    async def listener_debugz(self) -> JSONResponse :
        """
        Return listener diagnostics.
        """
        return JSONResponse(
            content     = self._listener_debug_data(),
            status_code = status.HTTP_200_OK,
        )
    
    async def verify( self, request : Request) -> PlainTextResponse :
        """
        WhatsApp webhook verification endpoint.
        """
        params    = request.query_params
        token     = params.get( "hub.verify_token", "")
        challenge = params.get( "hub.challenge",    "")
        expected  = os.getenv( "WA_VERIFY_TOKEN", default = "")
        
        if token and challenge and expected and ( token == expected ) :
            return PlainTextResponse(
                challenge,
                status_code = status.HTTP_200_OK,
            )
        
        return PlainTextResponse(
            "Verification failed",
            status_code = status.HTTP_403_FORBIDDEN,
        )
    
    async def webhook( self, request : Request) -> JSONResponse :
        """
        Parse an incoming webhook request and hand its payload to the queue.
        """
        try :
            payload_bytes = await request.body()
        except Exception as ex :
            logging.error(f"In {here()}: Unable to read request body: {str(ex)}")
            payload_bytes = b""
        
        if self.verify_app_secret :
            signature = request.headers.get("x-hub-signature-256")
            if not any(
                verify_app_secret( payload_bytes, signature, secret)
                for secret in self._app_secrets
            ) :
                return JSONResponse(
                    content = {
                        "status" : "error",
                        "error"  : "Invalid signature",
                    },
                    status_code = status.HTTP_401_UNAUTHORIZED,
                )
        
        try :
            data = json.loads(payload_bytes)
        except Exception as ex :
            logging.error(f"In {here()}: Unable to parse request JSON: {str(ex)}")
            data = {}
        
        print_sep()
        print("[INFO] WhatsApp API incoming payload:")
        print(json.dumps( data, indent = JSON_INDENT))
        
        try :
            enqueue_result = await self.queue.enqueue(data)
        except ValidationError as ve :
            logging.error(f"In {here()}: Malformed payload: {str(ve)}")
            return JSONResponse(
                content = {
                    "status" : "error",
                    "error"  : f"Malformed payload: {ve}",
                },
                status_code = status.HTTP_200_OK,
            )
        except Exception as ex :
            logging.error(
                f"In {here()}: Failed to enqueue webhook payload: {str(ex)}"
            )
            return JSONResponse(
                content = {
                    "status" : "error",
                    "error"  : str(ex),
                },
                status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        
        response = {
            "status" : "ok",
            **enqueue_result,
        }
        
        return JSONResponse(
            content     = response,
            status_code = status.HTTP_200_OK,
        )
