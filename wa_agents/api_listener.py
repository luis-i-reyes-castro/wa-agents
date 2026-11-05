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

from sofia_utils.io import write_to_json_string
from sofia_utils.printing import (
    get_qualname as here,
    print_sep,
)

from .database_queue import AsyncWhatsAppDatabaseQueue
from .io_functions import (
    WhatsAppApp,
    verify_payload_signature,
)


class WhatsAppAPIListener (FastAPI) :
    """
    FastAPI app with WhatsApp webhook routes and database enqueueing.
    """
    
    def __init__(
        self,
        *,
        apps              : tuple[ WhatsAppApp, ...],
        debug             : bool = False,
        queue             : AsyncWhatsAppDatabaseQueue | None = None,
        verify_signatures : bool = True,
        **kwargs          : Any,
    ) -> None :
        """
        Initialize the WhatsApp API listener. \\
        Args:
            apps              : Ordered app definitions with explicit callback names
            debug             : Whether to emit debug output
            queue             : Optional async WhatsApp database queue
            verify_signatures : Whether to verify payload signatures
            kwargs            : Forwarded to FastAPI
        """
        self._init_listener(
            apps              = apps,
            debug             = debug,
            queue             = queue or AsyncWhatsAppDatabaseQueue(),
            verify_signatures = verify_signatures,
        )
        kwargs.setdefault( "debug", debug)
        kwargs.setdefault( "lifespan", self.listener_lifespan)
        FastAPI.__init__( self, **kwargs)
        self.register_listener_routes( include_diagnostics = True)
        self.validate_listener_routes()
        
        return
    
    def _init_listener(
        self,
        *,
        apps              : tuple[ WhatsAppApp, ...],
        debug             : bool,
        queue             : AsyncWhatsAppDatabaseQueue,
        verify_signatures : bool,
    ) -> None :
        """
        Initialize listener state without initializing FastAPI.
        """
        
        if not apps :
            raise ValueError(
                f"In {here()}: Meta apps array must be non-empty"
            )
        
        self.apps_by_path = {
            f"/{app['app']}" : app for app in apps
        }
        if not ( len(apps) == len(self.apps_by_path) ) :
            raise ValueError(
                f"In {here()}: Meta app names must be unique webhook callback names"
            )
        
        self.debug             = debug
        self.queue             = queue
        self.verify_signatures = verify_signatures
        
        return
    
    @asynccontextmanager
    async def listener_lifespan( self, _app : FastAPI) -> AsyncIterator[None] :
        """
        Keep the queue's async database pool open for the app lifetime.
        """
        async with self.queue.connection_pool() :
            yield
        
        return
    
    def register_listener_routes(
        self,
        *,
        include_diagnostics : bool,
    ) -> None :
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
        
        for path in self.apps_by_path :
            self.add_api_route(
                path           = path,
                endpoint       = self.verify,
                methods        = ["GET"],
                response_class = PlainTextResponse,
            )
            self.add_api_route(
                path     = path,
                endpoint = self.webhook,
                methods  = ["POST"],
            )
        
        return
    
    def validate_listener_routes(self) -> None :
        """
        Reject callback paths that overlap any other application route. \\
        Call again after adding application-specific routes.
        """
        for path in self.apps_by_path :
            for route in self.routes :
                if (
                    route.path_regex.fullmatch(path) and
                    ( route.endpoint not in ( self.verify, self.webhook) )
                ) :
                    raise RuntimeError(
                        f"In {here()}: Meta webhook callback path '{path}' "
                        f"collides with application route '{route}'."
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
            "verify_meta_app_secret" : self.verify_signatures,
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
        
        if (
            self.verify_signatures and
            not verify_payload_signature(
                app_secret = self.apps_by_path[request.scope["route"].path]["secret"],
                payload    = payload_bytes,
                signature  = request.headers.get("x-hub-signature-256"),
            )
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
        
        if self.debug :
            print_sep()
            print("[INFO] WhatsApp API incoming payload:")
            print(write_to_json_string(data))
        
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
