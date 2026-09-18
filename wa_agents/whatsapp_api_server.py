"""
FastAPI app that receives WhatsApp webhooks and runs the async queue worker.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

from contextlib import (
    asynccontextmanager,
    suppress,
)
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
    TYPE_CHECKING,
    Type,
)

from sofia_utils.io import JSON_INDENT
from sofia_utils.printing import (
    get_qualname as here,
    print_sep,
)
from sofia_utils.psycopg import (
    close_async_database_connection_pool,
    open_async_database_connection_pool,
)

from .whatsapp_functions import verify_app_secret


if TYPE_CHECKING :
    from .case_handler_base import AsyncWhatsAppCaseHandler
    from .queue_db import AsyncQueueDB


class WhatsAppAPIServer(FastAPI) :
    """
    FastAPI app with webhook routes and an in-process async queue worker.
    """
    
    def __init__(
        self,
        handler_cls  : Type["AsyncWhatsAppCaseHandler"],
        queue_db     : "AsyncQueueDB | None" = None,
        webhook_path : str                   = "/webhook",
        *,
        verify_app_secret : bool = False,
        **kwargs     : Any,
    ) -> None :
        """
        Initialize the WhatsApp API server. \\
        Args:
            handler_cls  : Case handler class invoked by the worker
            queue_db     : Optional AsyncQueueDB instance
            webhook_path : Webhook route path
            verify_app_secret : Whether to verify the payload signature against the
                                configured Meta App Secret
            kwargs       : Forwarded to FastAPI
        """
        from .queue_db import AsyncQueueDB
        from .queue_worker import AsyncQueueWorker
        
        self.queue_db     = queue_db or AsyncQueueDB()
        self.queue_worker = AsyncQueueWorker( self.queue_db, handler_cls)
        self.webhook_path = webhook_path
        self.worker_task  : asyncio.Task[None] | None = None
        
        self.verify_app_secret = verify_app_secret
        
        kwargs.setdefault( "lifespan", self.lifespan)
        super().__init__(**kwargs)
        self.register_routes()
        
        return
    
    @asynccontextmanager
    async def lifespan( self, _app : FastAPI) -> AsyncIterator[None] :
        """
        Open the async DB pool and run the queue worker for the app lifetime.
        """
        from .supabase import get_database_url
        
        logging.info("WhatsApp API server lifespan starting")
        
        await open_async_database_connection_pool(get_database_url())
        
        self.worker_task = asyncio.create_task(self.queue_worker.serve_forever())
        self.worker_task.add_done_callback(self._log_worker_task_result)
        
        try :
            yield
        
        finally :
            self.queue_worker.stop()
            if self.worker_task :
                self.worker_task.cancel()
                with suppress(asyncio.CancelledError) :
                    await self.worker_task
                self.worker_task = None
            
            await close_async_database_connection_pool()
            
            logging.info("WhatsApp API server lifespan stopped")
        
        return
    
    def _log_worker_task_result( self, task : asyncio.Task[None]) -> None :
        """
        Log unexpected background worker termination.
        """
        if task.cancelled() :
            return
        
        exc = task.exception()
        if exc :
            logging.error(
                f"In {here()}: Async queue worker task stopped with an exception",
                exc_info = ( type(exc), exc, exc.__traceback__ ),
            )
        else :
            logging.info("Async queue worker task stopped")
        
        return
    
    def register_routes(self) -> None :
        """
        Register health, verification, and webhook endpoints.
        """
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
            endpoint = self.debugz,
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
    
    async def debugz(self) -> JSONResponse :
        """
        Return masked webhook verification configuration.
        """
        expected = os.getenv( "WA_VERIFY_TOKEN", default = "")
        masked   = ("*"*(len(expected)-4) + expected[-4:] ) if expected else ""
        
        worker_task_exception = None
        if (
            self.worker_task and
            self.worker_task.done() and
            not self.worker_task.cancelled()
        ) :
            exc = self.worker_task.exception()
            worker_task_exception = str(exc) if exc else None
        
        return JSONResponse(
            content = {
                "verify_token_set"         : bool(expected),
                "verify_token_tail"        : masked,
                "verify_meta_app_secret"   : self.verify_app_secret,
                "worker_task_created"      : bool(self.worker_task),
                "worker_task_done"         : (
                    self.worker_task.done() if self.worker_task else None
                ),
                "worker_task_cancelled"    : (
                    self.worker_task.cancelled() if self.worker_task else None
                ),
                "worker_task_exception"    : worker_task_exception,
                "worker_stop_flag"         : self.queue_worker._stop_flag,
            },
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
            try :
                if not verify_app_secret( payload_bytes, signature) :
                    return JSONResponse(
                        content = {
                            "status" : "error",
                            "error"  : "Invalid signature",
                        },
                        status_code = status.HTTP_401_UNAUTHORIZED,
                    )
            except RuntimeError as ex :
                logging.error(
                    f"In {here()}: Unable to verify payload signature: {str(ex)}"
                )
                return JSONResponse(
                    content = {
                        "status" : "error",
                        "error"  : (
                            "Webhook signature verification is not configured"
                        ),
                    },
                    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
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
            enqueue_result = await self.queue_db.enqueue(data)
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
