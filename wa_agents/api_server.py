"""
Combined WhatsApp webhook listener and database queue worker app.
"""

from __future__ import annotations

from datetime import timedelta
from fastapi import (
    FastAPI,
    status,
)
from fastapi.responses import JSONResponse
from typing import (
    Any,
    TYPE_CHECKING,
    Type,
)

from .api_listener import WhatsAppAPIListener
from .api_worker import WhatsAppAPIWorker
from .database_queue import AsyncWhatsAppDatabaseQueue

if TYPE_CHECKING :
    from .case_handler_base import AsyncWhatsAppCaseHandler


class WhatsAppAPIServer (
    WhatsAppAPIListener,
    WhatsAppAPIWorker,
) :
    """
    FastAPI app with WhatsApp webhook routes and an in-process queue worker.
    """
    
    def __init__(
        self,
        *,
        debug           : bool = False,
        handler_cls     : Type["AsyncWhatsAppCaseHandler"] | None = None,
        handler_classes : dict[
            str,
            Type["AsyncWhatsAppCaseHandler"],
        ] | None = None,
        poll_interval_busy : float | None = None,
        poll_interval_idle : float | None = None,
        queue              : AsyncWhatsAppDatabaseQueue | None = None,
        response_delay     : float | None = None,
        silence_timeout    : timedelta | None = None,
        verify_app_secret  : bool = False,
        webhook_path       : str  = "/webhook",
        **kwargs           : Any,
    ) -> None :
        """
        Initialize the combined WhatsApp API server. \\
        Args:
            debug              : Whether to emit debug output
            handler_cls        : Case handler class invoked by the worker
            handler_classes    : Case handler classes keyed by their `HANDLER_KEY`
            poll_interval_busy : Poll interval while the worker has pending jobs
            poll_interval_idle : Poll interval while the worker is idle
            queue              : Optional async WhatsApp database queue
            response_delay     : Quiet period after the latest message before running
                                 `run_while_in_action`
            silence_timeout    : Fallback echo silence timeout
            webhook_path       : Webhook route path
            verify_app_secret  : Whether to verify payload signatures
            kwargs             : Forwarded to FastAPI
        """
        queue = queue or AsyncWhatsAppDatabaseQueue()
        self._init_listener(
            debug            = debug,
            queue            = queue,
            verify_signature = verify_app_secret,
            webhook_path     = webhook_path,
        )
        self._init_worker(
            debug               = debug,
            flow_endpoint_path  = f"{webhook_path}/flows/{{waba_id}}",
            handler_cls         = handler_cls,
            handler_classes     = handler_classes,
            poll_interval_busy  = poll_interval_busy,
            poll_interval_idle  = poll_interval_idle,
            queue               = queue,
            response_delay      = response_delay,
            silence_timeout     = silence_timeout,
        )
        kwargs.setdefault( "debug", debug)
        kwargs.setdefault( "lifespan", self.worker_lifespan)
        FastAPI.__init__( self, **kwargs)
        self.register_listener_routes( include_diagnostics = False)
        self.register_worker_routes( include_diagnostics = False)
        self.register_server_diagnostic_routes()
        
        return
    
    def register_server_diagnostic_routes(self) -> None :
        """
        Register combined listener and worker diagnostics.
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
        
        return
    
    async def debugz(self) -> JSONResponse :
        """
        Return combined listener and worker diagnostics.
        """
        return JSONResponse(
            content = {
                **self._listener_debug_data(),
                **self._worker_debug_data(),
            },
            status_code = status.HTTP_200_OK,
        )
