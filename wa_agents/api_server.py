"""
Combined WhatsApp webhook listener and database queue worker app.
"""

from __future__ import annotations

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
        handler_cls     : Type["AsyncWhatsAppCaseHandler"] | None = None,
        handler_classes : dict[
            str,
            Type["AsyncWhatsAppCaseHandler"],
        ] | None = None,
        queue             : AsyncWhatsAppDatabaseQueue | None = None,
        verify_app_secret : bool = False,
        webhook_path      : str  = "/webhook",
        **kwargs          : Any,
    ) -> None :
        """
        Initialize the combined WhatsApp API server. \\
        Args:
            handler_cls       : Case handler class invoked by the worker
            handler_classes   : Case handler classes keyed by their `HANDLER_KEY`
            queue             : Optional async WhatsApp database queue
            webhook_path      : Webhook route path
            verify_app_secret : Whether to verify payload signatures
            kwargs            : Forwarded to FastAPI
        """
        queue = queue or AsyncWhatsAppDatabaseQueue()
        self._init_listener(
            queue            = queue,
            verify_signature = verify_app_secret,
            webhook_path     = webhook_path,
        )
        self._init_worker(
            queue              = queue,
            handler_cls        = handler_cls,
            handler_classes    = handler_classes,
            flow_endpoint_path = f"{webhook_path}/flows/{{waba_id}}",
        )
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
