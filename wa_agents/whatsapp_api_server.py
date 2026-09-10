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
from datetime import (
    UTC,
    datetime,
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

from sofia_utils.printing import (
    get_qualname as here,
    print_sep,
)
from sofia_utils.psycopg import (
    close_async_database_connection_pool,
    open_async_database_connection_pool,
)

from .whatsapp_functions import verify_app_secret
from .whatsapp_models import (
    WhatsAppContact,
    WhatsAppMessage,
    WhatsAppMessageEcho,
    WhatsAppPayload,
    WhatsAppValue,
)


if TYPE_CHECKING :
    from .queue_db import AsyncQueueDB


class WhatsAppAPIServer(FastAPI) :
    """
    FastAPI app with webhook routes and an in-process async queue worker.
    """
    
    def __init__(
        self,
        handler_cls  : Type[Any],
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

    @staticmethod
    def _message_data(
        message : WhatsAppMessage | WhatsAppMessageEcho,
    ) -> dict[str, Any] :
        """
        Return message-specific data stored outside normalized columns.
        """
        return message.model_dump(
            mode         = "json",
            by_alias     = True,
            exclude_none = True,
            exclude      = { "user", "user_id", "id", "timestamp", "type" },
        )

    @staticmethod
    def _value_contact( value : WhatsAppValue) -> WhatsAppContact :
        """
        Return the contact represented by one WhatsApp change value.
        """
        if value.contacts :
            return value.contacts[0]
        if value.messages :
            message = value.messages[0]
            return WhatsAppContact( wa_id = message.user, user_id = message.user_id)
        if value.message_echoes :
            message = value.message_echoes[0]
            return WhatsAppContact(
                wa_id   = message.to,
                user_id = message.to_user_id,
            )
        if value.statuses :
            message_status = value.statuses[0]
            return WhatsAppContact(
                wa_id   = message_status.recipient_id,
                user_id = message_status.recipient_user_id,
            )

        raise RuntimeError(f"In {here()}: WhatsApp value has no contact identity")

    async def _persist_valid_payload(
        self,
        payload_id : int,
        payload    : WhatsAppPayload,
    ) -> bool :
        """
        Normalize a validated payload and enqueue newly persisted messages.
        """
        storage  = self.queue_db.storage
        enqueued = False

        for item_idx, item in enumerate(payload.entry) :
            item_ts = (
                datetime.fromtimestamp( int(item.time), UTC)
                if item.time is not None else None
            )

            for change_idx, change in enumerate(item.changes) :
                value = change.value
                if not isinstance( value, WhatsAppValue) :
                    continue

                business = await storage.upsert_business(
                    waba_id              = item.id,
                    phone_number_id      = value.metadata.phone_number_id,
                    display_phone_number = value.metadata.display_phone_number,
                )
                if not business :
                    raise RuntimeError(f"In {here()}: Unable to persist business")

                contact_model = self._value_contact(value)
                contact       = await storage.upsert_contact(
                    business = business["id"],
                    wa_id    = contact_model.wa_id,
                    user_id  = contact_model.user_id,
                )
                if not contact :
                    raise RuntimeError(f"In {here()}: Unable to persist contact")

                if contact_model.profile :
                    profile = await storage.insert_contact_profile(
                        contact          = contact["id"],
                        profile_name     = contact_model.profile.name,
                        profile_username = contact_model.profile.username,
                    )
                    if not profile :
                        raise RuntimeError(
                            f"In {here()}: Unable to persist contact profile"
                        )

                metadata = await storage.insert_inbound_payload_metadata(
                    payload_id = payload_id,
                    contact    = contact["id"],
                    item_idx   = item_idx,
                    item_ts    = item_ts,
                    change_idx = change_idx,
                )
                if not metadata :
                    raise RuntimeError(
                        f"In {here()}: Unable to persist inbound payload metadata"
                    )

                messages = [ ( message, False) for message in value.messages ]
                messages.extend(
                    ( message, True) for message in value.message_echoes
                )
                for message, is_echo in messages :
                    message_row = await storage.insert_inbound_message(
                        payload  = metadata["id"],
                        is_echo  = is_echo,
                        msg_id   = message.id,
                        msg_ts   = datetime.fromtimestamp(
                            int(message.timestamp),
                            UTC,
                        ),
                        msg_type = message.type,
                        msg_data = self._message_data(message),
                    )
                    if message_row :
                        enqueued = await self.queue_db.enqueue(message.id) or enqueued

                for message_status in value.statuses :
                    status_row = await storage.insert_status(
                        payload      = metadata["id"],
                        msg_id       = message_status.id,
                        msg_status   = message_status.status,
                        status_ts    = datetime.fromtimestamp(
                            int(message_status.timestamp),
                            UTC,
                        ),
                        conversation = (
                            message_status.conversation.model_dump( mode = "json")
                            if message_status.conversation else None
                        ),
                        pricing = (
                            message_status.pricing.model_dump( mode = "json")
                            if message_status.pricing else None
                        ),
                        errors = (
                            [
                                error.model_dump( mode = "json")
                                for error in message_status.errors
                            ]
                            if message_status.errors else None
                        ),
                    )
                    if not status_row :
                        raise RuntimeError(
                            f"In {here()}: Unable to persist message status"
                        )

        return enqueued
    
    async def webhook( self, request : Request) -> JSONResponse :
        """
        Persist, validate, normalize, and enqueue a WhatsApp webhook payload.
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
        print( "Incoming:", data)

        try :
            raw_payload = await self.queue_db.storage.insert_inbound_payload(data)
            if not raw_payload or raw_payload.get("id") is None :
                raise RuntimeError(f"In {here()}: Unable to persist inbound payload")
        except Exception as ex :
            logging.error(
                f"In {here()}: Failed to store raw webhook payload: {str(ex)}"
            )
            return JSONResponse(
                content = {
                    "status" : "error",
                    "error"  : str(ex),
                    "stored" : False,
                },
                status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        payload_id = int(raw_payload["id"])
        stored     = bool(raw_payload.get("inserted"))
        
        try :
            payload = WhatsAppPayload.model_validate(data)
        except ValidationError as ve :
            logging.error(f"In {here()}: Malformed payload: {str(ve)}")
            errors = json.loads(ve.json(include_url = False))
            try :
                invalid = await self.queue_db.storage.mark_inbound_payload_invalid(
                    payload_id,
                    errors,
                )
                if not invalid :
                    raise RuntimeError(
                        f"In {here()}: Unable to persist payload validation errors"
                    )
            except Exception as ex :
                logging.error(
                    f"In {here()}: Failed to store validation errors: {str(ex)}"
                )
                return JSONResponse(
                    content = {
                        "status" : "error",
                        "error"  : str(ex),
                        "stored" : stored,
                    },
                    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            return JSONResponse(
                content = {
                    "status" : "error",
                    "error"  : f"Malformed payload: {ve}",
                    "stored" : stored,
                },
                status_code = status.HTTP_200_OK,
            )

        try :
            valid = await self.queue_db.storage.mark_inbound_payload_valid(payload_id)
            if not valid :
                raise RuntimeError(
                    f"In {here()}: Unable to mark inbound payload as valid"
                )
        except Exception as ex :
            logging.error(
                f"In {here()}: Failed to store validation result: {str(ex)}"
            )
            return JSONResponse(
                content = {
                    "status" : "error",
                    "error"  : str(ex),
                    "stored" : stored,
                },
                status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        if not stored :
            return JSONResponse(
                content = {
                    "status"   : "ok",
                    "enqueued" : False,
                    "stored"   : False,
                },
                status_code = status.HTTP_200_OK,
            )

        try :
            enqueue_result = await self._persist_valid_payload( payload_id, payload)
        except Exception as ex :
            logging.error(
                f"In {here()}: Failed to normalize webhook payload: {str(ex)}"
            )
            return JSONResponse(
                content = {
                    "status" : "error",
                    "error"  : str(ex),
                    "stored" : stored,
                },
                status_code = status.HTTP_200_OK,
            )
        
        response = {
            "status"   : "ok",
            "enqueued" : enqueue_result,
            "stored"   : stored,
        }

        return JSONResponse(
            content     = response,
            status_code = status.HTTP_200_OK,
        )
