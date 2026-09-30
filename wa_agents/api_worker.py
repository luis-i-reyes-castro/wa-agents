#!/usr/bin/env python3
"""
FastAPI worker for normalized inbound WhatsApp messages.
"""

import asyncio
import logging
import os
import time

from collections.abc import Callable
from contextlib import (
    asynccontextmanager,
    suppress,
)
from dataclasses import dataclass
from datetime import datetime
from fastapi import (
    FastAPI,
    status,
)
from fastapi.responses import JSONResponse
from inspect import iscoroutinefunction
from pydantic import (
    TypeAdapter,
    ValidationError,
)
from traceback import format_exc
from typing import (
    Any,
    AsyncIterator,
    Type,
)
from uuid import UUID

from sofia_utils.printing import get_qualname as here
from sofia_utils.pydantic import NO_WS_str

from .database_queue import AsyncWhatsAppDatabaseQueue
from .io_functions import async_fetch_media
from .io_models import (
    WhatsApp_IB_Message,
    WhatsApp_IB_MessageEcho,
    WhatsApp_IB_Profile,
)
from .supabase import (
    WhatsAppDatabaseRecord_Business,
    WhatsAppDatabaseRecord_Contact,
)


POLL_INTERVAL_BUSY = float( os.getenv( "QUEUE_POLL_INTERVAL_BUSY", 0.2))
POLL_INTERVAL_IDLE = float( os.getenv( "QUEUE_POLL_INTERVAL_IDLE", 1.0))
RESPONSE_DELAY     = float( os.getenv( "QUEUE_RESPONSE_DELAY",     1.0))


@dataclass( frozen = True)
class HandlerJob :
    """
    Everything required to reconstruct a contact-bound handler.
    """
    handler_id         : int | None
    handler_key        : str
    operator           : WhatsAppDatabaseRecord_Business
    user               : WhatsAppDatabaseRecord_Contact
    api_inbound_msg_id : int
    owner_token        : UUID | str


class JobTimeDict ( dict[ HandlerJob, float] ) :
    """
    Delayed response times keyed by contact-bound jobs.
    """
    
    def get_due_now(self) -> list[HandlerJob] :
        now = time.time()
        return [ job for job, response_time in self.items() if response_time <= now ]
    
    def mark_as_done( self, job : HandlerJob) -> None :
        self.pop( job, None)
        return


def _message_timestamp( value : datetime | str) -> str :
    
    if isinstance( value, datetime) :
        return str(int(value.timestamp()))
    
    return str(value)


def _job_and_message(
    item : dict[str, Any],
) -> tuple[ HandlerJob, WhatsApp_IB_Message] :
    """
    Reconstruct handler inputs from `get_inbound_message.sql` output.
    """
    profile = (
        WhatsApp_IB_Profile(
            name     = item["profile_name"],
            username = item.get("profile_username"),
        )
        if item.get("profile_name") else None
    )
    operator = WhatsAppDatabaseRecord_Business(
        row_id               = item["business"],
        waba_id              = item["waba_id"],
        display_phone_number = item["display_phone_number"],
        phone_number_id      = item["phone_number_id"],
    )
    user = WhatsAppDatabaseRecord_Contact(
        row_id  = item["contact"],
        profile = profile,
        wa_id   = item.get("wa_id"),
        user_id = item.get("user_id"),
    )
    
    msg_data = dict(item["msg_data"])
    msg_data.update(
        {
            "from"         : item.get("wa_id"),
            "from_user_id" : item.get("user_id"),
            "id"           : item["msg_id"],
            "timestamp"    : _message_timestamp(item["msg_ts"]),
            "type"         : item["msg_type"],
        }
    )
    MsgBM   = WhatsApp_IB_MessageEcho if item["is_echo"] else WhatsApp_IB_Message
    message = MsgBM.model_validate(msg_data)
    
    job = HandlerJob(
        handler_id         = item.get("handler_id"),
        handler_key        = item["handler_key"],
        operator           = operator,
        user               = user,
        api_inbound_msg_id = item["id"],
        owner_token        = item["owner_token"],
    )
    
    return job, message


class WhatsAppAPIWorker (FastAPI) :
    """
    Asynchronous worker for persisted inbound messages.
    """
    
    def __init__(
        self,
        *,
        handler_cls     : Type[Any]                  | None = None,
        handler_classes : dict[ str, Type[Any]]      | None = None,
        queue           : AsyncWhatsAppDatabaseQueue | None = None,
        **kwargs        : Any,
    ) -> None :
        """
        Initialize the WhatsApp database queue worker app. \\
        Args:
            handler_cls     : Case handler class invoked by the worker
            queue           : Optional async WhatsApp database queue
            handler_classes : Case handler classes keyed by their `HANDLER_KEY`
            kwargs          : Forwarded to FastAPI
        """
        self._init_worker(
            queue           = queue or AsyncWhatsAppDatabaseQueue(),
            handler_cls     = handler_cls,
            handler_classes = handler_classes,
        )
        kwargs.setdefault( "lifespan", self.worker_lifespan)
        FastAPI.__init__( self, **kwargs)
        self.register_worker_routes( include_diagnostics = True)
        
        return
    
    def _build_handler_registry(
        self,
        handler_cls     : Type[Any]             | None,
        handler_classes : dict[ str, Type[Any]] | None,
    ) -> tuple[str, ...] :
        """
        Validate and store handler classes, then return their sorted keys.
        """
        if handler_cls and handler_classes :
            raise ValueError(
                f"In {here()}: Pass handler_cls or handler_classes, not both"
            )
        
        is_shorthand = handler_classes is None
        if handler_classes is not None :
            registry = dict(handler_classes)
            if not registry :
                raise ValueError(f"In {here()}: handler_classes must not be empty")
        else :
            if not handler_cls :
                raise ValueError(
                    f"In {here()}: A case handler class or registry is required"
                )
            handler_key = getattr( handler_cls, "HANDLER_KEY", "default")
            registry    = { handler_key : handler_cls }
        
        for handler_key, Handler in registry.items() :
            try :
                TypeAdapter(NO_WS_str).validate_python(handler_key)
            except ValidationError :
                raise ValueError(
                    f"In {here()}: Invalid handler registry key '{handler_key}'"
                )
            expected_handler_key = getattr(
                Handler,
                "HANDLER_KEY",
                "default" if is_shorthand else None,
            )
            if expected_handler_key != handler_key :
                raise ValueError(
                    f"In {here()}: Handler registry key '{handler_key}' does not "
                    f"match {Handler.__name__}.HANDLER_KEY"
                )
        
        self.handler_classes = registry
        if handler_cls :
            self.queue.fallback_handler_key = getattr(
                handler_cls,
                "HANDLER_KEY",
                "default",
            )
        
        return tuple( sorted(registry) )
    
    def _init_worker(
        self,
        *,
        queue           : AsyncWhatsAppDatabaseQueue,
        handler_cls     : Type[Any]             | None,
        handler_classes : dict[ str, Type[Any]] | None,
    ) -> None :
        """
        Initialize worker state without initializing FastAPI.
        """
        self.queue        = queue
        self.handler_keys = self._build_handler_registry(
            handler_cls,
            handler_classes,
        )
        self.worker_task : asyncio.Task[None] | None = None
        
        self._job_td    = JobTimeDict()
        self._stop_flag = False
        
        return
    
    @asynccontextmanager
    async def worker_lifespan( self, _app : FastAPI) -> AsyncIterator[None] :
        """
        Open the database pool and run the queue worker for the app lifetime.
        """
        logging.info(
            "WhatsApp database queue worker lifespan starting, handler keys = %s",
            ", ".join(self.handler_keys),
        )
        
        async with self.queue.connection_pool() :
            self._stop_flag  = False
            self.worker_task = asyncio.create_task(self.serve_forever())
            self.worker_task.add_done_callback(self._log_worker_task_result)
            
            try :
                yield
            
            finally :
                self.stop()
                if self.worker_task :
                    self.worker_task.cancel()
                    with suppress(asyncio.CancelledError) :
                        await self.worker_task
                    self.worker_task = None
        
        logging.info("WhatsApp database queue worker lifespan stopped")
        
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
    
    def register_worker_routes( self, *, include_diagnostics : bool) -> None :
        """
        Register standalone worker diagnostics.
        """
        if not include_diagnostics :
            return
        
        self.add_api_route(
            path     = "/",
            endpoint = self.worker_root,
            methods  = ["GET"],
        )
        self.add_api_route(
            path     = "/healthz",
            endpoint = self.worker_healthz,
            methods  = ["GET"],
        )
        self.add_api_route(
            path     = "/debugz",
            endpoint = self.worker_debugz,
            methods  = ["GET"],
        )
        
        return
    
    async def worker_root(self) -> JSONResponse :
        """
        Root diagnostic endpoint.
        """
        return JSONResponse(
            content     = { "root" : "OK" },
            status_code = status.HTTP_200_OK,
        )
    
    async def worker_healthz(self) -> JSONResponse :
        """
        Health endpoint.
        """
        return JSONResponse(
            content     = { "healthy" : True },
            status_code = status.HTTP_200_OK,
        )
    
    def _worker_debug_data(self) -> dict[str, Any] :
        """
        Return worker task diagnostics.
        """
        worker_task_exception = None
        if (
            self.worker_task and
            self.worker_task.done() and
            not self.worker_task.cancelled()
        ) :
            exc = self.worker_task.exception()
            worker_task_exception = str(exc) if exc else None
        
        return {
            "worker_handler_keys"   : self.handler_keys,
            "worker_task_created"   : bool(self.worker_task),
            "worker_task_done"      : (
                self.worker_task.done() if self.worker_task else None
            ),
            "worker_task_cancelled" : (
                self.worker_task.cancelled() if self.worker_task else None
            ),
            "worker_task_exception" : worker_task_exception,
            "worker_stop_flag"      : self._stop_flag,
        }
    
    async def worker_debugz(self) -> JSONResponse :
        """
        Return worker diagnostics.
        """
        return JSONResponse(
            content     = self._worker_debug_data(),
            status_code = status.HTTP_200_OK,
        )
    
    def _handler( self, job : HandlerJob) -> Any :
        
        Handler = self.handler_classes[job.handler_key]
        return Handler(
            job.operator,
            job.user,
            api_inbound_msg_id = job.api_inbound_msg_id,
            handler_id         = job.handler_id,
            owner_token        = job.owner_token,
        )
    
    def stop( self, *_ : object) -> None :
        
        self._stop_flag = True
        
        return
    
    async def serve_forever(self) -> None :
        
        logging.info(
            "Async queue worker started, poll interval = %ss",
            POLL_INTERVAL_IDLE,
        )
        
        while not self._stop_flag :
            try :
                active = await self.tick()
            except asyncio.CancelledError :
                raise
            except Exception :
                logging.error(
                    f"In {here()}: Async queue worker tick failed\n"
                    f"Exception trace: {format_exc()}"
                )
                active = False
            
            await asyncio.sleep(
                POLL_INTERVAL_BUSY if active else POLL_INTERVAL_IDLE
            )
        
        logging.info("Async queue worker stopped")
        
        return
    
    async def tick(self) -> bool :
        
        received_message = await self._process_message()
        processed_jobs   = await self._process_jobs(self._job_td.get_due_now())
        
        return received_message or processed_jobs or bool(self._job_td)
    
    async def _call_handler_method(
        self,
        method : Callable[..., Any],
        *args  : object,
    ) -> object :
        
        if iscoroutinefunction(method) :
            return await method(*args)
        
        return await asyncio.to_thread( method, *args)
    
    async def _process_message(self) -> bool :
        
        item = await self.queue.claim_next(self.handler_keys)
        if not item :
            return False
        
        row_id    = item["row_id"]
        handler   = None
        keep_lease = False
        
        try :
            job, message = _job_and_message(item)
            handler      = self._handler(job)
            
            media_content = (
                await async_fetch_media(message.media_data)
                if message.media_data else None
            )
            respond = await self._call_handler_method(
                handler.process_message,
                message,
                media_content,
            )
            
            if getattr( handler, "_responded_in_ingest", False) :
                self._job_td.mark_as_done(job)
                respond = False
            
            await self.queue.mark_done(row_id)
            
            if respond :
                self._job_td[job] = time.time() + RESPONSE_DELAY
                keep_lease        = True
        
        except Exception as ex :
            logging.error(
                f"In {here()}: Worker failed for queue row {row_id}: {str(ex)}\n"
                f"Exception trace: {format_exc()}"
            )
            await self.queue.mark_error(row_id)
        
        finally :
            if not keep_lease :
                if handler :
                    await self._call_handler_method(handler.release_contact_lease)
                else :
                    await self.queue.release_contact_lease(
                        item["contact"],
                        item["owner_token"],
                    )
        
        return True
    
    async def _process_jobs( self, jobs_to_process : list[HandlerJob]) -> bool :
        
        processed_jobs = False
        
        for job in jobs_to_process :
            
            handler = self._handler(job)
            
            try :
                renewed = await self._call_handler_method(
                    handler.renew_contact_lease
                )
                if not renewed :
                    logging.warning(
                        "Contact lease expired before response for contact %s",
                        job.user.row_id,
                    )
                    continue
                
                run_again = True
                while run_again :
                    run_again = bool(
                        await self._call_handler_method(handler.run_while_in_action)
                    )
                    if run_again :
                        renewed = await self._call_handler_method(
                            handler.renew_contact_lease
                        )
                        if not renewed :
                            raise RuntimeError(
                                f"In {here()}: Contact lease expired during response"
                            )
                
                processed_jobs = True
            
            except Exception as ex :
                logging.error(
                    f"In {here()}: Response failed for contact "
                    f"{job.user.row_id}: {str(ex)}\n"
                    f"Exception trace: {format_exc()}"
                )
            
            finally :
                await self._call_handler_method(handler.release_contact_lease)
                self._job_td.mark_as_done(job)
        
        return processed_jobs
