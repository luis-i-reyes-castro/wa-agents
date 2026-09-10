#!/usr/bin/env python3
"""
Background workers for normalized inbound WhatsApp messages.
"""

import asyncio
import logging
import os
import time

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from inspect import iscoroutinefunction
from traceback import format_exc
from typing import (
    Any,
    Type,
)
from uuid import UUID

from .case_handler_base import (
    AsyncCaseHandlerBase,
    CaseHandlerBase,
)
from .queue_db import (
    AsyncQueueDB,
    QueueDB,
)
from .whatsapp_functions import (
    async_fetch_media,
    fetch_media,
)
from .whatsapp_models import (
    WhatsAppContact,
    WhatsAppMessage,
    WhatsAppMessageEcho,
    WhatsAppMetaData,
    WhatsAppProfile,
)


POLL_INTERVAL_BUSY = float( os.getenv( "QUEUE_POLL_INTERVAL_BUSY", 0.2))
POLL_INTERVAL_IDLE = float( os.getenv( "QUEUE_POLL_INTERVAL_IDLE", 1.0))
RESPONSE_DELAY     = float( os.getenv( "QUEUE_RESPONSE_DELAY",     1.0))


@dataclass( frozen = True)
class HandlerJob :
    """
    Everything required to reconstruct a contact-bound handler.
    """
    business_id        : int
    contact_id         : int
    api_inbound_msg_id : int
    owner_token        : UUID | str
    operator           : WhatsAppMetaData
    user               : WhatsAppContact


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
) -> tuple[ HandlerJob, WhatsAppMessage] :
    """
    Reconstruct handler inputs from `get_inbound_message.sql` output.
    """
    profile = (
        WhatsAppProfile(
            name     = item["profile_name"],
            username = item.get("profile_username"),
        )
        if item.get("profile_name") else None
    )
    operator = WhatsAppMetaData(
        display_phone_number = item["display_phone_number"],
        phone_number_id      = item["phone_number_id"],
    )
    user = WhatsAppContact(
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
    MsgBM   = WhatsAppMessageEcho if item["is_echo"] else WhatsAppMessage
    message = MsgBM.model_validate(msg_data)
    
    job = HandlerJob(
        business_id        = item["business"],
        contact_id         = item["contact"],
        api_inbound_msg_id = item["id"],
        owner_token        = item["owner_token"],
        operator           = operator,
        user               = user,
    )
    
    return job, message


# =========================================================================================
# SYNC QUEUE WORKER

class QueueWorker :
    """
    Sequential worker for persisted inbound messages.
    """
    
    def __init__(
        self,
        queue_db    : QueueDB,
        handler_cls : Type[CaseHandlerBase],
    ) -> None :
        
        self.queue       = queue_db
        self.handler_cls = handler_cls
        self._job_td     = JobTimeDict()
        self._stop_flag  = False
        
        return
    
    def _handler( self, job : HandlerJob) -> CaseHandlerBase :
        
        return self.handler_cls(
            job.operator,
            job.user,
            business_id        = job.business_id,
            contact_id         = job.contact_id,
            api_inbound_msg_id = job.api_inbound_msg_id,
            owner_token        = job.owner_token,
        )
    
    def stop( self, *_ : object) -> None :
        
        self._stop_flag = True
        
        return
    
    def serve_forever(self) -> None :
        
        logging.info(
            "Queue worker started, poll interval = %ss",
            POLL_INTERVAL_IDLE,
        )
        
        while not self._stop_flag :
            time.sleep(POLL_INTERVAL_BUSY if self.tick() else POLL_INTERVAL_IDLE)
        
        logging.info("Queue worker stopped")
        
        return
    
    def tick(self) -> bool :
        
        received_message = self._process_message()
        processed_jobs   = self._process_jobs(self._job_td.get_due_now())
        
        return received_message or processed_jobs or bool(self._job_td)
    
    def _process_message(self) -> bool :
        
        item = self.queue.claim_next()
        if not item :
            return False
        
        row_id  = item["row_id"]
        handler = None
        keep_lease = False
        
        try :
            job, message = _job_and_message(item)
            handler      = self._handler(job)
            
            media_content = (
                fetch_media(message.media_data)
                if message.media_data else None
            )
            respond = handler.process_message( message, media_content)
            
            if getattr( handler, "_responded_in_ingest", False) :
                self._job_td.mark_as_done(job)
                respond = False
            
            self.queue.mark_done(row_id)
            
            if respond :
                self._job_td[job] = time.time() + RESPONSE_DELAY
                keep_lease        = True
        
        except Exception as ex :
            logging.error(
                f"Worker failed for queue row {row_id}: {str(ex)}\n"
                f"Exception trace: {format_exc()}"
            )
            self.queue.mark_error(row_id)
        
        finally :
            if not keep_lease :
                if handler :
                    handler.release_contact_lease()
                else :
                    self.queue.release_contact_lease(
                        item["contact"],
                        item["owner_token"],
                    )
        
        return True
    
    def _process_jobs( self, jobs_to_process : list[HandlerJob]) -> bool :
        
        processed_jobs = False
        
        for job in jobs_to_process :
            
            handler = self._handler(job)
            
            try :
                if not handler.renew_contact_lease() :
                    logging.warning(
                        "Contact lease expired before response for contact %s",
                        job.contact_id,
                    )
                    continue
                
                respond = True
                while respond :
                    respond = handler.generate_response()
                    if respond and not handler.renew_contact_lease() :
                        raise RuntimeError("Contact lease expired during response")
                
                processed_jobs = True
            
            except Exception as ex :
                logging.error(
                    f"Response failed for contact {job.contact_id}: {str(ex)}\n"
                    f"Exception trace: {format_exc()}"
                )
            
            finally :
                handler.release_contact_lease()
                self._job_td.mark_as_done(job)
        
        return processed_jobs


# =========================================================================================
# ASYNC QUEUE WORKER

class AsyncQueueWorker :
    """
    Asynchronous worker for persisted inbound messages.
    """
    
    def __init__(
        self,
        queue_db    : AsyncQueueDB,
        handler_cls : Type[AsyncCaseHandlerBase],
    ) -> None :
        
        self.queue       = queue_db
        self.handler_cls = handler_cls
        self._job_td     = JobTimeDict()
        self._stop_flag  = False
        
        return
    
    def _handler( self, job : HandlerJob) -> AsyncCaseHandlerBase :
        
        return self.handler_cls(
            job.operator,
            job.user,
            business_id        = job.business_id,
            contact_id         = job.contact_id,
            api_inbound_msg_id = job.api_inbound_msg_id,
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
                    "Async queue worker tick failed\n"
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
        
        item = await self.queue.claim_next()
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
                f"Worker failed for queue row {row_id}: {str(ex)}\n"
                f"Exception trace: {format_exc()}"
            )
            await self.queue.mark_error(row_id)
        
        finally :
            if not keep_lease :
                if handler :
                    await handler.release_contact_lease()
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
                        job.contact_id,
                    )
                    continue
                
                respond = True
                while respond :
                    respond = bool(
                        await self._call_handler_method(handler.generate_response)
                    )
                    if respond :
                        renewed = await self._call_handler_method(
                            handler.renew_contact_lease
                        )
                        if not renewed :
                            raise RuntimeError("Contact lease expired during response")
                
                processed_jobs = True
            
            except Exception as ex :
                logging.error(
                    f"Response failed for contact {job.contact_id}: {str(ex)}\n"
                    f"Exception trace: {format_exc()}"
                )
            
            finally :
                await self._call_handler_method(handler.release_contact_lease)
                self._job_td.mark_as_done(job)
        
        return processed_jobs
