#!/usr/bin/env python3
"""
Background worker that drains the incoming WhatsApp queue and runs CaseHandler.
"""

import asyncio
import gc
import logging
import os
import time

from inspect import iscoroutinefunction
from traceback import format_exc
from typing import Type

from .basemodels import ( MediaContent,
                          WhatsAppContact,
                          WhatsAppMetaData,
                          WhatsAppPayload )
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


POLL_INTERVAL_BUSY = float( os.getenv( "QUEUE_POLL_INTERVAL_BUSY", 0.2))
POLL_INTERVAL_IDLE = float( os.getenv( "QUEUE_POLL_INTERVAL_IDLE", 1.0))
RESPONSE_DELAY     = float( os.getenv( "QUEUE_RESPONSE_DELAY",     1.0))


type HandlerJob = tuple[ WhatsAppMetaData, WhatsAppContact]

class JobTimeDict( dict[ HandlerJob, float] ) :
    
    def get_due_now(self) -> list[HandlerJob] :
        """
        Return jobs scheduled for immediate response generation \\
        Returns:
            List of handler jobs whose response time has elapsed
        """
        result = []
        for handlerjob, job_response_time in self.items() :
            if job_response_time < time.time() :
                result.append(handlerjob)
        
        return result
    
    def mark_as_done( self, job : HandlerJob) -> None :
        """
        Remove a job entry once its delayed response completes \\
        Args:
            job : Tuple of ( operator metadata, contact)
        """
        self.pop( job, 0.0)
        return


class QueueWorker :
    """
    Background worker that drains QueueDB and runs CaseHandler instances
    """
    
    def __init__( self,
                  queue_db    : QueueDB,
                  handler_cls : Type[CaseHandlerBase]) -> None :
        """
        Configure the worker with its queue database and handler class \\
        Args:
            queue_db    : QueueDB instance
            handler_cls : CaseHandlerBase subclass invoked per user/operator
        """
        self.queue       = queue_db
        self.handler_cls = handler_cls
        self._job_td     = JobTimeDict()
        self._stop_flag  = False
        
        return
    
    def stop( self, *_ : object) -> None :
        """
        Signal the worker loop to exit gracefully
        """
        self._stop_flag = True
        
        return
    
    def serve_forever( self) -> None :
        """
        Poll the queue database and schedule response jobs indefinitely
        """
        logging.info(
            "Queue worker started, poll interval = %ss",
            POLL_INTERVAL_IDLE,
        )
        
        while not self._stop_flag :
            if self.tick() :
                time.sleep(POLL_INTERVAL_BUSY)
            else :
                time.sleep(POLL_INTERVAL_IDLE)
        
        logging.info("Queue worker stopped")
        
        return
    
    def tick(self) -> bool :
        """
        Run one queue worker iteration \\
        Returns:
            True when the worker is active or has pending delayed jobs;
            False when the queue is idle.
        """
        received_payload  = self._process_payload()
        processed_jobs    = self._process_jobs(self._job_td.get_due_now())
        have_pending_jobs = bool(self._job_td)
        
        return received_payload or processed_jobs or have_pending_jobs
    
    def _process_payload( self) -> bool :
        """
        Claim and process a single queue payload \\
        Returns:
            True if a payload was processed; False if queue empty.
        """
        item = self.queue.claim_next()
        if not item :
            return False
        
        row_id : str = item["row_id"]
        try :
            payload : WhatsAppPayload = item["payload"]
            if not payload.has_messages() :
                self.queue.mark_done(row_id)
                return True
            
            for wa_changes in payload.entry :
                for wa_change_ in wa_changes.changes :
                    
                    value    = wa_change_.value
                    operator = value.metadata
                    
                    user_msgs = { user.wa_id : [] for user in value.contacts }
                    for msg in value.messages :
                        user_msgs[msg.user].append(msg)
                    
                    for user in value.contacts :
                        
                        handler = self.handler_cls( operator, user)
                        respond = False
                        
                        for msg in user_msgs.get( user.wa_id, []) :
                            
                            media_content = None
                            if msg.media_data :
                                mime_type     = msg.media_data.mime_type
                                media_bytes   = fetch_media(msg.media_data)
                                media_content = MediaContent( mime    = mime_type,
                                                              content = media_bytes)
                            
                            respond = handler.process_message( msg, media_content) \
                                      or respond
                        
                        if respond :
                            job = ( operator, user)
                            self._job_td[job] = time.time() + RESPONSE_DELAY
            
            self.queue.mark_done(row_id)
        
        except Exception as ex :
            logging.error(
                f"Worker failed for payload with row id: {row_id}\n"
                f"Exception raised: {ex}\n"
                f"Exception trace: {format_exc()}"
            )
            self.queue.mark_error( row_id, str(ex))
        
        finally :
            gc.collect()
        
        return True
    
    def _process_jobs( self, jobs_to_process : list[HandlerJob]) -> bool :
        """
        Run delayed assistant responses for the provided jobs \\
        Args:
            jobs_to_process : Jobs ready to generate responses
        Returns:
            True if at least one job finished; else False.
        """
        processed_jobs = False
        try :
            for ( operator, user) in jobs_to_process :
                
                handler  = self.handler_cls( operator, user)
                respond  = True
                while respond :
                    respond = handler.generate_response()
                
                self._job_td.mark_as_done(( operator, user))
                processed_jobs = True
        
        except Exception as ex :
            job_batch = [ ( jtp[0].model_dump_json(),
                            jtp[1].model_dump_json()) for jtp in jobs_to_process ]
            logging.error(
                f"Worker failed to for job batch: {job_batch}\n"
                f"Exception raised: {ex}\n"
                f"Exception trace: {format_exc()}"
            )
        
        finally :
            gc.collect()
        
        return processed_jobs


class AsyncQueueWorker :
    """
    Async background worker that drains AsyncQueueDB and runs CaseHandler instances
    """
    
    def __init__( self,
                  queue_db    : AsyncQueueDB,
                  handler_cls : Type[AsyncCaseHandlerBase]) -> None :
        """
        Configure the worker with its queue database and handler class \\
        Args:
            queue_db    : AsyncQueueDB instance
            handler_cls : AsyncCaseHandlerBase subclass invoked per user/operator
        """
        self.queue       = queue_db
        self.handler_cls = handler_cls
        self._job_td     = JobTimeDict()
        self._stop_flag  = False
        
        return
    
    def stop( self, *_ : object) -> None :
        """
        Signal the worker loop to exit gracefully
        """
        self._stop_flag = True
        
        return

    async def serve_forever( self) -> None :
        """
        Poll the queue database and schedule response jobs indefinitely
        """
        logging.info(
            "Async queue worker started, poll interval = %ss",
            POLL_INTERVAL_IDLE,
        )
        
        while not self._stop_flag :
            if await self.tick() :
                await asyncio.sleep(POLL_INTERVAL_BUSY)
            else :
                await asyncio.sleep(POLL_INTERVAL_IDLE)
        
        logging.info("Async queue worker stopped")
        
        return
    
    async def tick(self) -> bool :
        """
        Run one queue worker iteration \\
        Returns:
            True when the worker is active or has pending delayed jobs;
            False when the queue is idle.
        """
        received_payload  = await self._process_payload()
        processed_jobs    = await self._process_jobs(self._job_td.get_due_now())
        have_pending_jobs = bool(self._job_td)
        
        return received_payload or processed_jobs or have_pending_jobs
    
    async def _call_handler_method(
        self,
        method : object,
        *args  : object,
    ) -> object :
        """
        Run a handler method without blocking the event loop
        """
        if iscoroutinefunction(method) :
            return await method(*args)
        
        return await asyncio.to_thread( method, *args)
    
    async def _process_payload(self) -> bool :
        """
        Claim and process a single queue payload \\
        Returns:
            True if a payload was processed; False if queue empty.
        """
        item = await self.queue.claim_next()
        if not item :
            return False
        
        row_id : int = item["row_id"]
        try :
            payload : WhatsAppPayload = item["payload"]
            if not payload.has_messages() :
                await self.queue.mark_done(row_id)
                return True
            
            for wa_changes in payload.entry :
                for wa_change_ in wa_changes.changes :
                    
                    value    = wa_change_.value
                    operator = value.metadata
                    
                    user_msgs = { user.wa_id : [] for user in value.contacts }
                    for msg in value.messages :
                        user_msgs[msg.user].append(msg)
                    
                    for user in value.contacts :
                        
                        handler = self.handler_cls( operator, user)
                        respond = False
                        
                        for msg in user_msgs.get( user.wa_id, []) :
                            
                            media_content = None
                            if msg.media_data :
                                mime_type     = msg.media_data.mime_type
                                media_bytes   = await async_fetch_media(msg.media_data)
                                media_content = MediaContent( mime    = mime_type,
                                                              content = media_bytes)
                            
                            respond = await self._call_handler_method(
                                handler.process_message,
                                msg,
                                media_content,
                            ) or respond
                        
                        if respond :
                            job = ( operator, user)
                            self._job_td[job] = time.time() + RESPONSE_DELAY
            
            await self.queue.mark_done(row_id)
        
        except Exception as ex :
            logging.error(
                f"Worker failed for payload with row id: {row_id}\n"
                f"Exception raised: {ex}\n"
                f"Exception trace: {format_exc()}"
            )
            await self.queue.mark_error( row_id, str(ex))
        
        finally :
            gc.collect()
        
        return True
    
    async def _process_jobs( self, jobs_to_process : list[HandlerJob]) -> bool :
        """
        Run delayed assistant responses for the provided jobs \\
        Args:
            jobs_to_process : Jobs ready to generate responses
        Returns:
            True if at least one job finished; else False.
        """
        processed_jobs = False
        try :
            for ( operator, user) in jobs_to_process :
                handler = self.handler_cls( operator, user)
                respond = True
                while respond :
                    respond = await self._call_handler_method(
                        handler.generate_response
                    )
                
                self._job_td.mark_as_done(( operator, user))
                processed_jobs = True
        
        except Exception as ex :
            job_batch = [ ( jtp[0].model_dump_json(),
                            jtp[1].model_dump_json()) for jtp in jobs_to_process ]
            logging.error(
                f"Worker failed to for job batch: {job_batch}\n"
                f"Exception raised: {ex}\n"
                f"Exception trace: {format_exc()}"
            )
        
        finally :
            gc.collect()
        
        return processed_jobs
