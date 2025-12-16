#!/usr/bin/env python3
"""
Background worker that drains the incoming WhatsApp queue and runs CaseHandler.
"""

import gc
import logging
import os
import time

from traceback import format_exc
from typing import Type

from .basemodels import ( MediaContent,
                          WhatsAppContact,
                          WhatsAppMetaData,
                          WhatsAppPayload )
from .casehandlerbase import CaseHandlerBase
from .queue_db import QueueDB
from .whatsapp_functions import fetch_media


POLL_INTERVAL_BUSY = float( os.getenv( "QUEUE_POLL_INTERVAL_BUSY", 0.2))
POLL_INTERVAL_IDLE = float( os.getenv( "QUEUE_POLL_INTERVAL_IDLE", 1.0))
RESPONSE_DELAY     = float( os.getenv( "QUEUE_RESPONSE_DELAY",     1.0))


type HandlerJob = tuple[ WhatsAppMetaData, WhatsAppContact]

class JobTimeDict( dict[ HandlerJob, float] ) :
    
    def mark_as_done( self, job : HandlerJob) -> None :
        self.pop( job, 0.0)
        return
    
    def get_due_now(self) -> list[HandlerJob] :
        
        result = []
        for handlerjob, job_response_time in self.items() :
            if job_response_time < time.time() :
                result.append(handlerjob)
        
        return result


class QueueWorker :
    
    def __init__( self,
                  queue_db    : QueueDB,
                  handler_cls : Type[CaseHandlerBase]) -> None :
        
        self.queue       = queue_db
        self.handler_cls = handler_cls
        self._job_td     = JobTimeDict()
        self._stop_flag  = False
        
        return
    
    def stop( self, *_ : object) -> None :
        
        self._stop_flag = True
        
        return
    
    def serve_forever( self) -> None :
        
        logging.info( "Queue worker started, poll interval = %ss", POLL_INTERVAL_IDLE)
        
        while not self._stop_flag :
            
            received_payload  = self._process_payload()
            processed_jobs    = self._process_jobs(self._job_td.get_due_now())
            have_pending_jobs = bool(self._job_td)
            
            if ( received_payload or processed_jobs or have_pending_jobs ) :
                time.sleep(POLL_INTERVAL_BUSY)
            else :
                time.sleep(POLL_INTERVAL_IDLE)
        
        logging.info("Queue worker stopped")
        
        return
    
    def _process_payload( self) -> bool :
        
        item = self.queue.claim_next()
        if not item :
            return False
        
        row_id : str = item["row_id"]
        try :
            
            payload : WhatsAppPayload = item["payload"]
            
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
                            
                            respond = handler.process_msg_human( msg, media_content) \
                                      or respond
                        
                        if respond :
                            job = ( operator, user)
                            self._job_td[job] = time.time() + RESPONSE_DELAY
            
            self.queue.mark_done(row_id)
        
        except Exception as ex :
            logging.error( "Worker failed for payload with row id: %s\n"
                           "Exception raised: %s\n%s", row_id, ex, format_exc())
            self.queue.mark_error( row_id, str(ex))
        
        finally :
            gc.collect()
        
        return True
    
    def _process_jobs( self, jobs_to_process : list[HandlerJob]) -> bool :
        
        processed_jobs = False
        try :
            for ( operator, user) in jobs_to_process :
                handler  = self.handler_cls( operator, user)
                respond  = True
                while respond :
                    respond = handler.generate_response( debug = False)
                
                self._job_td.mark_as_done(( operator, user))
                processed_jobs = True
        
        except Exception as ex :
            job_batch = [ ( jtp[0].model_dump_json(),
                            jtp[1].model_dump_json()) for jtp in jobs_to_process ]
            logging.error( "Worker failed to generate response for job batch: %s\n"
                           "Exception raised: %s\n%s", job_batch, ex, format_exc())
        
        finally :
            gc.collect()
        
        return processed_jobs
