#!/usr/bin/env python3
"""
Example CaseHandler: single-turn chatbot without LLM or tools.

Use this when replies are deterministic (lookups, rules, fixed templates).
"""

from inspect import currentframe

from wa_agents.basemodels import ( MediaContent,
                                   ServerTextMsg,
                                   UserContentMsg,
                                   WhatsAppContact,
                                   WhatsAppMetaData,
                                   WhatsAppMsg )
from wa_agents.case_handler_base import CaseHandlerBase


class CaseHandler(CaseHandlerBase) :
    """
    Deterministic single-turn handler.
    """

    def __init__( self,
                  operator : WhatsAppMetaData,
                  user     : WhatsAppContact,
                  debug    : bool = False ) -> None :
        super().__init__( operator, user, debug )
        return

    def process_message( self,
                         message       : WhatsAppMsg,
                         media_content : MediaContent | None = None ) -> bool :
        """
        Deduplicate + ingest and decide whether to respond.
        """
        msg = self.dedup_and_ingest_message( message, media_content )
        if not msg :
            return False

        if message.type != "text" :
            return False

        return True

    def generate_response( self,
                           max_tokens : int | None = None ) -> bool :
        """
        Generate one deterministic reply and stop.
        """
        _orig_ = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"

        if not self.case_context :
            self.context_build()

        msg_user = None
        for msg in reversed(self.case_context) :
            if isinstance( msg, UserContentMsg ) and msg.text :
                msg_user = msg
                break

        if not msg_user :
            return False

        text_norm = msg_user.text.strip().lower()
        if text_norm in ( "close", "done", "resolved" ) :
            reply = "Case closed. Reply again anytime to open a new case."
            self.case_mark_as_resolved()
        elif "status" in text_norm :
            reply = "Your request is in progress."
        else :
            reply = "Received. We will review your message and reply shortly."

        msg_reply = ServerTextMsg( origin = _orig_,
                                   text   = reply )
        msg_reply.print()
        self.send_text(msg_reply)
        self.context_update(msg_reply)

        return False
