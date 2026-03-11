#!/usr/bin/env python3
"""
Example CaseHandler: single-turn chatbot with one LLM call.

Use this when each incoming user message should produce one model-generated reply.
"""

from inspect import currentframe

from wa_agents.agent import Agent
from wa_agents.basemodels import ( MediaContent,
                                   WhatsAppContact,
                                   WhatsAppMetaData,
                                   WhatsAppMsg )
from wa_agents.case_handler_base import CaseHandlerBase


class CaseHandler(CaseHandlerBase) :
    """
    Single-turn LLM handler.
    """

    MAIN_AGENT_MODELS = [ "openai/gpt-5-mini" ]

    def __init__( self,
                  operator : WhatsAppMetaData,
                  user     : WhatsAppContact,
                  debug    : bool = False ) -> None :
        super().__init__( operator, user, debug )
        self.main_agent : Agent | None = None
        return

    def setup_main_agent(self) -> None :
        self.main_agent = Agent( "main", self.MAIN_AGENT_MODELS )

        # Optional:
        # self.main_agent.load_prompts([ "prompts/main.md" ])

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
        Generate one LLM reply and stop.
        """
        _orig_ = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"

        if not self.case_context :
            self.context_build()

        if not self.main_agent :
            self.setup_main_agent()

        message = self.main_agent.get_response( context    = self.case_context,
                                                origin     = _orig_,
                                                max_tokens = max_tokens,
                                                debug      = self.debug )

        if not message or message.is_empty() :
            return False

        message.print()
        self.send_text(message)
        self.context_update(message)

        return False
