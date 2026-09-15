#!/usr/bin/env python3
"""
Example CaseHandler: single-turn chatbot with one LLM call.

Use this when each incoming user message should produce one model-generated reply.
"""

from uuid import UUID

from sofia_utils.printing import get_qualname as here

from wa_agents.agent import AsyncAgent
from wa_agents.case_handler_base import (
    AsyncCaseHandlerBase,
    WhatsAppDatabaseRecord_Business,
    WhatsAppDatabaseRecord_Contact,
)
from wa_agents.whatsapp_models import WhatsAppMessage


class CaseHandler (AsyncCaseHandlerBase) :
    """
    Single-turn LLM handler.
    """

    MAIN_AGENT_MODELS = [ "openai/gpt-5-mini" ]

    def __init__(
        self,
        operator : WhatsAppDatabaseRecord_Business,
        user     : WhatsAppDatabaseRecord_Contact,
        *,
        api_inbound_msg_id : int | None        = None,
        owner_token        : UUID | str | None = None,
        debug              : bool              = False,
        database_url       : str | None        = None,
    ) -> None :
        super().__init__(
            operator,
            user,
            api_inbound_msg_id = api_inbound_msg_id,
            owner_token        = owner_token,
            debug              = debug,
            database_url       = database_url,
        )
        self.main_agent : AsyncAgent | None = None
        return

    def setup_main_agent(self) -> None :
        self.main_agent = AsyncAgent( "main", self.MAIN_AGENT_MODELS )
        # Optional:
        # self.main_agent.load_prompts([ "prompts/main.md" ])
        return

    async def process_message(
        self,
        message       : WhatsAppMessage,
        media_content : bytes | None = None,
    ) -> bool :
        """
        Deduplicate + ingest and decide whether to respond.
        """
        msg = await self.dedup_and_ingest_message( message, media_content)
        if not msg :
            return False

        if message.type != "text" :
            return False

        return True

    async def generate_response(
        self,
        max_tokens : int | None = None,
    ) -> bool :
        """
        Generate one LLM reply and stop.
        """
        if not self.case_context :
            await self.context_build()

        if not self.main_agent :
            self.setup_main_agent()

        message = await self.main_agent.get_response(
            context    = self.case_context,
            origin     = here(),
            max_tokens = max_tokens,
            debug      = self.debug,
        )

        if not message or message.is_empty() :
            return False

        message.print()
        await self.send_text(message)
        await self.context_update(message)

        return False
