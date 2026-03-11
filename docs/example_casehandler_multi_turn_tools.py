#!/usr/bin/env python3
"""
Example CaseHandler: multi-turn chatbot with tool-call loop.

This mirrors the core loop shape used in production:
assistant response -> tool execution -> tool results -> assistant response.
"""

from inspect import currentframe
from pathlib import Path

from wa_agents.agent import Agent
from wa_agents.basemodels import ( MediaContent,
                                   ToolCall,
                                   ToolResult,
                                   ToolResultsMsg,
                                   WhatsAppContact,
                                   WhatsAppMetaData,
                                   WhatsAppMsg )
from wa_agents.case_handler_base import CaseHandlerBase


class ToolServer :
    """
    Minimal example tool server.
    """

    def process( self, tool_calls : list[ToolCall] ) -> list[ToolResult] :

        results = []

        for tool_call in tool_calls :

            name    = tool_call.name
            payload = tool_call.input or {}
            error   = False
            content = None

            if name == "get_business_hours" :
                content = "Mon-Fri 08:00-17:00"

            elif name == "mark_as_resolved" :
                content = "Case marked as resolved."

            else :
                error   = True
                content = f"Unknown tool '{name}'"

            results.append( ToolResult( id      = tool_call.id,
                                        error   = error,
                                        content = content ) )

        return results


class CaseHandler(CaseHandlerBase) :
    """
    Multi-turn tool-loop handler.
    """

    MAIN_AGENT_MODELS = [ "openai/gpt-5-mini" ]

    def __init__( self,
                  operator : WhatsAppMetaData,
                  user     : WhatsAppContact,
                  debug    : bool = False ) -> None :
        super().__init__( operator, user, debug )
        self.main_agent  : Agent | None = None
        self.tool_server = ToolServer()
        return

    def setup_main_agent(self) -> None :
        self.main_agent = Agent( "main", self.MAIN_AGENT_MODELS )

        # Optional:
        # self.main_agent.load_prompts([ "prompts/main.md" ])

        tools_path = Path("agent_tools/main_openai.json")
        if tools_path.exists() :
            self.main_agent.load_tools([ str(tools_path) ])

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

        return True

    def generate_response( self,
                           max_tokens : int | None = None ) -> bool :
        """
        Multi-turn generation with tool-call feedback loop.
        """
        _orig_ = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"

        if not self.case_context :
            self.context_build()

        if not self.main_agent :
            self.setup_main_agent()

        message = self.main_agent.get_response( context    = self.case_context,
                                                origin     = f"{_orig_}/stage-1",
                                                max_tokens = max_tokens,
                                                debug      = self.debug )

        if not message or message.is_empty() :
            return False

        message.print()
        if message.text :
            self.send_text(message)
        self.context_update(message)

        if not message.tool_calls :
            return False

        for tool_call in message.tool_calls :
            if tool_call.name == "mark_as_resolved" :
                self.case_mark_as_resolved()

        tool_results = self.tool_server.process(message.tool_calls)
        if tool_results :
            msg_tools = ToolResultsMsg( origin       = f"{_orig_}/stage-2",
                                        tool_results = tool_results )
            msg_tools.print()
            self.context_update(msg_tools)

        return bool( self.case_manifest.status == "open" )
