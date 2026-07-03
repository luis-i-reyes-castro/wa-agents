"""
Agent Classes
"""

import os

from abc import ABC
from base64 import b64encode
from inspect import currentframe
from openai import (
    AsyncOpenAI,
    OpenAI,
)
from openai.types.chat import (
    ChatCompletion,
    ParsedChatCompletion,
)
from pathlib import Path
from pydantic import BaseModel
from re import match
from typing import (
    Any,
    Callable,
    Literal,
    cast,
)

from sofia_utils.io import (
    extract_code_block,
    load_file_as_string,
    load_json_file,
    load_json_string,
    write_to_json_string,
)
from sofia_utils.printing import (
    print_recursively,
    print_sep,
)

from .basemodels import (
    AssistantMsg,
    BasicMsg,
    Message,
    StructuredDataMsg,
    ToolCall,
    ToolResultsMsg,
    UserContentMsg,
)


class AgentBase (ABC) :
    """
    Shared OpenRouter agent behavior \\
    Provides prompt/tool loading, request formatting, and response parsing.
    """
    
    # =====================================================================================
    # SHARED CONSTRUCTOR AND LOADERS
    # =====================================================================================
    
    API_DATA_PATTERN = r"^([\w\.:-]+)/([\w\.:-]+)$"
    BASE_URL         = "https://openrouter.ai/api/v1"
    
    def __init__(
        self,
        name   : str,
        models : str | list[str],
    ) -> None :
        """
        Configure the agent for OpenRouter model fallback order \\
        Args:
            name   : Friendly name to attach to generated responses
            models : One OpenRouter model string or ordered model fallback list
        """
        here = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        if not os.getenv("OPENROUTER_API_KEY") :
            raise RuntimeError(
                f"In {here}: Environment variable 'OPENROUTER_API_KEY' was not found"
            )
        
        if isinstance( models, str) :
            models = [models]
        
        if not (
            models
            and isinstance( models, list)
            and all( isinstance( m, str) for m in models )
            and all( bool( match( self.API_DATA_PATTERN, m) ) for m in models )
        ) :
            raise ValueError(f"In {here}: Invalid argument '{models}'")
        
        self.name            = name
        self.api             = "openrouter"
        self.model           = models[0]
        self.model_fallbacks = models[1:]
        
        self.prompts         : list[str]  = []
        self.prompts_merged  : str | None = None
        self.tools           : list[Any]  = []
        self.client          : Any | None = None
        self.post_processors : list[ Callable[ [str], str] ] = []
        
        return
    
    def load_prompts(
        self,
        list_prompt_paths : list[ str | Path | dict ],
    ) -> None :
        """
        Load prompt snippets from strings, paths or replacement dicts \\
        Args:
            list_prompt_paths : Sequence with file names, paths or replacement dicts.
            For replacement dicts, keys `path` and `replace` are required;
            key `path` must be a file name (`str`) or path (`Path`), and
            key `replace` must be a dict mapping strings to strings.
        """
        here = f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        
        for prompt_obj in list_prompt_paths :
            
            if (
                isinstance( prompt_obj, ( str, Path)) and
                ( prompt_as_str := load_file_as_string(prompt_obj) )
            ) :
                self.prompts.append(prompt_as_str)
            
            elif (
                isinstance( prompt_obj, dict)               and
                ( prompt_path   := prompt_obj.get("path") ) and
                isinstance( prompt_path, ( str, Path))      and
                ( prompt_as_str := load_file_as_string(prompt_path) )
            ) :
                replacements = prompt_obj.get("replace")
                if (
                    isinstance( replacements, dict) and
                    all(
                        ( isinstance( key, str) and isinstance( val, str) )
                        for key, val in replacements.items()
                    )
                ) :
                    for key, val in replacements.items() :
                        prompt_as_str = prompt_as_str.replace( key, val)
                else :
                    raise ValueError(
                        f"In {here}: Prompt path '{str(prompt_path)}' "
                        f"is assigned to invalid replacements dict '{str(replacements)}'."
                    )
                
                self.prompts.append(prompt_as_str)
            
            # Else raise exception
            else :
                raise ValueError(
                    f"In {here}: Argument 'list_prompt_paths' has an invalid "
                    f"item of type '{type(prompt_obj)}'; "
                    f"contents: '{str(prompt_obj)}'."
                )
        
        return
    
    def load_tools(
        self,
        list_tool_paths : list[ str | Path ],
    ) -> None :
        """
        Load JSON tool schemas \\
        Args:
            list_tool_paths : JSON files with either dict or list payloads
        """
        
        for tool_file in list_tool_paths :
            tool_file_content = load_json_file(tool_file)
            
            if isinstance( tool_file_content, dict) :
                self.tools.append(tool_file_content)
            
            elif isinstance( tool_file_content, list) :
                self.tools.extend(tool_file_content)
            
            else :
                msg = f"Invalid tool file content" \
                    + f"\n\tFile path: {tool_file}"   \
                    + f"\n\tContent type: {type(tool_file_content)}"
                raise ValueError(f"In Agent load_tools: {msg}")
        
        return
    
    def merge_prompts(self) -> None :
        """
        Merge loaded prompts into a single system message separated by blanks
        """
        
        # Merge prompts (adding 1-2 extra newlines between prompts)
        if self.prompts :
            self.prompts_merged = ""
            # Process and append all prompts except last one
            for prompt_str in self.prompts[:-1] :
                if prompt_str.endswith("\n") :
                    self.prompts_merged += ( prompt_str + "\n" )
                else :
                    self.prompts_merged += ( prompt_str + "\n\n" )
            # Append last prompt
            self.prompts_merged += self.prompts[-1]
        
        return
    
    # =====================================================================================
    # SHARED RESPONSE HELPERS
    # =====================================================================================

    def validate_get_response_args(
        self,
        load_imgs  : bool,
        imgs_cache : dict[ str, bytes],
    ) -> None :
        """
        Validate get_response arguments \\
        Args:
            load_imgs  : Whether images should be loaded into the request
            imgs_cache : Cache mapping image names to bytes
        """
        if load_imgs and not imgs_cache :
            msg = "Requested image loading but passed no images cache."
            raise ValueError(f"In Agent get_response: {msg}")
        
        return
    
    def debug_print(
        self,
        mode : Literal[ "input", "output"],
        data : Any,
        client_call : str | None = None,
    ) -> None :
        """
        Pretty-print API payloads/responses for debugging sessions \\
        Args:
            mode        : Either 'input' or 'output'
            data        : Arbitrary payload to print recursively
            client_call : Helpful string with the client call being made
        """
        print_sep()
        print(f"{self.api.upper()} API {mode.upper()}:")
        print_sep()
        print_recursively(data)
        
        if client_call :
            print_sep()
            print(f"Client call: {client_call}()")
        
        return
    
    def build_messages(
        self,
        context    : list[Message],
        load_imgs  : bool,
        imgs_cache : dict[ str, bytes],
    ) -> list[dict] :
        """
        Serialize message context into OpenRouter chat messages \\
        Args:
            context    : Message history to serialize
            load_imgs  : Whether to include user-provided images
            imgs_cache : Cache used when `load_imgs` is enabled
        Returns:
            List of chat completion messages
        """
        messages : list[dict] = []
        
        # Add system message if prompts are available
        if self.prompts_merged :
            messages.append( { "role" : "system", "content" : self.prompts_merged } )
        
        # Populate messages from context
        for message in context :
            
            # -----------------------------------------------------------------------------
            # CASE 1: BASIC MESSAGE SUBCLASS (I.E., MESSAGE WITH TEXT AND/OR MEDIA)
            if isinstance( message, BasicMsg) :
                
                # Prepare message header
                msg = { "role" : message.role, "content" : [] }
                
                # PROCESS TEXT
                # Case of Basic Message
                if isinstance( message, BasicMsg) and message.text :
                    text_cb = { "type" : "text", "text" : message.text }
                    msg["content"].append(text_cb)
                # Case of Structured Data Message
                elif isinstance( message, StructuredDataMsg) :
                    text_cb = { "type" : "text", "text" : message.as_text() }
                    msg["content"].append(text_cb)
                # Case of Assistant Message with structured output
                elif isinstance( message, AssistantMsg) and message.st_output :
                    text_cb = { "type" : "text",
                                "text" : write_to_json_string( message.st_output,
                                                               indent = None) }
                    msg["content"].append(text_cb)
                
                # PROCESS IMAGES
                if isinstance( message, UserContentMsg) and message.media :
                    # Prepare placeholder text message
                    media   = message.media
                    text_cb = { "type" : "text",
                                "text" : f"[SYSTEM] User sent media ({media.mime})" }
                    # If allowed to load images
                    if load_imgs and media.mime.startswith("image") :
                        # Attempt to retrieve image from cache
                        image_bin = imgs_cache.get(media.name)
                        # If retrieval successful then encode and insert
                        if image_bin and isinstance( image_bin, bytes) :
                            image_b64 = b64encode(image_bin).decode("utf-8")
                            image_url = {
                                "url" : f"data:{media.mime};base64,{image_b64}"
                            }
                            msg["content"].append( { "type"      : "image_url",
                                                     "image_url" : image_url } )
                        else :
                            msg["content"].append(text_cb)
                    else :
                        msg["content"].append(text_cb)
                
                # If message is only text then simplify structure
                if len(msg["content"]) == 1 \
                and msg["content"][0]["type"] == "text" :
                    msg["content"] = msg["content"][0]["text"]
                
                # INSERT MESSAGE WITH TEXT AND/OR IMAGE
                messages.append(msg)
                
                # PROCESS ASSISTANT MESSAGE TOOL CALLS
                if isinstance( message, AssistantMsg) and message.tool_calls :
                    
                    msg = { "role" : "assistant", "content" : "", "tool_calls" : [] }
                    
                    for tc in message.tool_calls :
                        # Convert tool call arguments to JSON string if needed
                        if not isinstance( tc.input, str) :
                            tc.input = write_to_json_string( tc.input, indent = None)
                        # Add tool call to the list
                        tc_cb = { "id"       : tc.id,
                                  "type"     : "function",
                                  "function" : { "name"      : tc.name,
                                                 "arguments" : tc.input } }
                        msg["tool_calls"].append(tc_cb)
                    
                    # Add single assistant message with all tool calls
                    messages.append(msg)
            
            # -----------------------------------------------------------------------------
            # CASE 2: TOOL RESULTS MESSAGE
            elif isinstance( message, ToolResultsMsg) and message.tool_results :
                for tr in message.tool_results :
                    # Convert tool result content to JSON string if needed
                    if not isinstance( tr.content, str) :
                        tr.content = write_to_json_string( tr.content, indent = None)
                    # Add tool result as tool message
                    messages.append( { "role"         : "tool",
                                       "content"      : tr.content,
                                       "tool_call_id" : tr.id } )
        
        return messages
        
    def build_request_params(
        self,
        context : list[Message],
        *,
        load_imgs  : bool = False,
        imgs_cache : dict[ str, bytes] = {},
        output_st  : str | type[BaseModel] | None = None,
        max_tokens : int | None = None,
    ) -> tuple[ dict, bool] :
        """
        Build OpenRouter chat completion request parameters \\
        Args:
            context    : Message history to serialize
            load_imgs  : Whether to include user-provided images
            imgs_cache : Cache used when `load_imgs` is enabled
            output_st  : Structured output schema (json or BaseModel subclass)
            max_tokens : Optional completion cap passed to the API
        Returns:
            Tuple of request parameters and parse mode flag.
        """
        resp_req_parms = {
            "model"    : self.model,
            "tools"    : self.tools,
            "messages" : self.build_messages( context, load_imgs, imgs_cache),
        }
        
        if max_tokens and isinstance( max_tokens, int) and max_tokens > 0 :
            resp_req_parms["max_tokens"] = max_tokens
        
        parse_mode = False
        if output_st :
            if isinstance( output_st, type) and issubclass( output_st, BaseModel) :
                resp_req_parms["response_format"] = output_st
                parse_mode = True
            elif isinstance( output_st, str) and output_st == "json" :
                resp_req_parms["response_format"] = { "type" : "json_object" }
        
        if self.model_fallbacks :
            resp_req_parms["extra_body"] = { "models" : self.model_fallbacks }
        
        return resp_req_parms, parse_mode
    
    def collect_response(
        self,
        response  : ChatCompletion | ParsedChatCompletion,
        context   : list[Message],
        origin    : str,
        output_st : str | type[BaseModel] | None = None,
    ) -> AssistantMsg :
        """
        Convert an OpenRouter response into an AssistantMsg \\
        Args:
            response  : Chat completion response object
            context   : Context messages used for the request
            origin    : Identifier describing who triggered the request
            output_st : Structured output schema, when requested
        Returns:
            Parsed assistant response
        """
        origin = origin or f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        
        ag_resp_obj = AssistantMsg(
            origin       = origin,
            agent        = self.name,
            api          = self.api,
            model        = getattr( response, "model", None),
            instructions = self.prompts_merged,
            tools        = self.tools,
            context      = [ message.id for message in context ],
        )
        
        usage = getattr( response, "usage", None)
        if usage :
            ag_resp_obj.tokens_input  = getattr( usage, "prompt_tokens", None)
            ag_resp_obj.tokens_output = getattr( usage, "completion_tokens", None)
            ag_resp_obj.tokens_total  = getattr( usage, "total_tokens", None)
        
        if (
            hasattr( response, "choices")
            and isinstance( response.choices, list)
            and response.choices
            and ( choice := response.choices[0] )
        ) :
            # Collect text
            content = getattr( choice.message, "content", None)
            if isinstance( content, str) :
                ag_resp_obj.append_to_text(content)
            elif isinstance( content, list) :
                for item in content :
                    if isinstance( item, str) :
                        ag_resp_obj.append_to_text(item)
                    elif isinstance( item, dict) :
                        item_text = item.get("text")
                        if item_text :
                            ag_resp_obj.append_to_text(item_text)
            elif isinstance( content, dict) :
                text_value = content.get("text")
                if text_value :
                    ag_resp_obj.append_to_text(text_value)
            
            # Collect tool calls
            tool_calls = getattr( choice.message, "tool_calls", None)
            if tool_calls :
                for tool_call in tool_calls :
                    # Parse tool call arguments
                    arguments = tool_call.function.arguments
                    if isinstance( arguments, str) :
                        arguments = load_json_string(arguments)
                    # Create tool call object
                    tc = ToolCall( id    = tool_call.id,
                                   name  = tool_call.function.name,
                                   input = arguments )
                    ag_resp_obj.tool_calls.append(tc)
            
            # Collect structured output
            if isinstance( output_st, type) and issubclass( output_st, BaseModel) :
                
                parsed = getattr( choice.message, "parsed", None)
                if parsed and isinstance( parsed, BaseModel) :
                    ag_resp_obj.st_output = parsed.model_dump()
                    ag_resp_obj.st_out_bm = parsed.__class__.__name__
                
                elif ag_resp_obj.text :
                    try :
                        code = extract_code_block(ag_resp_obj.text)
                        ag_resp_obj.st_output = output_st.model_validate_json(code)
                        ag_resp_obj.st_out_bm = output_st.__name__
                    except Exception :
                        pass
            
            # If structured output was collected then clear text
            if ag_resp_obj.st_output :
                ag_resp_obj.text = None
        
        return ag_resp_obj
    
    def validate_and_post_process_response(
        self,
        ag_resp_obj : AssistantMsg | None,
    ) -> AssistantMsg | None :
        """
        Validate and post-process an assistant response \\
        Args:
            ag_resp_obj : Response object returned by OpenRouter
        Returns:
            Post-processed response, or None if empty.
        """
        
        if not ag_resp_obj :
            print_sep()
            print("In Agent get_response: No response received.")
            return None
        
        elif ag_resp_obj.is_empty() :
            print_sep()
            print("In Agent get_response: Response received is empty.")
            return None
        
        if ag_resp_obj.text :
            for post_processor_ in self.post_processors :
                ag_resp_obj.text = post_processor_(ag_resp_obj.text)
        
        return ag_resp_obj


class Agent (AgentBase) :
    """
    Sequential OpenRouter chat-completion wrapper
    """
    
    def get_response(
        self,
        context : list[Message],
        *,
        origin     : str | None = None,
        load_imgs  : bool = False,
        imgs_cache : dict[ str, bytes] = {},
        output_st  : str | type[BaseModel] | None = None,
        max_tokens : int | None = None,
        debug      : bool = False,
    ) -> AssistantMsg | None :
        """
        Request a response from OpenRouter and post-process outputs
        """
        origin = origin or f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        
        self.validate_get_response_args( load_imgs, imgs_cache)
        self.merge_prompts()
        
        resp_req_parms, parse_mode = self.build_request_params(
            context    = context,
            load_imgs  = load_imgs,
            imgs_cache = imgs_cache,
            output_st  = output_st,
            max_tokens = max_tokens,
        )
        
        if debug :
            client_call = (
                "chat.completions.create"
                if not parse_mode else
                "chat.completions.parse"
            )
            self.debug_print( "input", resp_req_parms, client_call)
        
        self.client : OpenAI = OpenAI(
            api_key  = os.environ.get("OPENROUTER_API_KEY"),
            base_url = self.BASE_URL,
        )
        
        if not parse_mode :
            response = cast(
                ChatCompletion,
                self.client.chat.completions.create(**resp_req_parms),
            )
        else :
            response = cast(
                ParsedChatCompletion,
                self.client.chat.completions.parse(**resp_req_parms),
            )
        
        self.debug_print( "output", response) if debug else None
        
        if not response :
            return None
        
        response_ = self.collect_response( response, context, origin, output_st)
        
        return self.validate_and_post_process_response(response_)


class AsyncAgent (AgentBase) :
    """
    Async OpenRouter chat-completion wrapper
    """
    
    async def get_response(
        self,
        context : list[Message],
        *,
        origin     : str | None = None,
        load_imgs  : bool = False,
        imgs_cache : dict[ str, bytes] = {},
        output_st  : str | type[BaseModel] | None = None,
        max_tokens : int | None = None,
        debug      : bool = False,
    ) -> AssistantMsg | None :
        """
        Request a response from OpenRouter and post-process outputs asynchronously
        """
        origin = origin or f"{self.__class__.__name__}/{currentframe().f_code.co_name}"
        
        self.validate_get_response_args( load_imgs, imgs_cache)
        self.merge_prompts()
        
        resp_req_parms, parse_mode = self.build_request_params(
            context    = context,
            load_imgs  = load_imgs,
            imgs_cache = imgs_cache,
            output_st  = output_st,
            max_tokens = max_tokens,
        )
        
        if debug :
            client_call = (
                "chat.completions.create"
                if not parse_mode else
                "chat.completions.parse"
            )
            self.debug_print( "input", resp_req_parms, client_call)
        
        self.client : AsyncOpenAI = AsyncOpenAI(
            api_key  = os.environ.get("OPENROUTER_API_KEY"),
            base_url = self.BASE_URL,
        )
        
        if not parse_mode :
            response = cast(
                ChatCompletion,
                await self.client.chat.completions.create(**resp_req_parms),
            )
        else :
            response = cast(
                ParsedChatCompletion,
                await self.client.chat.completions.parse(**resp_req_parms),
            )
        
        self.debug_print( "output", response) if debug else None
        
        if not response :
            return None
        
        response_ = self.collect_response( response, context, origin, output_st)
        
        return self.validate_and_post_process_response(response_)
