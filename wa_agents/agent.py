"""
Agent Class
"""

import os

from base64 import b64encode
from dotenv import load_dotenv
from mistralai import Mistral
from openai import OpenAI
from pydantic import BaseModel
from re import match
from typing import ( Any,
                     Callable,
                     Literal,
                     Type )

from sofia_utils.io import ( extract_code_block,
                             load_file_as_string,
                             load_json_string,
                             load_json_file,
                             write_to_json_string )
from sofia_utils.printing import ( print_recursively,
                                   print_sep )

from .basemodels import ( AssistantContent,
                          AssistantMsg,
                          BasicMsg,
                          Message,
                          StructuredDataMsg,
                          ToolCall,
                          ToolResultsMsg,
                          UserContentMsg )


class Agent :
    
    # -------------------------------------------------------------------------------------
    API_DATA_PATTERN = r"^[a-z0-9.:-]{2,}/[a-z0-9.:-]{2,}$"
    API_DATA : dict[ str, dict] = \
    {
        "anthropic" :
        {
            "claude-opus"  : "claude-opus-4-1-20250805",
            "claude-sonet" : "claude-sonnet-4-20250514"
        },
        "mistral" :
        {
            "mistral-nemo" : "open-mistral-nemo",
            "pixtral"      : "pixtral-12b-2409"
        },
        "openai" :
        {
            "gpt-5"      : "gpt-5",
            "gpt-5-mini" : "gpt-5-mini",
        }
    }
    APIS_NO_TOOL_CALLS : list[str] = [ "mistral" ]
    
    # -------------------------------------------------------------------------------------
    def __init__( self, name : str, models : str | list[str]) -> None :
        
        self.name   = name
        self.api    = None
        self.client = None
        self.model  = None
        self.model_fallbacks : list[str] = []
        load_dotenv()
        
        # Case: Single provider (Anthropic, Mistral, OpenAI)
        if isinstance( models, str) :
            if match( self.API_DATA_PATTERN, models) :
                api_name, model_name = models.split("/")
                if api_name in self.API_DATA :
                    self.api   = api_name
                    self.model = self.API_DATA[api_name].get(model_name)
        
        # Case: OpenRouter routing (non-empty list of model ids)
        elif isinstance( models, list) and models :
            if all( isinstance( m, str) for m in models ) \
            and all( bool(match( self.API_DATA_PATTERN, m)) for m in models ) :
                self.api   = "openrouter"
                self.model = models[0]
                if len(models) > 1 :
                    self.model_fallbacks = models[1:]
        
        if not ( self.api and self.model ) :
            e_prefix = f"In class {self.__class__.__name__} method __init__"
            raise ValueError(f"{e_prefix}: Invalid argument '{models}'")
        
        self.prompts         : list[str]      = []
        self.prompts_merged  : str | None     = None
        self.tools           : list[Any]      = []
        self.post_processors : list[Callable] = []
        
        return
    
    # -------------------------------------------------------------------------------------
    # LOADERS
    # -------------------------------------------------------------------------------------
    
    def load_prompts( self, list_prompt_paths : list[str|dict] ) -> None :
        
        for prompt_obj in list_prompt_paths :
            # If prompt object is string then assume it is path
            if isinstance( prompt_obj, str) :
                prompt_as_str = load_file_as_string(prompt_obj)
                self.prompts.append(prompt_as_str)
            
            # If prompt object is dictionary it should have "path" and "replace"
            elif isinstance( prompt_obj, dict) :
                prompt_path = prompt_obj.get("path")
                if prompt_path and isinstance( prompt_path, str) :
                    prompt_as_str = load_file_as_string(prompt_obj["path"])
                    replacements  = prompt_obj.get("replace")
                    if replacements :
                        for key, val in replacements.items() :
                            prompt_as_str = prompt_as_str.replace( key, val)
                    self.prompts.append(prompt_as_str)
            
            # Else raise exception
            else :
                msg = f"Invalid prompt object of type '{type(prompt_obj)}'"
                raise ValueError(f"In Agent load_prompts: {msg}")
        
        return
    
    def load_tools( self, list_tool_paths : list[str]) -> None :
        
        for blacklisted_api in self.APIS_NO_TOOL_CALLS :
            msg = f"{blacklisted_api.upper()} API cannot make tool calls"
            if blacklisted_api == self.api :
                raise ValueError(f"In Agent load_tools: {msg}")
            for model_fallback_option in self.model_fallbacks :
                if blacklisted_api in model_fallback_option :
                    raise ValueError(f"In Agent load_tools: {msg}")
        
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
    # GET RESPONSE
    # =====================================================================================
    
    def get_response( self,
                      context : list[Message],
                      *,
                      load_imgs  : bool = False,
                      imgs_cache : dict[ str : bytes] = {},
                      output_st  : str | Type | None = None,
                      max_tokens : int | None = None,
                      debug      : bool = False
                    ) -> AssistantContent | None :
        
        # If loading images then arguments must include images cache
        if load_imgs and not imgs_cache :
            msg = "Requested image loading but passed no images cache."
            raise ValueError(f"In Agent get_response: {msg}")
        
        # Merge prompts
        self.merge_prompts()
        
        ag_resp_obj : AssistantContent | None = None
        
        if self.api == "anthropic" :
            ag_resp_obj = self.call_anthropic( context    = context,
                                               load_imgs  = load_imgs,
                                               imgs_cache = imgs_cache,
                                               max_tokens = max_tokens,
                                               debug      = debug )
        
        elif self.api in ( "mistral", "openrouter") :
            ag_resp_obj = self.call_openai_chat_completion( context    = context,
                                                            load_imgs  = load_imgs,
                                                            imgs_cache = imgs_cache,
                                                            output_st  = output_st,
                                                            max_tokens = max_tokens,
                                                            debug      = debug )
        
        elif self.api == "openai" :
            ag_resp_obj = self.call_openai_responses( context    = context,
                                                      load_imgs  = load_imgs,
                                                      imgs_cache = imgs_cache,
                                                      output_st  = output_st,
                                                      max_tokens = max_tokens,
                                                      debug      = debug )
        
        else :
            raise ValueError(f"In Agent get_response: Unsupported API '{self.api}'")
        
        # If no response received then print warning
        if not ag_resp_obj :
            print_sep()
            print(f"In Agent get_response: No response received.")
        elif ag_resp_obj.is_empty() :
            print_sep()
            print(f"In Agent get_response: Response received is empty.")
        
        # Else apply post processing
        elif ag_resp_obj.text_out :
            for post_processor_ in self.post_processors :
                ag_resp_obj.text_out = post_processor_(ag_resp_obj.text_out)
        
        return ag_resp_obj
    
    def debug_print( self,
                     mode : Literal[ "input", "output"],
                     data : Any,
                     client_call : str | None = None ) -> None :
        
        print_sep()
        print(f"{self.api.upper()} API {mode.upper()}:")
        print_sep()
        print_recursively(data)
        
        if client_call :
            print_sep()
            print(f"Client call: {client_call}()")
        
        return
    
    # =====================================================================================
    # DO NOT USE! REFACTORING INCOMPLETE!
    # TODO: Update to use caseflow_basemodels (see call_openai_chat_completion)
    def call_anthropic( self,
                        context : list[Message],
                        *,
                        load_imgs  : bool = False,
                        imgs_cache : dict[ str : bytes] = {},
                        max_tokens : int = 1024,
                        debug      : bool = False
                      ) -> AssistantContent | None :
        return None
    
    # =====================================================================================
    def call_openai_chat_completion( self,
                                     context : list[Message],
                                     *,
                                     load_imgs  : bool = False,
                                     imgs_cache : dict[ str : bytes] = {},
                                     output_st  : str | Type | None = None,
                                     max_tokens : int | None = None,
                                     debug      : bool = False
                                   ) -> AssistantContent | None :
        
        # Initialize API flag
        api_is_openrouter = bool( self.api == "openrouter" )
        
        # Initialize list of messages
        messages : list[dict] = []
        
        # Add system message if prompts are available
        if self.prompts_merged :
            messages.append( { "role" : "system", "content" : self.prompts_merged } )
        
        # Populate messages from context
        for message in context :
            # -----------------------------------------------------------------------------
            # PROCESS TEXT AND/OR MEDIA
            if isinstance( message, BasicMsg) :
                
                # Prepare message header
                msg = { "role" : message.role, "content" : [] }
                
                # -------------------------------------------------------------------------
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
                
                # -------------------------------------------------------------------------
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
                            image_b64 = b64encode(image_bin).decode('utf-8')
                            image_url = f"data:{media.mime};base64,{image_b64}"
                            if api_is_openrouter :
                                image_url = { "url" : image_url }
                            msg["content"].append( { "type"      : "image_url",
                                                    "image_url" : image_url } )
                        else :
                            msg["content"].append(text_cb)
                    else :
                        msg["content"].append(text_cb)
                
                # -------------------------------------------------------------------------
                # If using OpenRouter API and message is pure text then simplify
                if api_is_openrouter \
                and len(msg["content"]) == 1 \
                and msg["content"][0]["type"] == "text" :
                    msg["content"] = msg["content"][0]["text"]
                
                # -------------------------------------------------------------------------
                # INSERT MESSAGE WITH TEXT AND/OR IMAGE
                messages.append(msg)
                
                # -------------------------------------------------------------------------
                # PROCESS ASSISTANT MESSAGE TOOL CALLS
                if isinstance( message, AssistantMsg) and message.tool_calls :
                    # Prepare message for tool calls
                    msg = { "role" : "assistant", "tool_calls" : [] }
                    if api_is_openrouter :
                        msg["content"] = ""
                    # Iterate through tool calls
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
            # PROCESS TOOL RESULTS MESSAGE
            elif isinstance( message, ToolResultsMsg) and message.tool_results :
                for tr in message.tool_results :
                    # Convert tool result content to JSON string if needed
                    if not isinstance( tr.content, str) :
                        tr.content = write_to_json_string( tr.content, indent = None)
                    # Add tool result as tool message
                    messages.append( { "role"         : "tool",
                                       "content"      : tr.content,
                                       "tool_call_id" : tr.id } )
            
            # -----------------------------------------------------------------------------
        
        # Prepare API response request parameters
        resp_req_parms = { "model"      : self.model,
                           "tools"      : self.tools,
                           "messages"   : messages }
        # Setup max tokens
        if max_tokens and isinstance( max_tokens, int) and max_tokens > 0 :
            resp_req_parms["max_tokens"] = max_tokens
        # Setup output structure
        parse_mode = False
        if output_st :
            if isinstance( output_st, str) and output_st == "json" :
                resp_req_parms["response_format"] = { "type" : "json_object" }
            elif issubclass( output_st, BaseModel) :
                resp_req_parms["response_format"] = output_st
                parse_mode = True
        # Setup extra body (for OpenRouter fallback models)
        if api_is_openrouter and self.model_fallbacks :
            resp_req_parms["extra_body"] = { "models" : self.model_fallbacks }
        
        # Print debug info
        if debug :
            
            client_call = "chat."
            if api_is_openrouter :
                client_call += "completions."
                client_call += "create" if not parse_mode else "parse"
            else :
                client_call += "complete" if not parse_mode else "parse"
            
            self.debug_print( "input", resp_req_parms, client_call)
        
        # Generate response using appropriate client
        if api_is_openrouter :
            self.client = OpenAI( api_key  = os.environ.get("API_KEY_OPENROUTER"),
                                  base_url = "https://openrouter.ai/api/v1" )
            if not parse_mode :
                response = self.client.chat.completions.create(**resp_req_parms)
            else :
                response = self.client.chat.completions.parse(**resp_req_parms)
        else :
            self.client = Mistral( api_key = os.environ.get("API_KEY_MISTRAL"))
            if not parse_mode :
                response = self.client.chat.complete(**resp_req_parms)
            else :
                response = self.client.chat.parse(**resp_req_parms)
        
        # Print debug info
        self.debug_print( "output", response) if debug else None
        
        # ---------------------------------------------------------------------------------
        # POST-PROCESSING: COLLECT RESPONSE TEXT AND TOOL CALLS
        # Initialize agent response object
        ag_resp_obj = AssistantContent()
        
        # Collect model and token usage
        if response :
            ag_resp_obj.agent = self.name
            ag_resp_obj.api   = self.api
            ag_resp_obj.model = getattr( response, "model", None)
            usage = getattr( response, "usage", None)
            if usage :
                ag_resp_obj.tokens_input  = getattr( usage, "prompt_tokens", None)
                ag_resp_obj.tokens_output = getattr( usage, "completion_tokens", None)
                ag_resp_obj.tokens_total  = getattr( usage, "total_tokens", None)
            
            ag_resp_obj.instructions = self.prompts_merged
            ag_resp_obj.tools        = self.tools
            ag_resp_obj.context      = [ message.id for message in context ]
            
        else :
            return None
        
        # Select first choice because it is the only one generated
        if response and hasattr( response, "choices") \
        and isinstance( response.choices, list) and response.choices :
            choice = response.choices[0]
            
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
            if output_st and issubclass( output_st, BaseModel) :
                parsed = getattr( choice.message, "parsed", None)
                if parsed and isinstance( parsed, BaseModel) :
                    ag_resp_obj.st_output = parsed.model_dump()
                    ag_resp_obj.st_out_bm = parsed.__class__.__name__
                else :
                    try :
                        text_out = extract_code_block(ag_resp_obj.text_out)
                        ag_resp_obj.st_output = output_st.model_validate_json(text_out)
                        ag_resp_obj.st_out_bm = output_st.__name__
                    except Exception :
                        pass
            
            # If structured output was collected then clear text
            if ag_resp_obj.st_output :
                ag_resp_obj.text_out = None
        
        # ---------------------------------------------------------------------------------
        # The grand finale
        return ag_resp_obj
    
    # =====================================================================================
    # DO NOT USE! REFACTORING INCOMPLETE!
    # TODO: Update to use caseflow_basemodels (see call_openai_chat_completion)
    def call_openai_responses( self,
                               context : list[Message],
                               *,
                               load_imgs  : bool = False,
                               imgs_cache : dict[ str, bytes] = {},
                               output_st  : str | Type | None = None,
                               max_tokens : int = 1024,
                               debug      : bool = False
                             ) -> AssistantContent | None :
        return None
