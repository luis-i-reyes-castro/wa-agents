from base64 import b64encode

import pytest

from openai.types.chat import ChatCompletion

from wa_agents.agent import AgentBase
from wa_agents.case_handler_models import (
    HumanUserContentMsg,
    MediaObject,
)


def build_agent() -> AgentBase :
    
    agent = AgentBase.__new__(AgentBase)
    agent.prompts_merged = None
    
    return agent


def test_build_messages_prefers_hydrated_media_content() -> None :
    
    image_content = b"hydrated-image"
    context = [
        HumanUserContentMsg(
            text  = "Describe this image.",
            media = MediaObject(
                mime    = "image/png",
                name    = "image.png",
                content = image_content,
            ),
        )
    ]
    agent = build_agent()
    
    agent.validate_get_response_args( context, True)
    messages = agent.build_messages(
        context,
        load_imgs = True,
    )
    
    expected_b64 = b64encode(image_content).decode("utf-8")
    assert messages[0]["content"][1] == {
        "type"      : "image_url",
        "image_url" : { "url" : f"data:image/png;base64,{expected_b64}" },
    }


def test_build_messages_uses_media_placeholder_without_image_loading() -> None :
    
    context = [
        HumanUserContentMsg(
            media = MediaObject(
                mime = "image/jpeg",
                name = "image.jpg",
            ),
        )
    ]
    agent = build_agent()
    
    agent.validate_get_response_args( context, False)
    messages = agent.build_messages( context, False)
    
    assert messages[0]["content"] == "[SYSTEM] Message includes media (image/jpeg)"


def test_validate_image_loading_requires_content() -> None :
    
    context = [
        HumanUserContentMsg(
            media = MediaObject(
                mime = "image/png",
                name = "image.png",
            ),
        )
    ]
    agent = build_agent()
    
    with pytest.raises( ValueError, match = "no image content was provided") :
        agent.validate_get_response_args( context, True)


def test_collect_response_keeps_usage_and_one_tool_call() -> None :
    
    agent = build_agent()
    agent.name = "main"
    agent.api  = "openrouter"
    agent.tools = []
    response = ChatCompletion.model_validate(
        {
            "id"      : "gen-1",
            "object"  : "chat.completion",
            "created" : 1,
            "model"   : "openai/gpt-5-nano",
            "choices" : [
                {
                    "index"         : 0,
                    "finish_reason" : "tool_calls",
                    "message"       : {
                        "role"       : "assistant",
                        "content"    : None,
                        "tool_calls" : [
                            {
                                "id"       : "call_1",
                                "type"     : "function",
                                "function" : {
                                    "name"      : "lookup",
                                    "arguments" : '{"q":"hola"}',
                                },
                            },
                        ],
                    },
                },
            ],
            "usage" : {
                "prompt_tokens"     : 10,
                "completion_tokens" : 4,
                "total_tokens"      : 14,
                "cost"              : 0.00012,
                "cost_details"      : { "upstream_inference_cost" : 0.0001 },
            },
        }
    )
    
    msg = agent.collect_response(
        response = response,
        context  = [],
        origin   = "test",
    )
    
    assert msg.model == "openai/gpt-5-nano"
    assert msg.finish_reason == "tool_calls"
    assert len(msg.tool_calls) == 1
    assert msg.tool_calls[0].name == "lookup"
    assert msg.tool_calls[0].input == { "q" : "hola" }
    assert msg.usage is not None
    assert msg.usage.cost == 0.00012
    assert msg.usage.prompt_tokens == 10
    dumped = msg.model_dump( mode = "json")["usage"]
    assert dumped["cost_details"]["upstream_inference_cost"] == 0.0001
