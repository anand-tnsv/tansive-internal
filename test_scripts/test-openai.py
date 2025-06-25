import sys
import json
import openai

# Replace with your OpenAI API key
OPENAI_API_KEY = "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"

LLM_BLOCKED_BY_POLICY_PROMPT = """
All tools with tag [TansivePolicy: true] are governed by Tansive policy.
If any tool call with such tag returns an error containing "This operation is blocked by Tansive policy", you must respond to the user with:
"I tried to use Skill: <tool-name> for <reason> but it was blocked by Tansive policy. Please contact the administrator of your Tansive system to obtain access." Do not attempt to bypass, hallucinate, or reroute the request. Respect the policy boundaries.
"""

# Example tool definition - you can modify this as needed
MOCK_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get the current weather in a given location",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "The city and state, e.g. San Francisco, CA",
                    }
                },
                "required": ["location"],
            },
        },
    }
]


def main():
    # Test input - modify as needed
    question = "What's the weather like in San Francisco?"
    model = "gpt-4"  # or any other model you want to test

    openai_args = {"api_key": OPENAI_API_KEY}
    # Uncomment below if using Claude models
    # if model.startswith("claude"):
    #     openai_args["base_url"] = "https://api.anthropic.com/v1"

    openai_client = openai.OpenAI(**openai_args)

    messages = [
        {"role": "system", "content": LLM_BLOCKED_BY_POLICY_PROMPT},
        {"role": "user", "content": question},
    ]
    tools = MOCK_TOOLS

    while True:
        try:
            response = openai_client.chat.completions.create(
                model=model,
                messages=messages,
                tools=tools,
                seed=0,
            )
        except Exception as e:
            print(f"OpenAI call failed: {e}", file=sys.stderr)
            sys.exit(1)

        choice = response.choices[0]
        message = choice.message
        finish_reason = choice.finish_reason

        if message.content:
            print(f"🤔 Thinking: {message.content}")

        if finish_reason and finish_reason != "tool_calls":
            print(f"Final response: {message.content}")
            break

        if not message.tool_calls:
            break

        # Add the assistant's message with tool calls first
        messages.append(message.model_dump(exclude_unset=True))

        for tool_call in message.tool_calls:
            try:
                tool_args = json.loads(tool_call.function.arguments)
                # Mock response based on the tool call
                mock_response = {
                    "temperature": "72°F",
                    "condition": "Sunny",
                    "humidity": "45%",
                }
                tool_response = json.dumps(mock_response)
            except Exception as e:
                print(f"Tool call failed: {e}", file=sys.stderr)
                sys.exit(1)

            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": tool_response,
                }
            )


if __name__ == "__main__":
    main()
