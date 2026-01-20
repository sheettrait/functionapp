import os
from typing import List, Dict

try:
    from openai import AzureOpenAI
except ImportError:  # pragma: no cover
    AzureOpenAI = None  # type: ignore[misc]


class SimpleMemory:
    def __init__(self, max_turns: int = 6):
        self.max_turns = max_turns
        self.messages: List[Dict[str, str]] = []

    def add(self, role: str, content: str) -> None:
        self.messages.append({"role": role, "content": content})
        if len(self.messages) > self.max_turns:
            self.messages = self.messages[-self.max_turns :]

    def history(self) -> List[Dict[str, str]]:
        return list(self.messages)


class Agent:
    def __init__(self):
        if AzureOpenAI is None:
            raise ImportError("openai package is required. pip install openai>=1.46.0")

        self.client = AzureOpenAI(
            azure_endpoint=os.environ.get("AZURE_OPENAI_ENDPOINT"),
            api_key=os.environ.get("AZURE_OPENAI_API_KEY"),
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-06-01"),
        )
        self.deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-5.2-chat")
        self.memory = SimpleMemory()

    def respond(self, user_message: str) -> str:
        system_prompt = (
            "You are a clinical assistant. "
            "Be concise and cite data origins (table names) if provided." 
        )

        self.memory.add("user", user_message)

        messages = [{"role": "system", "content": system_prompt}] + self.memory.history()

        completion = self.client.chat.completions.create(
            model=self.deployment,
            messages=messages,
            temperature=0.1,
            max_tokens=400,
        )
        answer = completion.choices[0].message.content or ""
        self.memory.add("assistant", answer)
        return answer


if __name__ == "__main__":
    # Example usage (requires Azure OpenAI env vars set)
    agent = Agent()
    print(agent.respond("Show latest vitals for patient P001"))
