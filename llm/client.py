"""LLM API wrapper (OpenAI/Codex + Claude fallback)."""

from __future__ import annotations

import json
import time
from typing import Any
from urllib import request

from rich.console import Console

from config.settings import (
    ANTHROPIC_API_KEY,
    CLAUDE_MODEL,
    LLM_PROVIDER,
    MAX_TOKENS,
    OPENAI_API_KEY,
    OPENAI_FALLBACK_MODEL,
    OPENAI_MODEL,
)

console = Console()


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    payload = {
        "sessionId": "f07731",
        "runId": "initial",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        req = request.Request(
            "http://127.0.0.1:7897/ingest/d0e90649-22bc-4799-98cf-38260af08d14",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-Debug-Session-Id": "f07731",
            },
            method="POST",
        )
        request.urlopen(req, timeout=2).read()
    except Exception:
        pass


class ClaudeClient:
    """Backward-compatible client name; supports OpenAI and Anthropic."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        provider: str | None = None,
    ):
        self.provider = (provider or LLM_PROVIDER or "openai").lower().strip()
        if self.provider == "openai":
            self.api_key = api_key or OPENAI_API_KEY
            self.model = model or OPENAI_MODEL
            self.fallback_model = OPENAI_FALLBACK_MODEL
        else:
            self.api_key = api_key or ANTHROPIC_API_KEY
            self.model = model or CLAUDE_MODEL
            self.fallback_model = ""

        if not self.api_key:
            raise ValueError(
                "API key not set. Configure OPENAI_API_KEY (or ANTHROPIC_API_KEY)."
            )

        if self.provider == "openai":
            from openai import OpenAI

            self.client = OpenAI(api_key=self.api_key)
        elif self.provider == "anthropic":
            from anthropic import Anthropic

            self.client = Anthropic(api_key=self.api_key)
        else:
            raise ValueError(
                f"Unsupported LLM_PROVIDER '{self.provider}'. Use 'openai' or 'anthropic'."
            )

        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_calls = 0

    def _ask_openai_chat(
        self,
        model: str,
        user_message: str,
        system_prompt: str,
        max_tokens: int,
        temperature: float,
    ) -> str:
        messages: list[dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_message})
        kwargs: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
        }
        if model.lower().startswith("gpt-5"):
            kwargs["max_completion_tokens"] = max_tokens
        else:
            kwargs["max_tokens"] = max_tokens
        response = self.client.chat.completions.create(**kwargs)
        usage = response.usage
        if usage:
            self.total_input_tokens += int(usage.prompt_tokens or 0)
            self.total_output_tokens += int(usage.completion_tokens or 0)
        return response.choices[0].message.content or ""

    def _ask_openai_completion(
        self,
        model: str,
        user_message: str,
        system_prompt: str,
        max_tokens: int,
        temperature: float,
    ) -> str:
        prompt_parts = []
        if system_prompt:
            prompt_parts.append(system_prompt.strip())
        prompt_parts.append(user_message.strip())
        prompt = "\n\n".join(p for p in prompt_parts if p)
        response = self.client.completions.create(
            model=model,
            prompt=prompt,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        usage = getattr(response, "usage", None)
        if usage:
            self.total_input_tokens += int(getattr(usage, "prompt_tokens", 0) or 0)
            self.total_output_tokens += int(getattr(usage, "completion_tokens", 0) or 0)
        return response.choices[0].text or ""

    def ask(
        self,
        user_message: str,
        system_prompt: str = "",
        max_tokens: int = MAX_TOKENS,
        temperature: float = 0.0,
    ) -> str:
        """Send a single message and return the text response."""
        self.total_calls += 1

        if self.provider == "openai":
            models_to_try: list[str] = [self.model]
            if self.fallback_model and self.fallback_model != self.model:
                models_to_try.append(self.fallback_model)

            # region agent log
            _debug_log(
                "H5",
                "llm/client.py:152",
                "OpenAI ask starting",
                {
                    "provider": self.provider,
                    "models_to_try": models_to_try,
                    "api_key_present": bool(self.api_key),
                    "api_key_prefix": (self.api_key[:7] + "***") if self.api_key else "",
                },
            )
            # endregion

            last_exc: Exception | None = None
            for m in models_to_try:
                try:
                    # Codex-style models can require /v1/completions instead of chat.
                    if "codex" in m.lower():
                        return self._ask_openai_completion(
                            model=m,
                            user_message=user_message,
                            system_prompt=system_prompt,
                            max_tokens=max_tokens,
                            temperature=temperature,
                        )

                    return self._ask_openai_chat(
                        model=m,
                        user_message=user_message,
                        system_prompt=system_prompt,
                        max_tokens=max_tokens,
                        temperature=temperature,
                    )
                except Exception as exc:
                    last_exc = exc
                    # region agent log
                    _debug_log(
                        "H5",
                        "llm/client.py:178",
                        "OpenAI model call failed",
                        {
                            "model": m,
                            "error_type": type(exc).__name__,
                            "error_text": str(exc)[:500],
                        },
                    )
                    # endregion
                    msg = str(exc).lower()
                    if "not a chat model" in msg or "/v1/completions" in msg:
                        try:
                            return self._ask_openai_completion(
                                model=m,
                                user_message=user_message,
                                system_prompt=system_prompt,
                                max_tokens=max_tokens,
                                temperature=temperature,
                            )
                        except Exception as inner_exc:
                            last_exc = inner_exc
                            # region agent log
                            _debug_log(
                                "H5",
                                "llm/client.py:196",
                                "OpenAI completion fallback failed",
                                {
                                    "model": m,
                                    "error_type": type(inner_exc).__name__,
                                    "error_text": str(inner_exc)[:500],
                                },
                            )
                            # endregion
                    continue

            if last_exc:
                raise last_exc
            raise RuntimeError("OpenAI request failed without specific error")

        messages = [{"role": "user", "content": user_message}]
        kwargs: dict[str, Any] = {
            "model": self.model,
            "max_tokens": max_tokens,
            "messages": messages,
            "temperature": temperature,
        }
        if system_prompt:
            kwargs["system"] = system_prompt

        response = self.client.messages.create(**kwargs)
        self.total_input_tokens += response.usage.input_tokens
        self.total_output_tokens += response.usage.output_tokens
        return response.content[0].text

    def ask_json(
        self,
        user_message: str,
        system_prompt: str = "",
        max_tokens: int = MAX_TOKENS,
        temperature: float = 0.0,
    ) -> Any:
        """Send a message and parse the JSON response."""
        raw = self.ask(user_message, system_prompt, max_tokens, temperature)
        # Strip markdown code fences if present
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove first line (```json) and last line (```)
            lines = [l for l in lines[1:] if l.strip() != "```"]
            text = "\n".join(lines)
        return json.loads(text)

    def ask_batch(
        self,
        items: list[dict],
        system_prompt: str,
        prompt_template: str,
        batch_size: int = 25,
        max_tokens: int = MAX_TOKENS,
    ) -> list[Any]:
        """Process items in batches, sending each batch as a single API call.

        Args:
            items: List of dicts, each representing one item to process.
            system_prompt: System prompt for context.
            prompt_template: Template string. Will receive {batch_json} and {batch_index}.
            batch_size: Number of items per API call.
            max_tokens: Max tokens for response.

        Returns:
            Combined list of results from all batches (parsed JSON).
        """
        all_results = []
        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            batch_json = json.dumps(batch, ensure_ascii=False, indent=2)
            prompt = prompt_template.format(
                batch_json=batch_json,
                batch_index=i // batch_size + 1,
                batch_count=(len(items) + batch_size - 1) // batch_size,
            )
            result = self.ask_json(prompt, system_prompt, max_tokens)
            if isinstance(result, list):
                all_results.extend(result)
            else:
                all_results.append(result)
            # Small delay to avoid rate limits
            if i + batch_size < len(items):
                time.sleep(0.5)
        return all_results

    def get_usage_summary(self) -> str:
        """Return a summary of API usage."""
        return (
            f"Provider: {self.provider} | Model: {self.model} | "
            f"API calls: {self.total_calls} | "
            f"Tokens: {self.total_input_tokens:,} in + {self.total_output_tokens:,} out"
        )
