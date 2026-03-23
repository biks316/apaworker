from __future__ import annotations

import os
import re

from openai import OpenAI

from services.ssm_loader import SSMParameterError, load_openai_api_key

DEFAULT_MODEL = "gpt-5.4-mini"


class ParagraphGenerationError(RuntimeError):
    """Raised when paragraph generation fails."""


def _normalize_paragraph(text: str) -> str:
    # Collapse newlines/tabs into single spaces for one clean paragraph output.
    return re.sub(r"\s+", " ", text).strip()


def generate_paragraph(topic: str) -> str:
    if not topic or not topic.strip():
        raise ValueError("Topic is required.")

    model = os.getenv("OPENAI_MODEL", DEFAULT_MODEL)

    try:
        api_key = load_openai_api_key()
    except SSMParameterError:
        raise

    client = OpenAI(api_key=api_key)

    instructions = (
        "Write exactly one paragraph of about 120 to 180 words. "
        "Use clear, professional, human-readable language. "
        "Do not include bullet points. Do not include a title."
    )

    try:
        response = client.responses.create(
            model=model,
            input=[
                {
                    "role": "system",
                    "content": [{"type": "text", "text": instructions}],
                },
                {
                    "role": "user",
                    "content": [{"type": "text", "text": f"Topic: {topic.strip()}"}],
                },
            ],
        )
    except Exception as exc:
        raise ParagraphGenerationError("OpenAI API request failed.") from exc

    text = getattr(response, "output_text", "") or ""
    paragraph = _normalize_paragraph(text)
    if not paragraph:
        raise ParagraphGenerationError("OpenAI API returned an empty response.")

    return paragraph
