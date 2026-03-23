from __future__ import annotations

import sys

from services.openai_client import ParagraphGenerationError, generate_paragraph
from services.ssm_loader import SSMParameterError


def main() -> int:
    topic = " ".join(sys.argv[1:]).strip()
    if not topic:
        print("Error: missing topic. Usage: python app.py \"your topic\"", file=sys.stderr)
        return 1

    try:
        paragraph = generate_paragraph(topic)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except SSMParameterError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    except ParagraphGenerationError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 3

    print(paragraph)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
