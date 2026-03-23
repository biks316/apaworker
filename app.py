from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from services.openai_client import ParagraphGenerationError, generate_paragraph
from services.ssm_loader import SSMParameterError

app = FastAPI(title="Paragraph Generator API", version="1.0.0")


class TopicRequest(BaseModel):
    topic: str


class ParagraphResponse(BaseModel):
    paragraph: str


@app.get("/internal/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/gettext/openapi", response_model=ParagraphResponse)
def get_text_openapi(topic: str = "") -> ParagraphResponse:
    try:
        paragraph = generate_paragraph(topic)
        return ParagraphResponse(paragraph=paragraph)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SSMParameterError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ParagraphGenerationError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/gettext/openapi", response_model=ParagraphResponse)
def post_text_openapi(payload: TopicRequest) -> ParagraphResponse:
    try:
        paragraph = generate_paragraph(payload.topic)
        return ParagraphResponse(paragraph=paragraph)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SSMParameterError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ParagraphGenerationError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
