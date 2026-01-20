import json
import logging

import azure.functions as func


# Stub: replace with actual Azure text analytics / content safety call

def scrub_text(text: str) -> dict:
    # In real implementation, call Azure service to detect/mask PII
    masked = text  # placeholder
    findings = []  # placeholder list of detected entities
    return {"masked_text": masked, "findings": findings}


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("pii_clean function processing a request")
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    text = body.get("text")
    if not text:
        return func.HttpResponse("Missing 'text' field", status_code=400)

    cleaned = scrub_text(text)
    return func.HttpResponse(
        json.dumps(cleaned, ensure_ascii=False),
        status_code=200,
        mimetype="application/json",
    )
