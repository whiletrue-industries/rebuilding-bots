import json
import re
from typing import Any

from .config import get_async_openai_client, get_logger, get_openai_client
from ._concurrency import async_retry_openai

logger = get_logger(__name__)


_DEFAULT_TEMPLATE = """{
    "DocumentTitle": "",
    "Summary": "",
    "PublicationDate": "",
    "OfficialSource": "",
    "ReferenceLinks": [],
    "ClauseRepresentation": "",
    "OfficialRoles": [
      {
        "Role": "",
        "ClauseLocation": "",
        "Quote": ""
      }
    ],
    "OfficialOrganizations": [
      {
        "Organization": "",
        "ClauseLocation": "",
        "Quote": ""
      }
    ],
    "Placenames": [
      {
        "Name": "",
        "ClauseLocation": "",
        "Quote": ""
      }
    ],
    "LegalReferences": [
    {
      "ReferenceTitle": "",
      "ReferenceText": "",
      "ReferenceQuote": ""
    }
  ],
  "Amendments": [],
  "AdditionalKeywords": [],
  "Topics": []
}"""


def _build_system_message(text: str, template: str | None, document_type: str | None) -> str:
    if template is None:
        template = _DEFAULT_TEMPLATE
    return f"""You are a highly accurate legal text extraction engine. Your task is to extract all relevant metadata from the provided legal text according to the JSON template below. Follow these rules exactly:

        1. Use only the information given in the text.
        2. Output must be valid JSON that exactly follows the provided schema—do not add any extra keys or commentary.
        3. Ensure all special characters, especially quotes within text, are properly escaped.
        4. At the document level (DocumentMetadata), extract:
            - "DocumentTitle": Extract the main title or heading that describes the document content in a single line. This should be descriptive of what the document is about.
            - "Summary": Create a comprehensive paragraph summarizing the essence and output of the document. Include the background, main issues discussed, and final results/decisions/conclusions. This should capture the document's purpose, process, and outcome in one cohesive paragraph.
            - "OfficialSource" from any indicated section (e.g. "סעיף 137") and include any associated URL in "ReferenceLinks".
            - "ClauseRepresentation" should indicate whether the metadata pertains to a main clause, sub-clause, or specific section.
            - Extract any official roles/positions mentioned in the document and list them in "OfficialRoles".
            - Extract any official organizations mentioned in the document and list them in "OfficialOrganizations".
            - Extract any real-world locations or placenames mentioned in the document and list them in "Placenames".

        5. At the document level, also extract:
            - "LegalReferences": For each legal reference
            - "Amendments": If any amendment information is present
            - "AdditionalKeywords": Extract key legal terms, topics, and identifiers
            - "Topics": Aggregate the main topics discussed in the document as one or two word entries.
        6. For any field where no data is provided, return an empty string or an empty array as appropriate.
        7. Do not infer or generate data that is not explicitly provided.
        8. Ensure all key names follow standard, consistent naming.
        9. Output only the JSON.

        Extraction Template:
        {template}

        Text:
        {text}

        Output (JSON only):"""


def _parse_response_content(raw_content: str) -> dict[str, Any]:
    """Parse the JSON response from OpenAI, with the legacy Hebrew-quote fallback."""
    try:
        extracted_data = json.loads(raw_content)
        logger.info(
            "Successfully extracted structured content: %s...\n",
            json.dumps(extracted_data, ensure_ascii=False),
        )
        return extracted_data
    except json.JSONDecodeError as e:
        logger.error("Failed to parse API response as JSON: %s -->", e)
        logger.error("Response content: %s", raw_content)
        # Recovery: the older Hebrew-quote normalization path.
        try:
            content = re.sub(r'(["]\w+)["]([\w\s]+["]\w+)', r'\1\"\2', raw_content)
            extracted_data = json.loads(content)
            logger.info(
                "Successfully parsed JSON after fixing: %s",
                json.dumps(extracted_data, ensure_ascii=False),
            )
            return extracted_data
        except Exception as recovery_error:
            logger.error("Recovery attempt failed: %s", recovery_error)
            return {
                "DocumentMetadata": {
                    "DocumentTitle": "Parsing Error",
                    "Description": "Failed to parse API response",
                },
                "error": str(e),
                "raw_content": raw_content,
            }


def extract_structured_content(
    text: str,
    template: str | None = None,
    document_type: str | None = None,
) -> dict[str, Any]:
    """Sync extraction — kept for callers that don't run inside an event loop.

    The concurrent sync pipeline (collect_sources.collect_context_sources_async)
    uses ``extract_structured_content_async`` directly.
    """
    try:
        client = get_openai_client()
        logger.info("Extracting structured content for document type: %s", document_type)
        system_message = _build_system_message(text, template, document_type)
        logger.info("Calling OpenAI API for content extraction")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system_message}],
            temperature=0.0,
            max_tokens=2000,
            stream=False,
            response_format={"type": "json_object"},
        )
        return _parse_response_content(response.choices[0].message.content)
    except Exception as e:
        logger.error("Error in extract_structured_content: %s", e)
        return {"error": str(e)}


@async_retry_openai()
async def _async_chat_completion(client, system_message: str):
    """Raw call, isolated so the retry decorator wraps exactly one network op."""
    return await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": system_message}],
        temperature=0.0,
        max_tokens=2000,
        stream=False,
        response_format={"type": "json_object"},
    )


async def extract_structured_content_async(
    text: str,
    template: str | None = None,
    document_type: str | None = None,
    *,
    client=None,
) -> dict[str, Any]:
    """Async counterpart to ``extract_structured_content``.

    The caller is expected to have acquired the concurrency semaphore
    before calling this (see SyncConcurrency.run_bounded). 429s and other
    transient errors are handled by the ``async_retry_openai`` decorator
    on ``_async_chat_completion``; other exceptions propagate so the
    caller can record them on the individual document without poisoning
    the batch.
    """
    try:
        if client is None:
            client = get_async_openai_client()
        logger.info("Extracting structured content (async) for document type: %s", document_type)
        system_message = _build_system_message(text, template, document_type)
        response = await _async_chat_completion(client, system_message)
        return _parse_response_content(response.choices[0].message.content)
    except Exception as e:
        logger.error("Error in extract_structured_content_async: %s", e)
        return {"error": str(e)}
