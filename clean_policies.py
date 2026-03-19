from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib import error, request


ROOT = Path(__file__).parent
ENV_PATH = ROOT / ".env"
POLICIES_PATH = ROOT / "data" / "data" / "policies.json"
OUTPUT_PATH = ROOT / "cleaned_policies.json"

LIST_SECTIONS = [
    "approval_thresholds",
    "preferred_suppliers",
    "restricted_suppliers",
    "category_rules",
    "geography_rules",
    "escalation_rules",
]


def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def strip_json_wrapping(content: str) -> str:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        lines = cleaned.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
    return cleaned


def call_moonshot(messages: list[dict[str, str]]) -> dict[str, Any]:
    api_key = os.getenv("MOONSHOT_API_KEY")
    if not api_key:
        raise RuntimeError("MOONSHOT_API_KEY is not set. Load it via .env before running this script.")

    base_url = os.getenv("MOONSHOT_BASE_URL", "https://api.moonshot.ai/v1").rstrip("/")
    model = os.getenv("MOONSHOT_MODEL", "kimi-k2-turbo-preview")
    payload = {
        "model": model,
        "temperature": 0,
        "thinking": {"type": "disabled"},
        "response_format": {"type": "json_object"},
        "messages": messages,
    }
    req = request.Request(
        f"{base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=120) as response:
            raw = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Moonshot request failed with HTTP {exc.code}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Moonshot network error: {exc.reason}") from exc
    except TimeoutError as exc:
        raise RuntimeError("Moonshot request timed out.") from exc

    content = raw.get("choices", [{}])[0].get("message", {}).get("content")
    if not isinstance(content, str):
        raise RuntimeError("Moonshot response did not include message content.")
    try:
        result = json.loads(strip_json_wrapping(content))
    except json.JSONDecodeError as exc:
        raise RuntimeError("Moonshot response content was not valid JSON.") from exc
    if not isinstance(result, dict):
        raise RuntimeError("Moonshot response must be a JSON object.")
    return result


def get_identity(item: dict[str, Any], section: str) -> tuple[Any, ...]:
    identity_keys = {
        "approval_thresholds": ("threshold_id",),
        "preferred_suppliers": ("supplier_id", "category_l1", "category_l2"),
        "restricted_suppliers": ("supplier_id", "category_l1", "category_l2"),
        "category_rules": ("rule_id",),
        "geography_rules": ("rule_id",),
        "escalation_rules": ("rule_id",),
    }
    keys = identity_keys[section]
    return tuple(item.get(key) for key in keys)


def category_catalog_csv() -> str:
    return (ROOT / "data" / "data" / "categories.csv").read_text(encoding="utf-8")


def build_schema_system_prompt() -> str:
    return (
        "You design a normalization schema for procurement policy JSON. "
        "Return JSON only. "
        "You are not cleaning the file yet. "
        "Infer a small, reusable set of optional machine-readable fields that can be added to individual policy objects "
        "when their enforcement logic is partly described in natural language. "
        "Be conservative and generalizable across arbitrary policy sets."
    )


def build_schema_user_prompt(policies: dict[str, Any]) -> str:
    sample = {
        section: policies.get(section, [])[: min(len(policies.get(section, [])), 8)]
        for section in LIST_SECTIONS
    }
    return json.dumps(
        {
            "task": "Infer a reusable normalization schema and cleaning rules for this policy file.",
            "requirements": [
                "Return an object with keys: added_fields, section_guidance, global_rules.",
                "added_fields must be a map of field_name -> short purpose.",
                "section_guidance must explain how to clean each top-level policy list.",
                "global_rules must be a short list of strict invariants for per-object cleaning.",
                "Focus on fields that make natural-language enforcement machine-readable.",
                "Do not rewrite policy data here.",
            ],
            "category_catalog_csv": category_catalog_csv(),
            "policy_sample": sample,
        },
        ensure_ascii=False,
    )


def infer_schema(policies: dict[str, Any]) -> dict[str, Any]:
    return call_moonshot(
        [
            {"role": "system", "content": build_schema_system_prompt()},
            {"role": "user", "content": build_schema_user_prompt(policies)},
        ]
    )


def build_item_system_prompt() -> str:
    return (
        "You clean one procurement policy object at a time. "
        "Return JSON only with exactly one key: cleaned_item. "
        "Preserve all original fields and values unless adding machine-readable structure inferred from the text. "
        "Do not rename ids, do not drop fields, and do not change the object's identity. "
        "Only add fields strongly supported by the object's text and the provided schema guidance."
    )


def build_scope_system_prompt() -> str:
    return (
        "You infer category_l2 scope for one procurement policy rule. "
        "Return JSON only with exactly these keys: applies_to, scope_rationale. "
        "If the rule text clearly narrows applicability to specific category_l2 values, return them in applies_to. "
        "If the text does not clearly narrow applicability, return applies_to as null. "
        "Use only category_l2 values from the provided catalog."
    )


def build_item_user_prompt(
    section: str,
    item: dict[str, Any],
    schema: dict[str, Any],
    previous_item: dict[str, Any] | None,
    next_item: dict[str, Any] | None,
) -> str:
    return json.dumps(
        {
            "task": "Clean this single policy object into a more machine-readable version.",
            "section": section,
            "schema": schema,
            "category_catalog_csv": category_catalog_csv(),
            "rules": [
                "Return {'cleaned_item': <object>} only.",
                "Keep every original key and value unless adding structured fields.",
                "Do not remove or reorder arrays inside the object unless necessary to preserve original content.",
                "Do not change ids or category fields.",
                "If the natural language implies thresholds, approvals, applicability, residency constraints, or approved-provider conditions, add structured fields conservatively.",
            ],
            "neighbor_context": {
                "previous_item": previous_item,
                "next_item": next_item,
            },
            "item": item,
        },
        ensure_ascii=False,
    )


def build_scope_user_prompt(section: str, item: dict[str, Any]) -> str:
    return json.dumps(
        {
            "task": "Infer applies_to only when the rule text clearly narrows scope to specific category_l2 values.",
            "section": section,
            "category_catalog_csv": category_catalog_csv(),
            "rules": [
                "Use only exact category_l2 values from the catalog.",
                "If the text implies a broad but still category-specific family like cloud requests or end-user-device requests, map that family to the matching category_l2 list.",
                "If the text is not clearly category-l2-specific, return applies_to as null.",
                "Do not infer supplier, country, currency, or approval fields here.",
            ],
            "item": item,
        },
        ensure_ascii=False,
    )


def clean_item(
    section: str,
    item: dict[str, Any],
    schema: dict[str, Any],
    previous_item: dict[str, Any] | None,
    next_item: dict[str, Any] | None,
) -> dict[str, Any]:
    response = call_moonshot(
        [
            {"role": "system", "content": build_item_system_prompt()},
            {
                "role": "user",
                "content": build_item_user_prompt(section, item, schema, previous_item, next_item),
            },
        ]
    )
    cleaned_item = response.get("cleaned_item")
    if not isinstance(cleaned_item, dict):
        raise RuntimeError(f"Moonshot did not return a valid cleaned_item for section {section}.")
    return cleaned_item


def maybe_infer_applies_to(section: str, cleaned_item: dict[str, Any]) -> dict[str, Any]:
    if section not in {"geography_rules", "category_rules"}:
        return cleaned_item
    if cleaned_item.get("applies_to"):
        return cleaned_item
    if not (cleaned_item.get("rule_text") or cleaned_item.get("rule")):
        return cleaned_item

    response = call_moonshot(
        [
            {"role": "system", "content": build_scope_system_prompt()},
            {"role": "user", "content": build_scope_user_prompt(section, cleaned_item)},
        ]
    )
    applies_to = response.get("applies_to")
    scope_rationale = response.get("scope_rationale")
    if isinstance(applies_to, list) and applies_to:
        cleaned_item = dict(cleaned_item)
        cleaned_item["applies_to"] = applies_to
        if isinstance(scope_rationale, str) and scope_rationale.strip():
            cleaned_item["scope_rationale"] = scope_rationale.strip()
    return cleaned_item


def verify_cleaned_item(section: str, original: dict[str, Any], cleaned: dict[str, Any]) -> None:
    if get_identity(original, section) != get_identity(cleaned, section):
        raise RuntimeError(
            f"Identity changed in section {section}: {get_identity(original, section)} -> {get_identity(cleaned, section)}"
        )
    for key in original:
        if key not in cleaned:
            raise RuntimeError(f"Original key '{key}' missing from cleaned item in section {section}.")


def clean_policies(policies: dict[str, Any]) -> dict[str, Any]:
    schema = infer_schema(policies)
    cleaned = dict(policies)

    for section in LIST_SECTIONS:
        original_items = policies.get(section, [])
        cleaned_items: list[dict[str, Any]] = []
        for index, item in enumerate(original_items):
            previous_item = original_items[index - 1] if index > 0 else None
            next_item = original_items[index + 1] if index + 1 < len(original_items) else None
            cleaned_item = clean_item(section, item, schema, previous_item, next_item)
            cleaned_item = maybe_infer_applies_to(section, cleaned_item)
            verify_cleaned_item(section, item, cleaned_item)
            cleaned_items.append(cleaned_item)
        if len(cleaned_items) != len(original_items):
            raise RuntimeError(f"Item count changed in section {section}.")
        cleaned[section] = cleaned_items

    cleaned["_cleaning_notes"] = {
        "generated_by": "clean_policies.py",
        "source_file": str(POLICIES_PATH),
        "pipeline": [
            "global_schema_inference",
            "per_object_cleaning",
            "deterministic_merge_and_verification",
        ],
        "schema_summary": schema,
    }
    return cleaned


def main() -> None:
    load_dotenv_file(ENV_PATH)
    policies = load_json(POLICIES_PATH)
    cleaned = clean_policies(policies)
    OUTPUT_PATH.write_text(json.dumps(cleaned, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
