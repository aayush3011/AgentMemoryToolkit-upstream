"""Pure helpers shared between sync and async pipeline services.

Anything that's a function of its inputs only (no LLM/Cosmos/embedding IO)
lives here so :class:`PipelineService` and :class:`AsyncPipelineService` can
share it without duplication. The :class:`PromptyLoader` cache-and-prepare
helper is here too because it is pure (filesystem read + dict shaping); the
LLM call itself stays on the service.
"""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from azure.cosmos.agent_memory._embedding_tokens import count_tokens
from azure.cosmos.agent_memory.exceptions import LLMError
from azure.cosmos.agent_memory.logging import get_logger

logger = get_logger(__name__)

_NON_RETRYABLE_LLM_MARKERS = (
    "content_filter",
    "content management policy",
    "context_length_exceeded",
    "maximum context length",
)


def is_retryable_llm_error(exc: BaseException) -> bool:
    """Classify an extraction LLM failure as retryable (transient) or not."""
    text = str(exc).lower()
    return not any(marker in text for marker in _NON_RETRYABLE_LLM_MARKERS)


def batch_turns_by_tokens(
    items: list[dict[str, Any]],
    max_tokens: int,
    *,
    model: str = "gpt-5.4",
) -> list[list[dict[str, Any]]]:
    """Greedily pack ordered *items* into batches whose combined content stays
    within *max_tokens*.

    Token-bounded batching keeps each extraction call small enough that (a) the
    model can attend to every turn (more complete extraction - smaller windows
    extract more faithfully) and (b) a single poisoned turn fails only its own
    batch instead of the whole backlog. A turn larger than *max_tokens* on its
    own becomes a singleton batch (never dropped).
    """
    if not items:
        return []
    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_tokens = 0
    for item in items:
        item_tokens = count_tokens(str(item.get("content") or ""), model)
        if current and current_tokens + item_tokens > max_tokens:
            batches.append(current)
            current = []
            current_tokens = 0
        current.append(item)
        current_tokens += item_tokens
    if current:
        batches.append(current)
    return batches


# Separator for deterministic id seeds. Using NUL ensures user_id /
# thread_id values can never collide with literal section markers
# (e.g. a thread literally named ``"merged"`` cannot collide with the
# reconcile-merge id namespace). Defined as a module constant because
# escape sequences are not permitted inside f-strings on Python 3.11.
ID_SEED_SEP = "\x00"

# Mapping from prompty 2.x ModelOptions field names (camelCase) to the
# snake_case kwargs accepted by OpenAI's chat completions API. We include
# snake_case variants too because some prompty releases serialize options
# already lowercased.
PROMPTY_OPTION_ALIASES = {
    "topP": "top_p",
    "top_p": "top_p",
    "topK": "top_k",
    "top_k": "top_k",
    "frequencyPenalty": "frequency_penalty",
    "frequency_penalty": "frequency_penalty",
    "presencePenalty": "presence_penalty",
    "presence_penalty": "presence_penalty",
    "maxOutputTokens": "max_completion_tokens",
    "max_output_tokens": "max_completion_tokens",
    "maxTokens": "max_completion_tokens",
    "max_tokens": "max_completion_tokens",
    "stopSequences": "stop",
    "stop_sequences": "stop",
    "allowMultipleToolCalls": "parallel_tool_calls",
    "allow_multiple_tool_calls": "parallel_tool_calls",
}

_FRONT_MATTER_VERSION = re.compile(r"^version:\s*(\S+)\s*$", re.MULTILINE)
DEFAULT_PROMPT_VERSION = "v1"
_TOPIC_TAG_UNSAFE = re.compile(r"[^a-z0-9_:./-]+")


def build_topic_tags(values: Any) -> list[str]:
    tags: set[str] = set()
    for value in values or []:
        raw = str(value).strip().lower()
        if raw.startswith("topic:"):
            raw = raw[len("topic:") :]
        topic = _TOPIC_TAG_UNSAFE.sub("-", raw).strip("-")
        if topic:
            tags.add(f"topic:{topic}")
    return sorted(tags)


def is_real_number(v: Any) -> bool:
    """True for ``int``/``float`` excluding ``bool`` (``isinstance(True, int)`` is True)."""
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def max_or_none(values: Any) -> Optional[float]:
    """Return max of numeric values, ignoring None / non-numeric / bool. None if empty."""
    nums = [float(v) for v in values if is_real_number(v)]
    return max(nums) if nums else None


def chat_text(response: Any) -> str:
    """Extract assistant text from the chat client response.

    Sync/async ``ChatClient.generate`` returns a plain string. The remaining
    branches handle legacy dict/object shapes still emitted by mocks in
    the unit tests.
    """
    if response is None:
        raise LLMError("LLM returned no response")
    if isinstance(response, str):
        return response
    if isinstance(response, dict):
        content = response.get("content") or response.get("text")
        if isinstance(content, str):
            return content
        message = response.get("message")
        if isinstance(message, dict) and isinstance(message.get("content"), str):
            return message["content"]
        choices = response.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                first_message = first.get("message")
                if isinstance(first_message, dict) and isinstance(first_message.get("content"), str):
                    return first_message["content"]
                if isinstance(first.get("text"), str):
                    return first["text"]
    content_attr = getattr(response, "content", None)
    if isinstance(content_attr, str):
        return content_attr
    raise LLMError(f"LLM response did not contain text content: {type(response).__name__}")


def messages_to_dicts(messages: Any) -> list[dict[str, str]]:
    """Normalize prompty's prepared output to OpenAI-style message dicts.

    Prompty 2.x returns ``list[Message]`` dataclasses with ``role`` and
    ``parts`` (rich content parts). Older releases returned plain dicts.
    We collapse text parts into a single ``content`` string so the result
    is always the ``[{"role": ..., "content": ...}]`` shape OpenAI's
    chat completions API expects.
    """
    normalized: list[dict[str, str]] = []
    for msg in messages or []:
        if isinstance(msg, dict):
            normalized.append(msg)
            continue
        role = getattr(msg, "role", None)
        content = getattr(msg, "text", None)
        if content is None:
            parts = getattr(msg, "parts", None) or []
            content = "".join(getattr(part, "value", "") for part in parts)
        if role is None:
            continue
        normalized.append({"role": role, "content": content or ""})
    return normalized


def extract_prompty_params(p: Any) -> dict[str, Any]:
    """Pull model parameters from a Prompty object across library versions.

    - Prompty 2.x exposes ``model.options`` as a ``ModelOptions``
      dataclass with camelCase fields plus an ``additionalProperties``
      dict for things like ``response_format``.
    - Older 0.1.x releases expose ``model.parameters`` as a plain dict.

    We probe both, normalize camelCase → snake_case for known aliases,
    flatten ``additionalProperties``, and drop ``None`` values so the
    underlying ChatClient defaults still apply when a field is unset.
    """
    model = getattr(p, "model", None)
    if model is None:
        return {}

    # Prompty 0.1.x: parameters is already a dict.
    legacy = getattr(model, "parameters", None)
    if legacy:
        return {k: v for k, v in dict(legacy).items() if v is not None}

    options = getattr(model, "options", None)
    if options is None:
        return {}

    # Prompty 2.x: ModelOptions dataclass.
    try:
        import dataclasses

        raw = dataclasses.asdict(options) if dataclasses.is_dataclass(options) else dict(options)
    except Exception:
        raw = {}

    params: dict[str, Any] = {}
    for key, value in raw.items():
        if value is None:
            continue
        if key in ("additionalProperties", "additional_properties"):
            if isinstance(value, dict):
                params.update(value)
            continue
        if isinstance(value, list) and not value:
            continue
        params[PROMPTY_OPTION_ALIASES.get(key, key)] = value
    return params


def _normalize_metadata_keys(
    value: Optional[Iterable[str]],
) -> Optional[tuple[str, ...]]:
    """Validate + coerce a ``transcript_metadata_keys`` argument to a tuple.

    Rejects ``str`` outright (a bare string is iterable char-by-char, which
    would silently produce a one-letter allow-list). Returns ``None`` for
    empty or missing input.
    """
    if value is None:
        return None
    if isinstance(value, str):
        raise TypeError(
            "transcript_metadata_keys must be a sequence of keys "
            "(list/tuple/set), not a single str. "
            f"Got: {value!r}. Did you mean [{value!r}]?"
        )
    keys = tuple(str(k) for k in value if str(k))
    return keys or None


def _normalize_cadence_thresholds(
    value: Optional[Mapping[str, int]],
) -> Optional[dict[str, int]]:
    """Validate + defensively copy a ``cadence_thresholds`` argument.

    Coerces each value to ``int`` and rejects negatives (``0`` disables the
    corresponding step). Returns a new ``dict`` so later mutation of the
    caller's mapping cannot change client behavior. Returns ``None`` for
    missing or empty input.
    """
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError(
            "cadence_thresholds must be a mapping of env-var name to int "
            f"(e.g. {{'FACT_EXTRACTION_EVERY_N': 5}}), not {type(value).__name__}."
        )
    normalized: dict[str, int] = {}
    for key, raw in value.items():
        try:
            coerced = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"cadence_thresholds[{key!r}] must be an int, got {raw!r}.") from exc
        if coerced < 0:
            raise ValueError(f"cadence_thresholds[{key!r}] must be >= 0 (0 disables the step), got {coerced}.")
        normalized[str(key)] = coerced
    return normalized or None


def _format_metadata_segment(
    metadata: Any,
    metadata_keys: Optional[tuple[str, ...]],
) -> str:
    """Render the trailing ``[metadata: {...}]`` segment for a transcript line.

    Returns an empty string unless ``metadata_keys`` is a non-empty tuple
    AND at least one of those keys is present in ``metadata``. Only the
    explicitly allow-listed keys are serialized, in the iteration order of
    ``metadata_keys``.
    """
    if not metadata_keys or not isinstance(metadata, dict):
        return ""
    filtered = {k: metadata[k] for k in metadata_keys if k in metadata}
    if not filtered:
        return ""
    payload = json.dumps(filtered, separators=(",", ":"), ensure_ascii=False, default=str)
    return f" [metadata: {payload}]"


def build_transcript(
    items: list[dict[str, Any]],
    *,
    group_by_thread: bool = False,
    metadata_keys: Optional[Iterable[str]] = None,
    include_timestamp: bool = False,
) -> str:
    """Build a formatted transcript from memory documents.

    Parameters
    ----------
    items:
        Memory dicts with ``role``, ``content``, and optional ``metadata``.
    group_by_thread:
        If *True*, group messages under ``=== Thread <id> ===`` headers.
    metadata_keys:
        Allow-list of metadata keys to surface in each transcript line.
        Defaults to ``None`` (no metadata serialized - only ``[role]:
        content`` lines). When provided, only the listed keys are emitted,
        in iteration order. Keys absent from a given turn's metadata are
        silently skipped.

        Set this when callers stash semantically useful breadcrumbs in
        ``TurnRecord.metadata`` that the extraction LLM should see
        (e.g. ``["agent_id", "timestamp"]``). Leaving it unset keeps free-form
        metadata blobs (raw tool calls, IDE schema, etc.) out of every
        prompt - they're often 10-100x larger than the dialog itself and
        dilute extraction quality.

        Accepts any iterable of strings except ``str`` itself (which would
        be interpreted char-by-char). Generators are coerced to a tuple so
        the allow-list is reusable across turns.
    include_timestamp:
        If *True*, prefix each line with the turn's top-level ``created_at``
        (its event time) as ``[<created_at> | role]: content``. This lets the
        extraction LLM anchor relative time expressions ("3 weeks ago", "last
        June") to absolute dates instead of leaving them unresolved. Turns
        without a ``created_at`` fall back to the plain ``[role]:`` form.
    """
    keys = _normalize_metadata_keys(metadata_keys)

    def _line(m: dict[str, Any]) -> str:
        role = _canonical_speaker(m.get("role", "unknown"))
        content = m.get("content", "")
        meta_str = _format_metadata_segment(m.get("metadata", {}), keys)
        created_at = m.get("created_at") if include_timestamp else None
        prefix = f"[{created_at} | {role}]" if created_at else f"[{role}]"
        return f"{prefix}: {content}{meta_str}"

    if not group_by_thread:
        return "\n".join(_line(m) for m in items)

    threads: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for m in items:
        threads[m.get("thread_id", "")].append(m)

    parts: list[str] = []
    for tid, thread_items in threads.items():
        parts.append(f"=== Thread {tid} ===")
        for m in thread_items:
            parts.append(_line(m))
        parts.append("")
    return "\n".join(parts)


# Map common role synonyms onto the toolkit's canonical speaker labels. Callers
# may write turns with any role string (e.g. OpenAI's ``assistant``); the
# extraction prompt reasons about ``user`` vs ``agent``, so synonyms are folded
# onto those. ``tool`` and ``system`` are distinct roles and kept as-is;
# unrecognized labels pass through unchanged rather than being misattributed.
_SPEAKER_ALIASES = {
    "user": "user",
    "human": "user",
    "customer": "user",
    "end_user": "user",
    "person": "user",
    "agent": "agent",
    "assistant": "agent",
    "ai": "agent",
    "bot": "agent",
    "chatbot": "agent",
    "model": "agent",
    "copilot": "agent",
    "tool": "tool",
    "system": "system",
}


def _canonical_speaker(role: Any) -> str:
    """Normalize a free-form role to the canonical speaker label for prompts."""
    normalized = str(role or "").strip().lower()
    return _SPEAKER_ALIASES.get(normalized, str(role or "unknown"))


def parse_llm_json(text: str | None) -> dict[str, Any]:
    """Parse JSON from an LLM response, stripping markdown fences."""
    if text is None:
        raise LLMError("LLM returned no content (None response body)")
    cleaned = text.strip()
    if cleaned.startswith("```"):
        first_newline = cleaned.find("\n")
        if first_newline >= 0:
            cleaned = cleaned[first_newline + 1 :]
        else:
            cleaned = cleaned.lstrip("`").lstrip()
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()
    decoder = json.JSONDecoder()
    try:
        obj, end = decoder.raw_decode(cleaned)
    except json.JSONDecodeError as exc:
        preview = (text or "")[:200].replace("\n", " ")
        if _looks_truncated(cleaned, exc):
            raise LLMError(
                "LLM JSON output appears TRUNCATED (decode error at the very end of a "
                f"{len(cleaned)}-char body - the model almost certainly hit its output-token "
                "cap mid-object). Increase 'maxOutputTokens' in the calling prompty, or reduce "
                "the amount of input per call (e.g. lower the fact-extraction batch size / "
                f"recent_k, or split oversized turns). Decode error: {exc}. preview={preview!r}"
            ) from exc
        raise LLMError(f"LLM returned invalid JSON (preview={preview!r}): {exc}") from exc
    remainder = cleaned[end:]
    if not remainder.strip():
        # Fast path: exactly one JSON object, no trailing content (the vast
        # majority of responses). Behaviour identical to before.
        return obj

    # Some models (notably under strict json_schema on certain deployments) emit
    # MULTIPLE back-to-back top-level JSON objects for a single call - e.g.
    # ``{"facts":[...]}{"facts":[...]}``. ``raw_decode`` only returns the first,
    # so keeping just it would silently DROP every item in the trailing
    # object(s) - and the batch's turns get stamped ``extracted_at``, so those
    # facts are never recovered. Instead, decode every top-level object and merge
    # them: list-valued keys (``facts``, ``events``, ...) are concatenated and any
    # other/scalar key keeps its first-seen value. Downstream exact-dup hashing
    # removes any repeats, so merging is always safe.
    if not isinstance(obj, dict):
        # Non-object JSON root with trailing data: cannot merge into a dict;
        # preserve the historical behaviour (keep the first value, warn).
        leftover = remainder.strip()
        logger.warning(
            "LLM response had %d chars of extra data after a non-object JSON root; using the "
            "first value and ignoring the remainder (trailing_preview=%r)",
            len(leftover),
            leftover[:120].replace("\n", " "),
        )
        return obj

    merged: dict[str, Any] = obj
    object_count = 1
    pos = end
    length = len(cleaned)
    while pos < length:
        # Skip inter-object whitespace and stray separators between objects.
        while pos < length and cleaned[pos] in " \t\r\n,":
            pos += 1
        if pos >= length:
            break
        try:
            nxt, pos = decoder.raw_decode(cleaned, pos)
        except json.JSONDecodeError:
            leftover = cleaned[pos:].strip()
            logger.warning(
                "LLM response had %d chars of non-JSON trailing data after %d concatenated "
                "JSON object(s); merged the objects and ignored the remainder "
                "(trailing_preview=%r)",
                len(leftover),
                object_count,
                leftover[:120].replace("\n", " "),
            )
            break
        object_count += 1
        if isinstance(nxt, dict):
            _merge_json_objects(merged, nxt)
        # A non-dict follow-on cannot be merged into a dict result; skip it.

    if object_count > 1:
        logger.info(
            "LLM response contained %d concatenated JSON objects; merged their list fields into "
            "one result so no items were dropped.",
            object_count,
        )
    return merged


def _merge_json_objects(base: dict[str, Any], extra: dict[str, Any]) -> None:
    """Merge ``extra`` into ``base`` in place.

    List-valued keys present in both are concatenated (this is what recovers the
    facts/events an LLM splits across multiple concatenated JSON objects); keys
    only in ``extra`` are added; any other conflict keeps ``base``'s (first-seen)
    value. Exact-duplicate items are pruned later by the pipeline's content-hash
    dedup, so concatenating without de-duping here is safe.
    """
    for key, value in extra.items():
        if key not in base:
            base[key] = value
        elif isinstance(base[key], list) and isinstance(value, list):
            base[key].extend(value)


def _looks_truncated(cleaned: str, exc: json.JSONDecodeError) -> bool:
    """Heuristic: did the JSON fail because the model ran out of output tokens?"""
    if not cleaned:
        return False
    unbalanced = cleaned.count("{") > cleaned.count("}") or cleaned.count("[") > cleaned.count("]")
    unterminated_string = "Unterminated string" in str(exc)
    return unbalanced or unterminated_string


def default_prompts_dir() -> str:
    """Default ``prompts/`` directory location: under ``azure/cosmos/agent_memory/``."""
    pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(pkg_dir, "prompts")


_EXTRACT_MEMORIES_PROMPT_DEFAULT = "extract_memories-v2.prompty"
_EXTRACT_MEMORIES_PROMPT_ALLOWED = frozenset(
    {"extract_memories.prompty", "extract_memories-v2.prompty"}
)


def extract_memories_prompt_file() -> str:
    """Return the fact-extraction prompt filename, env-selectable.

    Defaults to the v2 ``extract_memories-v2.prompty`` extractor (higher recall;
    also captures assistant-provided information: lists, tables, instructions,
    and researched answers the user may later reference). Set
    ``AMT_EXTRACT_MEMORIES_PROMPT=extract_memories.prompty`` to fall back to the
    v1 extractor without touching the default. An unknown value falls back to
    the v2 default so a typo can never point extraction at an arbitrary or
    missing prompt file. The chosen filename must be registered in
    ``PROMPTY_SCHEMAS`` so it still gets the structured-output response format.
    """
    name = os.environ.get("AMT_EXTRACT_MEMORIES_PROMPT", _EXTRACT_MEMORIES_PROMPT_DEFAULT)
    return name if name in _EXTRACT_MEMORIES_PROMPT_ALLOWED else _EXTRACT_MEMORIES_PROMPT_DEFAULT


def _read_prompty_version(path: str | Path) -> str:
    """Read the ``version:`` key from a prompty file's YAML front-matter."""
    text = Path(path).read_text(encoding="utf-8")
    if text.startswith("---"):
        end = text.find("\n---", 3)
        if end != -1:
            text = text[:end]
    match = _FRONT_MATTER_VERSION.search(text)
    return match.group(1) if match else DEFAULT_PROMPT_VERSION


class PromptyLoader:
    """Caching prompty template loader.

    Pure-IO module-aware: only reads the filesystem; never calls the LLM.
    Shared by sync and async pipeline services so the prepared (messages,
    params) pair has identical formatting on both code paths.
    """

    def __init__(self, prompts_dir: str | None = None) -> None:
        self._prompts_dir = prompts_dir if prompts_dir is not None else default_prompts_dir()
        self._cache: dict[str, Any] = {}
        self._version_cache: dict[str, str] = {}

    @property
    def prompts_dir(self) -> str:
        return self._prompts_dir

    def _path_for(self, filename: str) -> str:
        return os.path.join(self._prompts_dir, filename)

    def load(self, filename: str) -> Any:
        cached = self._cache.get(filename)
        if cached is not None:
            return cached
        import prompty

        loaded = prompty.load(self._path_for(filename))
        self._disable_strict_format(prompty, loaded)
        self._cache[filename] = loaded
        return loaded

    def _disable_strict_format(self, prompty: Any, loaded: Any) -> None:
        """Disable prompty strict nonce parsing for SDK-owned internal prompts."""
        template = getattr(loaded, "template", None)
        if template is None:
            template_type = getattr(prompty, "Template", None)
            if template_type is None:
                return
            template = template_type()
            loaded.template = template

        format_config = getattr(template, "format", None)
        if format_config is None:
            format_type = getattr(prompty, "FormatConfig", None)
            if format_type is None:
                return
            format_config = format_type()
            template.format = format_config

        if hasattr(format_config, "strict"):
            format_config.strict = False

    def prompt_version(self, filename: str) -> str:
        """Return the ``version:`` declared in the prompty front-matter."""
        cached = self._version_cache.get(filename)
        if cached is not None:
            return cached
        version = _read_prompty_version(self._path_for(filename))
        self._version_cache[filename] = version
        return version

    def prepare(self, filename: str, inputs: dict[str, Any]) -> tuple[list[dict[str, str]], dict[str, Any]]:
        """Render a prompty template and return ``(messages, model_params)``."""
        import prompty

        p = self.load(filename)
        messages = messages_to_dicts(prompty.prepare(p, inputs=inputs))
        params = extract_prompty_params(p)
        return messages, params


# Legacy valence helpers retained for the async v4 extraction path while that
# path still accepts old episodic prompt fields.
VALID_VALENCES = frozenset({"positive", "negative", "neutral", "mixed"})


def coerce_valence(value: Any) -> str:
    """Map a legacy LLM-emitted ``outcome_valence`` to a safe value.

    Null and any unknown value fall through to ``"neutral"`` so a single
    drifted legacy extraction never aborts the whole batch.
    """
    if isinstance(value, str) and value in VALID_VALENCES:
        return value
    return "neutral"


# Per-section caps on the persisted ``structured_summary``. Strict-mode JSON
# output does not enforce ``maxItems``, so the LLM grows lists unboundedly
# across incremental updates. Capping at persist time keeps both Cosmos
# document size and the next call's ``prior_summary`` prompt bounded.
SUMMARY_LIST_CAPS: dict[str, int] = {
    "key_facts": 50,
    "personal_preferences": 30,
    "account_environment": 30,
    "goals_current_work": 30,
    "behavioral_patterns": 30,
    "compliance_requirements": 30,
    "open_items": 30,
    "topics": 15,
    "goals": 30,
    "relationships": 30,
    "entities": 30,
}
SUMMARY_DEFAULT_CAP = 30


def cap_structured_summary(parsed: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Truncate every list field of a parsed summary to its configured cap.

    Returns a shallow copy with list fields replaced by their first-N slice.
    Non-list values are passed through unchanged. ``None`` returns ``None``.
    """
    if not isinstance(parsed, dict):
        return parsed
    out = dict(parsed)
    for key, value in list(out.items()):
        if isinstance(value, list):
            cap = SUMMARY_LIST_CAPS.get(key, SUMMARY_DEFAULT_CAP)
            if len(value) > cap:
                out[key] = value[:cap]
    return out
