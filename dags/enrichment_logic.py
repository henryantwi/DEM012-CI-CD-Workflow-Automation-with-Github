"""
Helper functions for AI enrichment retry, rate-limit handling, and fallback behavior.

Kept Airflow-free so the logic can be unit-tested locally without installing Airflow.
"""

from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

TRUE_VALUES = {"1", "true", "yes", "on"}


class CategoryResult(Protocol):
    category: str
    confidence: float


@dataclass(frozen=True)
class EnrichmentConfig:
    fail_open: bool
    fallback_category: str
    requests_per_second: float
    max_attempts: int
    retry_base_seconds: float
    retry_max_seconds: float

    @property
    def min_request_interval(self) -> float:
        return 1.0 / max(self.requests_per_second, 0.1)


def build_enrichment_config_from_env(
    env: Mapping[str, str] | None = None,
) -> EnrichmentConfig:
    source = os.environ if env is None else env
    return EnrichmentConfig(
        fail_open=source.get("AI_ENRICHMENT_FAIL_OPEN", "true").lower() in TRUE_VALUES,
        fallback_category=source.get("AI_ENRICHMENT_FALLBACK_CATEGORY", "Other"),
        requests_per_second=float(source.get("GROQ_REQUESTS_PER_SECOND", "0.8")),
        max_attempts=int(source.get("GROQ_MAX_ATTEMPTS", "8")),
        retry_base_seconds=float(source.get("GROQ_RETRY_BASE_SECONDS", "2.0")),
        retry_max_seconds=float(source.get("GROQ_RETRY_MAX_SECONDS", "30.0")),
    )


def is_rate_limited_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "429" in message or "rate limit" in message or "too many requests" in message


def invoke_with_rate_limit_and_retries(
    llm: Any,
    prompt: str,
    config: EnrichmentConfig,
    state: dict[str, float] | None = None,
    log: logging.Logger | None = None,
) -> CategoryResult:
    logger = log or logging.getLogger(__name__)
    timer_state = state if state is not None else {}

    for attempt in range(1, config.max_attempts + 1):
        last_request_at = timer_state.get("last_request_at", 0.0)
        elapsed = time.monotonic() - last_request_at
        if elapsed < config.min_request_interval:
            time.sleep(config.min_request_interval - elapsed)

        try:
            result: CategoryResult = llm.invoke(prompt)
            timer_state["last_request_at"] = time.monotonic()
            return result
        except Exception as exc:  # noqa: BLE001
            if (not is_rate_limited_error(exc)) or attempt == config.max_attempts:
                raise

            backoff = min(
                config.retry_max_seconds,
                config.retry_base_seconds * (2 ** (attempt - 1)),
            )
            sleep_seconds = backoff + random.uniform(0, backoff * 0.5)
            logger.warning(
                "[enrich_ai] Rate limit hit, retrying in %.1fs (attempt %s/%s)",
                sleep_seconds,
                attempt,
                config.max_attempts,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError("[enrich_ai] Unreachable retry guard")


def enrich_products_with_llm(
    products: list[dict[str, Any]],
    funnel_records: list[dict[str, Any]],
    llm: Any | None,
    config: EnrichmentConfig,
    log: logging.Logger | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    logger = log or logging.getLogger(__name__)

    if llm is None:
        if config.fail_open:
            for row in funnel_records:
                row["category"] = config.fallback_category
            for row in products:
                row["category"] = config.fallback_category
            return funnel_records, products, {"unique_prompts": 0, "fallbacks": 0}
        raise ValueError("GROQ_API_KEY is not set and AI_ENRICHMENT_FAIL_OPEN=false")

    seen: dict[str, str] = {}
    categorised: dict[str, str] = {}
    fallback_count = 0
    invoke_state: dict[str, float] = {"last_request_at": 0.0}

    for product in products:
        pid = product["product_id"]
        name = product["name"]
        desc = product.get("description", "")

        if name in seen:
            categorised[pid] = seen[name]
            continue

        prompt = (
            f"Product name: {name}\n"
            f"Description: {desc}\n\n"
            "Classify this product into the most fitting category."
        )

        try:
            result = invoke_with_rate_limit_and_retries(
                llm=llm,
                prompt=prompt,
                config=config,
                state=invoke_state,
                log=logger,
            )
            seen[name] = result.category
            categorised[pid] = result.category
            logger.info(
                "[enrich_ai] %r -> %s (confidence: %.2f)",
                name,
                result.category,
                result.confidence,
            )
        except Exception as exc:  # noqa: BLE001
            if not config.fail_open:
                raise
            fallback_count += 1
            seen[name] = config.fallback_category
            categorised[pid] = config.fallback_category
            logger.warning(
                "[enrich_ai] fallback for %r: %s. Using category=%r",
                name,
                exc,
                config.fallback_category,
            )

    for row in funnel_records:
        row["category"] = categorised.get(row["product_id"], config.fallback_category)

    for row in products:
        row["category"] = categorised.get(row["product_id"], config.fallback_category)

    return funnel_records, products, {"unique_prompts": len(seen), "fallbacks": fallback_count}
