from __future__ import annotations

from dataclasses import dataclass

import pytest

from dags.enrichment_logic import (
    EnrichmentConfig,
    enrich_products_with_llm,
    invoke_with_rate_limit_and_retries,
)


@dataclass
class DummyCategoryResult:
    category: str
    confidence: float = 0.8


class SequenceLLM:
    def __init__(self, responses: list[object]):
        self._responses = responses
        self.calls = 0

    def invoke(self, prompt: str):
        response = self._responses[self.calls]
        self.calls += 1
        if isinstance(response, Exception):
            raise response
        return response


class PromptAwareLLM:
    def __init__(self):
        self.calls = 0

    def invoke(self, prompt: str):
        self.calls += 1
        if "Product name: Alpha" in prompt:
            return DummyCategoryResult(category="Electronics")
        return DummyCategoryResult(category="Home & Kitchen")


def make_config(**overrides) -> EnrichmentConfig:
    base = {
        "fail_open": True,
        "fallback_category": "Other",
        "requests_per_second": 1000.0,
        "max_attempts": 4,
        "retry_base_seconds": 2.0,
        "retry_max_seconds": 30.0,
    }
    base.update(overrides)
    return EnrichmentConfig(**base)


def sample_records():
    products = [
        {"product_id": "p1", "name": "Alpha", "description": "A"},
        {"product_id": "p2", "name": "Alpha", "description": "A duplicate"},
        {"product_id": "p3", "name": "Beta", "description": "B"},
    ]
    funnel = [
        {"product_id": "p1"},
        {"product_id": "p2"},
        {"product_id": "p3"},
    ]
    return products, funnel


def test_enrich_products_missing_llm_fail_open_true():
    products, funnel = sample_records()
    config = make_config(fail_open=True, fallback_category="Fallback")

    out_funnel, out_products, stats = enrich_products_with_llm(
        products=products,
        funnel_records=funnel,
        llm=None,
        config=config,
    )

    assert all(row["category"] == "Fallback" for row in out_funnel)
    assert all(row["category"] == "Fallback" for row in out_products)
    assert stats == {"unique_prompts": 0, "fallbacks": 0}


def test_enrich_products_missing_llm_fail_open_false_raises():
    products, funnel = sample_records()
    config = make_config(fail_open=False)

    with pytest.raises(ValueError, match="GROQ_API_KEY is not set"):
        enrich_products_with_llm(
            products=products,
            funnel_records=funnel,
            llm=None,
            config=config,
        )


def test_invoke_retries_on_429_then_succeeds(monkeypatch):
    llm = SequenceLLM(
        [
            Exception("429 Too Many Requests"),
            DummyCategoryResult(category="Electronics"),
        ]
    )
    config = make_config(max_attempts=3, retry_base_seconds=2.0, retry_max_seconds=10.0)
    sleeps: list[float] = []

    monkeypatch.setattr("dags.enrichment_logic.random.uniform", lambda a, b: 0.0)
    monkeypatch.setattr("dags.enrichment_logic.time.sleep", lambda sec: sleeps.append(sec))

    result = invoke_with_rate_limit_and_retries(
        llm=llm,
        prompt="Product name: Alpha",
        config=config,
        state={"last_request_at": 0.0},
    )

    assert result.category == "Electronics"
    assert llm.calls == 2
    assert sleeps == [2.0]


def test_invoke_does_not_retry_non_rate_limit(monkeypatch):
    llm = SequenceLLM([Exception("connection reset")])
    config = make_config(max_attempts=5)
    sleeps: list[float] = []

    monkeypatch.setattr("dags.enrichment_logic.time.sleep", lambda sec: sleeps.append(sec))

    with pytest.raises(Exception, match="connection reset"):
        invoke_with_rate_limit_and_retries(
            llm=llm,
            prompt="Product name: Alpha",
            config=config,
            state={"last_request_at": 0.0},
        )

    assert sleeps == []
    assert llm.calls == 1


def test_rate_pacing_sleeps_when_calls_too_close(monkeypatch):
    llm = SequenceLLM([DummyCategoryResult(category="Electronics")])
    config = make_config(requests_per_second=2.0)
    sleeps: list[float] = []
    monotonic_values = iter([10.2, 10.2])

    monkeypatch.setattr("dags.enrichment_logic.time.monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr("dags.enrichment_logic.time.sleep", lambda sec: sleeps.append(sec))

    state = {"last_request_at": 10.0}
    result = invoke_with_rate_limit_and_retries(
        llm=llm,
        prompt="Product name: Alpha",
        config=config,
        state=state,
    )

    assert result.category == "Electronics"
    assert sleeps == pytest.approx([0.3], rel=1e-6)
    assert state["last_request_at"] == pytest.approx(10.2, rel=1e-6)


def test_enrich_products_deduplicates_by_name():
    products, funnel = sample_records()
    llm = PromptAwareLLM()
    config = make_config(fail_open=False)

    out_funnel, out_products, stats = enrich_products_with_llm(
        products=products,
        funnel_records=funnel,
        llm=llm,
        config=config,
    )

    assert llm.calls == 2
    assert stats["unique_prompts"] == 2
    assert stats["fallbacks"] == 0

    categories_by_pid = {row["product_id"]: row["category"] for row in out_products}
    assert categories_by_pid["p1"] == "Electronics"
    assert categories_by_pid["p2"] == "Electronics"
    assert categories_by_pid["p3"] == "Home & Kitchen"

    funnel_categories = {row["product_id"]: row["category"] for row in out_funnel}
    assert funnel_categories == categories_by_pid


def test_enrich_products_fail_open_applies_fallback_after_retry_exhaustion(monkeypatch):
    products, funnel = sample_records()
    llm = SequenceLLM(
        [
            Exception("429 Too Many Requests"),
            Exception("429 Too Many Requests"),
            Exception("429 Too Many Requests"),
            Exception("429 Too Many Requests"),
            Exception("429 Too Many Requests"),
            Exception("429 Too Many Requests"),
            Exception("429 Too Many Requests"),
            Exception("429 Too Many Requests"),
        ]
    )
    config = make_config(fail_open=True, max_attempts=4, fallback_category="Other")

    monkeypatch.setattr("dags.enrichment_logic.random.uniform", lambda a, b: 0.0)
    monkeypatch.setattr("dags.enrichment_logic.time.sleep", lambda _sec: None)

    out_funnel, out_products, stats = enrich_products_with_llm(
        products=products,
        funnel_records=funnel,
        llm=llm,
        config=config,
    )

    assert all(row["category"] == "Other" for row in out_funnel)
    assert all(row["category"] == "Other" for row in out_products)
    assert stats["fallbacks"] == 2
    assert stats["unique_prompts"] == 2


def test_enrich_products_strict_mode_raises_after_retry_exhaustion(monkeypatch):
    products, funnel = sample_records()
    llm = SequenceLLM(
        [
            Exception("429 Too Many Requests"),
            Exception("429 Too Many Requests"),
            Exception("429 Too Many Requests"),
            Exception("429 Too Many Requests"),
        ]
    )
    config = make_config(fail_open=False, max_attempts=2)

    monkeypatch.setattr("dags.enrichment_logic.random.uniform", lambda a, b: 0.0)
    monkeypatch.setattr("dags.enrichment_logic.time.sleep", lambda _sec: None)

    with pytest.raises(Exception, match="429 Too Many Requests"):
        enrich_products_with_llm(
            products=products,
            funnel_records=funnel,
            llm=llm,
            config=config,
        )
