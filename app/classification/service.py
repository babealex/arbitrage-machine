from __future__ import annotations

import json

import requests

from app.events.models import ClassifiedEvent, NewsItem
from app.classification.rule_engine import classify_with_rules


class EventClassifier:
    def __init__(
        self,
        enable_openai: bool = False,
        openai_api_key: str = "",
        openai_model: str = "gpt-5.4-mini",
    ) -> None:
        self.enable_openai = enable_openai and bool(openai_api_key)
        self.openai_api_key = openai_api_key
        self.openai_model = openai_model

    def classify(self, item: NewsItem) -> ClassifiedEvent | None:
        rule_based = classify_with_rules(item)
        if not self.enable_openai:
            return rule_based
        llm_classification = self._classify_with_openai(item)
        return llm_classification or rule_based

    def _classify_with_openai(self, item: NewsItem) -> ClassifiedEvent | None:
        try:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers={
                    "Authorization": f"Bearer {self.openai_api_key}",
                    "Content-Type": "application/json",
                },
                timeout=10,
                json={
                    "model": self.openai_model,
                    "input": [
                        {
                            "role": "system",
                            "content": [
                                {
                                    "type": "input_text",
                                    "text": "Return compact JSON with event_type, region, severity, assets_affected, directional_bias, tags, rationale. Use only geopolitical, macro, earnings, supply_shock, regulation.",
                                }
                            ],
                        },
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "input_text",
                                    "text": f"Headline: {item.headline}\nBody: {item.body}",
                                }
                            ],
                        },
                    ],
                },
            )
            response.raise_for_status()
            payload = response.json()
            raw = (((payload.get("output") or [{}])[0].get("content") or [{}])[0]).get("text")
            data = json.loads(raw) if raw else {}
            if not data:
                return None
            return ClassifiedEvent(
                event_type=str(data.get("event_type") or "macro"),
                region=str(data.get("region") or "Global"),
                severity=float(data.get("severity") or 0.5),
                assets_affected=[str(item) for item in data.get("assets_affected", [])],
                directional_bias=str(data.get("directional_bias") or "mixed"),
                tags=[str(item) for item in data.get("tags", [])],
                rationale=str(data.get("rationale") or "openai"),
                metadata={"source": "openai"},
            )
        except Exception:
            return None
