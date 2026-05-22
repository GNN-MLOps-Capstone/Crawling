from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))


class _GenerateContentConfig:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def _install_dependency_stubs(monkeypatch) -> None:
    google_module = types.ModuleType("google")
    genai_module = types.ModuleType("google.genai")
    genai_types_module = types.ModuleType("google.genai.types")
    genai_types_module.GenerateContentConfig = _GenerateContentConfig
    genai_module.types = genai_types_module
    genai_module.Client = lambda **_: object()
    google_module.genai = genai_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.genai", genai_module)
    monkeypatch.setitem(sys.modules, "google.genai.types", genai_types_module)
    boto3_module = types.ModuleType("boto3")
    boto3_module.client = lambda *_, **__: object()
    monkeypatch.setitem(sys.modules, "boto3", boto3_module)
    monkeypatch.setitem(sys.modules, "pandas", types.ModuleType("pandas"))
    monkeypatch.setitem(sys.modules, "psycopg2", types.ModuleType("psycopg2"))


def _gemini_service(monkeypatch):
    _install_dependency_stubs(monkeypatch)
    sys.modules.pop("modules.analysis.gemini_service", None)
    return importlib.import_module("modules.analysis.gemini_service")


def test_extract_valid_analysis_accepts_nested_analysis_shape(monkeypatch) -> None:
    gemini_service = _gemini_service(monkeypatch)
    analyzer = gemini_service.GeminiNewsAnalyzer.__new__(gemini_service.GeminiNewsAnalyzer)

    summary, sentiment = analyzer._extract_valid_analysis(
        {
            "related_stocks": ["삼성전자"],
            "keywords": ["반도체", "실적", "투자"],
            "analysis": {
                "summary": "삼성전자가 반도체 투자 계획을 밝혔다.",
                "sentiment": "긍정",
            },
        }
    )

    assert summary == "삼성전자가 반도체 투자 계획을 밝혔다."
    assert sentiment == "긍정"


def test_gemini_config_declares_required_response_schema(monkeypatch, tmp_path) -> None:
    gemini_service = _gemini_service(monkeypatch)
    config_path = tmp_path / "analysis_config.yaml"
    config_path.write_text(
        """
aws:
  bucket_name: silver
gemini:
  model_name: gemini-test
  temperature: 0.1
  system_prompt: prompt
""",
        encoding="utf-8",
    )

    analyzer = gemini_service.GeminiNewsAnalyzer(
        aws_info={
            "access_key": "access",
            "secret_key": "secret",
            "endpoint_url": "http://minio",
            "gemini_api_key": "key",
        },
        config_path=str(config_path),
        stock_map={},
    )

    schema = analyzer.gen_config.kwargs["response_json_schema"]
    assert set(schema["required"]) == {"related_stocks", "keywords", "summary", "sentiment"}
    assert schema["properties"]["related_stocks"]["items"] == {"type": "string"}
    assert schema["properties"]["sentiment"]["enum"] == ["긍정", "중립", "부정"]
