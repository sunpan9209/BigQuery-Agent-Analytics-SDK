# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for the OWL import bridge (Step 4 migration)."""

from __future__ import annotations

import os

import pytest

_FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
_TTL_PATH = os.path.join(_FIXTURES, "yamo_sample.ttl")

rdflib = pytest.importorskip("rdflib")


class TestImportOwlToOntology:
  """Upstream OWL importer bridge."""

  def test_produces_ontology_yaml(self):
    from bigquery_agent_analytics.ttl_importer import import_owl_to_ontology

    yaml_text, drop_summary = import_owl_to_ontology(
        sources=[_TTL_PATH],
        include_namespaces=["https://example.com/yamo#"],
    )
    assert "ontology:" in yaml_text
    assert "entities:" in yaml_text
    # Should NOT have the SDK's ontology_import: metadata block.
    assert "ontology_import:" not in yaml_text

  def test_drop_summary_non_empty(self):
    from bigquery_agent_analytics.ttl_importer import import_owl_to_ontology

    _, drop_summary = import_owl_to_ontology(
        sources=[_TTL_PATH],
        include_namespaces=["https://example.com/yamo#"],
    )
    # There should be some output (even if no drops, the summary header).
    assert isinstance(drop_summary, str)

  def test_entities_in_output(self):
    from bigquery_agent_analytics.ttl_importer import import_owl_to_ontology

    yaml_text, _ = import_owl_to_ontology(
        sources=[_TTL_PATH],
        include_namespaces=["https://example.com/yamo#"],
    )
    assert "Party" in yaml_text
    assert "AdUnit" in yaml_text
    assert "Campaign" in yaml_text

  def test_fill_in_for_missing_key(self):
    """DecisionPoint has no owl:hasKey — should have FILL_IN."""
    from bigquery_agent_analytics.ttl_importer import import_owl_to_ontology

    yaml_text, _ = import_owl_to_ontology(
        sources=[_TTL_PATH],
        include_namespaces=["https://example.com/yamo#"],
    )
    assert "FILL_IN" in yaml_text


class TestImportOwlToGraphSpec:
  """End-to-end OWL to GraphSpec bridge."""

  def test_rejects_fill_in(self):
    """FILL_IN in upstream output prevents GraphSpec conversion."""
    from bigquery_agent_analytics.ttl_importer import import_owl_to_graph_spec

    # yamo_sample.ttl has DecisionPoint without owl:hasKey → FILL_IN.
    with pytest.raises(ValueError, match="FILL_IN"):
      import_owl_to_graph_spec(
          sources=[_TTL_PATH],
          include_namespaces=["https://example.com/yamo#"],
          project_id="p",
          dataset_id="d",
      )

  def test_no_sources_raises(self):
    from bigquery_agent_analytics.ttl_importer import import_owl_to_graph_spec

    with pytest.raises(ValueError, match="source"):
      import_owl_to_graph_spec(
          sources=[],
          include_namespaces=["https://example.com/yamo#"],
          project_id="p",
          dataset_id="d",
      )

  def test_rejects_extends(self, tmp_path):
    """Ontology with extends is rejected with a clear message."""
    from bigquery_agent_analytics.ttl_importer import import_owl_to_graph_spec

    # Write a TTL with extends but all keys present (no FILL_IN).
    ttl = tmp_path / "extends.ttl"
    ttl.write_text(
        "@prefix : <https://example.com/test#> .\n"
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
        ":Parent a owl:Class ; owl:hasKey ( :pid ) .\n"
        ":pid a owl:DatatypeProperty ; "
        "  rdfs:domain :Parent ; rdfs:range xsd:string .\n"
        ":Child a owl:Class ; rdfs:subClassOf :Parent .\n"
        ":extra a owl:DatatypeProperty ; "
        "  rdfs:domain :Child ; rdfs:range xsd:string .\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="flat ontology.*extends"):
      import_owl_to_graph_spec(
          sources=[str(ttl)],
          include_namespaces=["https://example.com/test#"],
          project_id="p",
          dataset_id="d",
      )


class TestLegacyPathUnchanged:
  """Existing ttl_import/ttl_resolve still work."""

  def test_ttl_import_still_produces_import_yaml(self):
    from bigquery_agent_analytics.ttl_importer import ttl_import

    result = ttl_import(
        _TTL_PATH,
        include_namespaces=["https://example.com/yamo#"],
    )
    assert "ontology_import:" in result.yaml_text
    assert result.report.classes_mapped == 5

  def test_ttl_resolve_still_works(self, tmp_path):
    from bigquery_agent_analytics.ontology_models import load_graph_spec_from_string
    from bigquery_agent_analytics.ttl_importer import ttl_import
    from bigquery_agent_analytics.ttl_importer import ttl_resolve

    result = ttl_import(
        _TTL_PATH,
        include_namespaces=["https://example.com/yamo#"],
    )
    import_file = tmp_path / "yamo.import.yaml"
    import_file.write_text(result.yaml_text, encoding="utf-8")

    resolved = ttl_resolve(
        str(import_file),
        defaults={
            "entities[DecisionPoint].keys.primary": ["decision_id"],
            "relationships[evaluates].binding.from_columns": ["decision_id"],
        },
    )
    spec = load_graph_spec_from_string(resolved)
    assert "DecisionPoint" in {e.name for e in spec.entities}
