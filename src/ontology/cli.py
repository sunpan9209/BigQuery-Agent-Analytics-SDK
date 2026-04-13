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

"""``gm`` command-line interface (v0).

Implements ``docs/ontology/cli.md``. Only ``gm validate`` is functional in
this revision — ``gm compile`` and ``gm import-owl`` are reserved
subcommands that exit with code 2 until ``binding.md`` and
``owl-import.md`` have implementations under ``src/ontology/``.

Exit codes (per cli.md §3):

  0 — success
  1 — validation / compilation error
  2 — usage error (bad flag, missing file, unimplemented command)
  3 — internal error
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import List, Optional

import typer
import yaml
from pydantic import ValidationError

from .loader import load_ontology_from_string


app = typer.Typer(
    name="gm",
    help="Graph-model CLI: validate, compile, and import ontologies.",
    add_completion=False,
    no_args_is_help=True,
)


@app.callback()
def _root() -> None:
  """Keep Typer in multi-command mode even when only one subcommand exists."""


# --------------------------------------------------------------------- #
# Error reporting                                                        #
# --------------------------------------------------------------------- #


def _emit_errors(
    errors: List[dict],
    *,
    as_json: bool,
) -> None:
  """Write structured errors to stderr in the requested format."""
  if as_json:
    typer.echo(json.dumps(errors, indent=2), err=True)
    return
  for e in errors:
    line = e.get("line") or 0
    col = e.get("col") or 0
    typer.echo(
        f"{e['file']}:{line}:{col}: {e['rule']} \u2014 {e['message']}",
        err=True,
    )


def _collect_errors(
    file: str,
    exc: BaseException,
) -> List[dict]:
  """Convert an exception raised during loading into structured errors."""
  if isinstance(exc, ValidationError):
    out: List[dict] = []
    for err in exc.errors():
      loc = ".".join(str(p) for p in err.get("loc", ())) or "<root>"
      out.append({
          "file": file,
          "line": 0,
          "col": 0,
          "rule": f"ontology-shape:{err.get('type', 'invalid')}",
          "severity": "error",
          "message": f"{loc}: {err.get('msg', '')}",
      })
    return out

  if isinstance(exc, yaml.YAMLError):
    line = 0
    col = 0
    mark = getattr(exc, "problem_mark", None)
    if mark is not None:
      line = mark.line + 1
      col = mark.column + 1
    return [{
        "file": file,
        "line": line,
        "col": col,
        "rule": "yaml-parse",
        "severity": "error",
        "message": str(exc),
    }]

  return [{
      "file": file,
      "line": 0,
      "col": 0,
      "rule": "ontology-validation",
      "severity": "error",
      "message": str(exc),
  }]


# --------------------------------------------------------------------- #
# File-kind detection                                                    #
# --------------------------------------------------------------------- #


def _detect_kind(text: str) -> str:
  """Return ``'ontology'``, ``'binding'``, or ``'unknown'``.

  Raises ``yaml.YAMLError`` on parse failure so the caller can route it
  through the ``yaml-parse`` error path.
  """
  data = yaml.safe_load(text)
  if not isinstance(data, dict):
    return "unknown"
  if "ontology" in data and "binding" not in data:
    return "ontology"
  if "binding" in data:
    return "binding"
  return "unknown"


# --------------------------------------------------------------------- #
# gm validate                                                            #
# --------------------------------------------------------------------- #


@app.command("validate")
def validate(
    file: Path = typer.Argument(
        ...,
        exists=True,
        readable=True,
        dir_okay=False,
        help="Path to an ontology or binding YAML file.",
    ),
    json_output: bool = typer.Option(
        False,
        "--json",
        help="Emit structured JSON errors on stderr.",
    ),
) -> None:
  """Validate a single ontology or binding YAML file."""
  text = file.read_text(encoding="utf-8")
  try:
    kind = _detect_kind(text)
  except yaml.YAMLError as exc:
    _emit_errors(_collect_errors(str(file), exc), as_json=json_output)
    raise typer.Exit(code=1)

  if kind == "binding":
    _emit_errors(
        [{
            "file": str(file),
            "line": 0,
            "col": 0,
            "rule": "cli-unimplemented",
            "severity": "error",
            "message": (
                "Binding validation is not yet implemented in src/ontology."
            ),
        }],
        as_json=json_output,
    )
    raise typer.Exit(code=2)

  if kind != "ontology":
    _emit_errors(
        [{
            "file": str(file),
            "line": 0,
            "col": 0,
            "rule": "cli-unknown-kind",
            "severity": "error",
            "message": (
                "File is neither an ontology (top-level 'ontology:') nor a "
                "binding (top-level 'binding:')."
            ),
        }],
        as_json=json_output,
    )
    raise typer.Exit(code=2)

  try:
    load_ontology_from_string(text)
  except (ValueError, ValidationError, yaml.YAMLError) as exc:
    _emit_errors(_collect_errors(str(file), exc), as_json=json_output)
    raise typer.Exit(code=1)
  except Exception as exc:  # pragma: no cover - defensive
    typer.echo(f"internal error: {exc}", err=True)
    raise typer.Exit(code=3)
  # Success: nothing on stdout (cli.md §4).


def main() -> None:
  """Entry point for the ``gm`` console script."""
  app()


if __name__ == "__main__":
  sys.exit(app() or 0)
