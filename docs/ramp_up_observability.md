# Agent Observability Ramp-Up Guide

This guide gives a diagram-first map of the SDK so you can quickly add new **agent observability** use cases.

## 1) Big picture: what this SDK is

```mermaid
flowchart LR
    A[ADK agent events in BigQuery table] --> B[Client]
    B --> C[Trace reconstruction]
    B --> D[Evaluations]
    B --> E[Insights + Drift]

    D --> D1[CodeEvaluator]
    D --> D2[LLMAsJudge]
    D --> D3[TraceEvaluator + TrialRunner]
    D --> D4[GraderPipeline]

    E --> E1[Question distribution]
    E --> E2[Production-vs-golden drift]
    E --> E3[Session facets + executive summary]

    B --> F[Memory services]
    B --> G[BigQuery AI/ML integration]
```

## 2) Package map (where to read first)

```mermaid
graph TD
    init[__init__.py exports] --> client[client.py]

    client --> trace[trace.py]
    client --> evals[evaluators.py]
    client --> feedback[feedback.py]
    client --> insights[insights.py]

    trace_eval[trace_evaluator.py] --> multi[multi_trial.py]
    evals --> grader[grader_pipeline.py]

    suite[eval_suite.py]
    validator[eval_validator.py]
    views[views.py]
    memory[memory_service.py]
    bqai[ai_ml_integration.py]
    bf[bigframes_evaluator.py]
```

## 3) Request path: `Client.get_trace(session_id)`

```mermaid
sequenceDiagram
    participant U as You
    participant C as Client
    participant BQ as BigQuery
    participant T as Trace

    U->>C: get_trace(session_id)
    C->>BQ: run _GET_TRACE_QUERY
    BQ-->>C: rows (ordered by timestamp)
    C->>C: Span.from_bigquery_row(row)
    C->>T: Trace(session_id, spans)
    T->>T: build parent/child DAG
    C-->>U: Trace object
```

## 4) Evaluation path: code vs LLM judge

```mermaid
flowchart TD
    A[Client.evaluate] --> B{Evaluator type?}

    B -->|CodeEvaluator| C[_evaluate_code]
    C --> C1[SESSION_SUMMARY_QUERY]
    C --> C2[evaluate_session per row]
    C --> C3[_build_report]

    B -->|LLMAsJudge| D[_evaluate_llm_judge]
    D --> D1{Execution mode}
    D1 -->|AI.GENERATE| D2[_ai_generate_judge]
    D1 -->|ML.GENERATE_TEXT| D3[_bqml_judge]
    D1 -->|Gemini API| D4[_api_judge]
    D2 --> D5[_build_report]
    D3 --> D5
    D4 --> D5
```

## 5) Observability extension points for your new use case

```mermaid
mindmap
  root((New observability use case))
    Trace-level
      Add fields to TraceFilter
      Add helper methods on Trace
      Extend SQL projections in client.py
    Evaluation-level
      New CodeEvaluator metric
      New LLMAsJudge criterion
      New aggregation strategy in grader_pipeline.py
    Analysis-level
      New drift mode in feedback.py
      New facet dimension in insights.py
    Productization
      New materialized view via views.py
      Optional BigFrames batch evaluation
      Optional memory-aware signals
```

## 6) Recommended learning order (fastest ramp-up)

```mermaid
flowchart LR
    A[README.md] --> B[src/.../__init__.py exports]
    B --> C[client.py orchestration]
    C --> D[trace.py data model]
    C --> E[evaluators.py]
    E --> F[trace_evaluator.py + multi_trial.py]
    C --> G[insights.py + feedback.py]
    G --> H[eval_suite.py + eval_validator.py]
```

## 7) First implementation checklist for a new observability use case

- Define the smallest user-facing API on `Client` (single clear method).
- Reuse `TraceFilter` and existing SQL template style for query safety.
- Return dataclasses (or report objects) with `summary()` methods.
- Add unit tests in `tests/` mirroring existing naming patterns.
- If the use case is evaluative, support both standalone and pipeline composition.
