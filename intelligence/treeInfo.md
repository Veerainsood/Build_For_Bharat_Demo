```python
├── intelligence/               ← all reasoning, orchestration, agents
│   ├── __init__.py
│   ├── query_parser.py         ← extract entities, time, region, intent
│   ├── relevance_ranker.py     ← semantic + heuristic ranking of datasets
│   ├── dataset_bundler.py      ← merge related datasets (e.g. monthly → yearly)
│   ├── analysis_orchestrator.py← coordinates fetcher + analyzer + summarizer
│   ├── agents/                 ← (optional) if you later use LangChain/LangGraph
│   │    ├── query_agent.py
│   │    ├── retrieval_agent.py
│   │    ├── analysis_agent.py
│   │    └── synthesis_agent.py
│   └── llm_tools/              ← wrappers exposing modules as LangChain Tools
│        ├── dataset_search_tool.py
│        ├── data_analysis_tool.py
│        └── summarization_tool.py
```
---
| **Agent**             | **Function**                                                                 |
|------------------------|------------------------------------------------------------------------------|
| **Query Decomposer**   | Break the natural-language question into structured subtasks (entities, time span, metrics, comparisons). |
| **Dataset Retriever**  | Use `relevance_ranker` + `dataset_selector` to fetch matching dataset URLs.  |
| **Data Fetcher**       | Download, clean, and normalize CSVs.                                        |
| **Data Analyst**       | Run numeric operations (averages, correlation, ranking) using pandas.       |
| **Synthesis Agent**    | Take results, explain in plain English, and cite all data sources.          |
