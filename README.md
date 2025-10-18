# 🇮🇳 Build for Bharat — Data Ingestion Layer

This repository implements a **local, agent-based data intelligence pipeline** for accessing live datasets from [data.gov.in](https://data.gov.in).  
It powers the *BharatGPT* prototype — a self-contained Q&A system that connects user prompts to official Indian government datasets.

## 🌐 Overview

### 🔹 What it does
- Fetches **live metadata** from data.gov.in using the hidden JSON API  
  `https://www.data.gov.in/backend/dmspublic/v1/resources`
- Builds a **local DuckDB index** of all datasets under:
  - **Ministry of Agriculture and Farmers Welfare**
  - **India Meteorological Department (IMD)**  
- Enables **offline dataset search and retrieval** using titles and notes.

### 🔹 Why it matters
This forms the foundation for a fully local, transparent AI system that can:
> *“Answer questions about Indian datasets — without internet, without APIs, and without scraping.”*

## 🧩 Architecture
```
bharatgpt/
├── connectors/
│   └── ogdp_scraper.py          ← Fetches metadata from data.gov.in
├── indexer/
│   ├── metadata_index.py        ← DuckDB-based metadata store
│   └── dataset_selector.py      ← Keyword/semantic dataset search
├── demo/
│   ├── demo_scraper.py          ← Builds the initial local index
│   └── demo_query.py            ← Tests dataset retrieval
├── data/
│   └── ogdp_index.db            ← Local DuckDB database (auto-created)
└── utils/
    └── helpers.py               ← Utility functions (timestamp, I/O)
```

## ⚙️ Usage

### 1️⃣ Setup
```bash
pip install duckdb requests pandas
```
### 2️⃣ Run the scraper
```bash
python -m dataHandlers.demo.demo_scraper
```
This populates `data/ogdp_index.db` with live datasets from both ministries.

### 3️⃣ Query datasets
```bash
python -m dataHandlers.connectors.temp
```
or
```python
from dataHandlers.indexer.dataset_selector import DatasetSelector

selector = DatasetSelector()
results = selector.search("rainfall data 2025", limit=5)
for r in results:
    print(r["title"], "->", r["id"])
```
## 🧠 Next Steps
- [ ] Add **semantic embeddings** for intelligent dataset selection  
- [ ] Add **DataFetcher** (load CSV/JSON into pandas)  
- [ ] Add **Analyzer Agent** (aggregate, summarize, visualize)  
- [ ] Integrate with a local LLM (Mistral / Phi / Ollama)  
- [ ] Build CLI / web chat interface

## 📦 Local Data Policy
All dataset metadata and content are fetched from public data.gov.in resources and stored locally for research and development purposes.  
This system performs no unauthorized scraping or login-based access.
"""
