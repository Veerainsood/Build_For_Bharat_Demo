# 🇮🇳 Build for Bharat — Intelligent Data Analysis Pipeline

A local, agent-based data intelligence system for discovering, retrieving, and analyzing live datasets from https://data.gov.in.  
It powers BharatGPT — a fully offline Q&A platform that answers analytical questions using official Indian government datasets.

Built By Veerain Sood
Btech IIT Tirupati CSE
CS22B049
---

## 🌐 Overview

### 🔹 What it does
- Connects directly to data.gov.in’s backend API:
      https://www.data.gov.in/backend/dmspublic/v1/resources
- Builds a local DuckDB + JSON index for sectors such as:
  - Crop Development & Seed Production
  - Research, Education & Biotechnology
  - Temperature and Rainfall
  - PM-KISAN Beneficiaries
- Supports semantic retrieval, dataset reasoning, and step-wise execution using a local LLM.

---

## 🧩 Architecture

      Build_For_Bharat/
      ├── dataHandlers/
      │   ├── connectors/
      │   │   └── ogdp_scraper.py          ← Pulls datasets metadata from data.gov.in
      │   ├── indexer/
      │   │   ├── metadata_index.py        ← Builds and merges DuckDB metadata indices
      │   │   └── dataset_selector.py      ← Handles dataset-family classification
      │   ├── llm_tools/
      │   │   ├── dataframeFetcher.py      ← Loads datasets into pandas DataFrames
      │   │   └── ollama_utils.py          ← Manages local model sessions (Ollama)
      │   ├── analyzers/
      │   │   └── runAnaysis.py            ← Executes LLM-generated function sequences
      │   ├── agents/
      │   │   ├── head1_planner.py         ← Generates analysis plan
      │   │   ├── head3_summarizer.py      ← Summarizes analytical results
      │   │   └── selfCritique.py          ← Registry + dataset introspection
      │   └── intelligence/
      │       └── backend.py               ← FastAPI streaming backend
      │
      ├── bharat-ui/                       ← React frontend (Vite + Tailwind)
      │   ├── src/App.jsx                  ← Animated chat interface
      │   └── src/bharat.css               ← Obsidian-glass UI theme
      └── models/
          └── all-MiniLM-L6-v2             ← Local embedding model cache

---

## ⚙️ Setup & Installation

I have used  deadsnakes repo
```bash=
sudo add-apt-repository ppa:deadsnakes/nightly
sudo apt update
sudo apt install python3.10 python3.10-venv python3.10-distutils
```

then do 
```bash=
python3.10 -m venv .bharat
```

then 
```bash=
source .bharat/bin/activate
```

### 1️⃣ Install dependencies

```bash=
pip install -r requirements.txt
```
---

### 2️⃣ Install Ollama

Download from https://ollama.com/download  
Once installed, pull the required models:

      ollama pull mistral-nemo:12b
      ollama pull qwen2.5:7b
      ollama pull qwen2.5:14b
      ollama pull phi3:mini

🧠 These models power the Head-1 Planner, Head-2 Executor, and Head-3 Summarizer modules.

---

## 🧠 SentenceTransformer Auto-Installer

In dataHandlers/llm_tools/embeddings.py (or wherever you initialize embeddings):

      from sentence_transformers import SentenceTransformer
      import os

      try:
          model_path = "./models/all-MiniLM-L6-v2"
          if not os.path.exists(model_path):
              raise FileNotFoundError

          self.sentence_model = SentenceTransformer(
              model_path,
              device="cpu",
              local_files_only=True
          )
      except Exception:
          print("⚠️ Local embedding model not found. Downloading...")
          self.sentence_model = SentenceTransformer("all-MiniLM-L6-v2")
          os.makedirs("./models", exist_ok=True)
          self.sentence_model.save("./models/all-MiniLM-L6-v2")
          print("✅ Model cached locally.")

This ensures your embedding model is automatically downloaded and cached once.

---

## 🚀 Running the System

### 1️⃣ Start the backend
```bash=
in the Build_For_Bharat_Demo-main repository (while in venv do)
uvicorn intelligence.backend:app --reload --port 8000
sudo systemctl stop ollama.service
sudo systemctl disable ollama.service 
```
### 2️⃣ Start Ollama
```bash=
ollama serve
```
### 3️⃣ Start the frontend
```bash=
cd bharat-ui
npm install
npm run dev
```

### Note if your Node.js version is old <20.19 then update it using
```bash=
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
```

Then open http://localhost:5173

---

## 💬 Query Flow

User enters:

      "Compare rainfall with wheat yield in Maharashtra between 2015–2020"

Backend pipeline executes:

      Head-1 Planner: Generate analytical plan
      Head-2 Executor: Run sequential dataset operations
      Head-3 Summarizer: Produce final insight

Frontend displays live streamed updates for each stage in translucent boxes.

---

## 🧱 Local-Only Policy

- All operations run entirely offline once datasets are cached.
- Datasets are fetched from public endpoints only (no scraping or login).
- Models are locally hosted through Ollama.
- No cloud dependency at any stage.

---

## 🪄 Roadmap

- Add charting and visualization in the UI (Plotly / Chart.js)
- Integrate voice input and speech summary
- Build agent registry for modular dataset families
- Add auto-dataset updater for new data.gov.in releases

---

## 🏁 Credits

Developed under the Build for Bharat Fellowship  
Leveraging open Indian datasets to enable transparent, sovereign AI.
