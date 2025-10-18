bharatgpt/
│
├── connectors/
│   ├── __init__.py
│   └── ogdp_scraper.py            ← Scraper Agent (data.gov.in API wrapper)
│
├── indexer/
│   ├── __init__.py
│   ├── metadata_index.py          ← DuckDB-based metadata store
│   └── dataset_selector.py        ← Query Agent (searches metadata by text/embedding)
│
├── data/
│   ├── ogdp_index.db              ← Local DuckDB database (auto-created)
│   └── ogdp_backup.json           ← Optional JSON snapshot for inspection
│
├── demo/
│   ├── demo_scraper.py            ← Runs scraper, populates index
│   ├── demo_query.py              ← Example query pipeline
│   └── demo_schedule.sh           ← Example cron/scheduler entry
│
├── config/
│   ├── settings.yaml              ← (future) sectors, ministries, sync frequency, etc.
│   └── logging.conf               ← logging format, rotation
│
├── logs/
│   └── scraper.log                ← Rotating logs for sync jobs
│
├── utils/
│   ├── __init__.py
│   └── helpers.py                 ← Common helpers (timestamping, string cleanup, etc.)
│
└── main.py                        ← (optional) unified entrypoint for local prototype
