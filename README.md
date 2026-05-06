# ChessForge Analytics

An end-to-end data engineering and ML portfolio project built on real-world [Lichess](https://lichess.org/) game data.

The project demonstrates a full stack from raw data ingestion to structured querying, with a natural language LLM interface and ML pipeline planned as next phases.



## Features

- Download monthly Lichess game dumps (millions of games per file)
- Stream and parse compressed `.pgn.zst` files without loading them into memory
- Store structured game data (Elo, opening ECO, time control, result, …) in PostgreSQL
- Manage datasets: ingest, list, and delete monthly snapshots independently
- Query the database via predefined SQL queries through the CLI
- Platform-independent thanks to Docker containerization



## Requirements

- [Docker](https://www.docker.com/) (including Docker Compose)
- Optional: Python, only needed for the shorter CLI wrapper commands



## Getting Started

Clone the repository:

```bash
git clone https://github.com/niabicht/chessforge-analytics.git
cd chessforge-analytics
```

Docker images are built automatically on first use. To build manually:

```bash
docker compose build
```

A small example file (1000 games) is included in the repo. You can ingest and query it immediately without downloading any large files.

If you want real but manageable data, older Lichess dumps are much smaller (starting from 2013-01). Note that dumps before 2021-04 and especially before 2016-12 may have minor data quirks such as missing evaluations.



## Usage

The app is available via two equivalent interfaces. Choose whichever fits your setup.

> [!NOTE]
> These examples show the most common commands. Every command supports `--help` for full flag documentation — e.g. `python chessforge.py ingest --help`. The `--help` output is always the authoritative reference, this README may lag behind.

<details>
<summary><strong>Docker commands</strong> (no Python required)</summary>

```bash
# Download a specific month or the latest available Lichess dataset
docker compose run --rm app python -m chessforge.cli download --month 2026-04
docker compose run --rm app python -m chessforge.cli download --latest

# List downloaded input files
docker compose run --rm app python -m chessforge.cli list-files

# Ingest the example dataset or a specific month into the database
docker compose run --rm app python -m chessforge.cli ingest --example
docker compose run --rm app python -m chessforge.cli ingest --month 2026-04

# List all ingested datasets
docker compose run --rm app python -m chessforge.cli list-datasets

# Run a predefined SQL query
docker compose run --rm app python -m chessforge.cli query --name opening-winrates

# Delete one or all datasets
docker compose run --rm app python -m chessforge.cli delete-dataset --month 2026-04
docker compose run --rm app python -m chessforge.cli delete-dataset --all

# Delete one or all downloaded input files
docker compose run --rm app python -m chessforge.cli delete-file --month 2026-04
docker compose run --rm app python -m chessforge.cli delete-file --all
```

</details>

<details>
<summary><strong>Python wrapper commands</strong> (recommended for convenience)</summary>

Requires Python installed locally. The wrapper shortens the lengthy Docker commands.

```bash
# Download a specific month or the latest available Lichess dataset
python chessforge.py download --month 2026-04
python chessforge.py download --latest

# List downloaded input files
python chessforge.py list-files

# Ingest the example dataset or a specific month into the database
python chessforge.py ingest --example
python chessforge.py ingest --month 2026-04

# List all ingested datasets
python chessforge.py list-datasets

# Run a predefined SQL query
python chessforge.py query --name opening-winrates

# Delete one or all datasets
python chessforge.py delete-dataset --month 2026-04
python chessforge.py delete-dataset --all

# Delete one or all downloaded input files
python chessforge.py delete-file --month 2026-04
python chessforge.py delete-file --all
```

</details>

### Available predefined queries

| Query name | Description |
|---|---|
| `common-openings` | Most frequently played openings |
| `opening-winrates` | Win/draw/loss per opening |
| `ratings` | Rating average, minimum and maximum across ingested games |
| `elo-diff-winrates` | Win rates bucketed by Elo difference |



## Tech Stack

| Area | Tools |
|---|---|
| Infrastructure | Docker, Docker Compose |
| Data ingestion | Python, `python-chess`, `zstandard` |
| Database | PostgreSQL, `psycopg2` |
| CLI | Typer, tqdm |
| Testing & CI | `pytest`, GitHub Actions |
| Planned: ML | `pandas`, PyTorch, MLflow |
| Planned: LLM/RAG | LLaMA 3 (Ollama), LangChain |
| Planned: UI | Streamlit |



## Planned Features

### Phase 2: ML pipeline
A PyTorch neural network predicting game outcome (win/draw/loss) from structured features: Elo difference, opening ECO, and time control. Full experiment tracking via MLflow.
Optional extension: a CNN predicting outcome from the board state after the opening phase, encoding positions as spatial tensors.

### Phase 3: Natural language querying
A local LLM (LLaMA 3 via Ollama) translates natural language questions into SQL queries.

> *"Show me the most successful openings for 1800–2000 Elo players in blitz."*

The pipeline injects the schema and example rows as context (retrieval-augmented generation), validates the generated SQL before execution, and surfaces both the query and result in a Streamlit UI.


## Developer notes

<details>
<summary><strong>Expand</strong></summary>

## Architecture

```
CLI (Typer)           # user I/O only
  └── Services        # orchestration, validation (and some simple operations)
        └── Ingestion # downloading, streaming, parsing
        └── Database  # connection management, all SQL
        └── ...
```

The CLI is the only layer that imports Typer or interacts with the user directly. Services are UI-agnostic and accept callbacks (`log`, `on_progress`, `on_done`), so they can be reused by a future Streamlit frontend without modification (hopefully). For service functions: Every service function returns `is_success: bool`. Any result data is surfaced via callbacks.

## Intentional Design Trade-offs

This project prioritises end-to-end functionality over production-grade robustness. Known simplifications:

- Input validation: The pipeline assumes well-formed Lichess PGN input and somewhat reasonable CLI usage. Handling every possible edge case is not the focus of this project. A production pipeline would use a dedicated validation layer (e.g. Great Expectations).
- Error handling: Errors generally propagate as unhandled exceptions rather than being caught, classified, and reported cleanly. There is no structured logging, no verbosity levels, and no distinction between recoverable and fatal errors. Log messages are passed as plain-string callbacks from the backend to the CLI. Functional for the scope of this project I guess.
- Connection management: One connection is opened and closed per service call via a `InitializedConnection` class. No connection pooling, no transaction retry logic. Sufficient for a single-user CLI.
- Python path: `ENV PYTHONPATH=/app/src` in the Dockerfile avoids the overhead of proper packaging via `pip install`. Would not fly for a multi-package monorepo but works fine here.

## How-Tos

### Running tests

```bash
docker compose run --rm app pytest
```

### Adding a predefined query

Add a `.sql` file to `src/chessforge/queries/`. The `query_service.py` discovers queries by scanning that folder at runtime (Docker rebuild required).

## TODOs

- Move database password to environment variable
- Add screenshots / GIFs to README
- Performance improvements: 
    - PostgreSQL `COPY` instead of `executemany` for bulk inserts
    - parallelize parsing
    - custom parser, preferably in C or Rust
    - header-only parsing if moves are unused (but will probably use moves in future)
- ~6% of games include Stockfish evaluations (`[%eval 2.35]`, `[%eval #-4]`)... consider parsing these as an additional feature for the ML pipeline

</details>



## Data Source

Game data sourced from the [Lichess open database](https://database.lichess.org/), monthly dumps of all rated standard games, released under [CC0](https://www.tldrlegal.com/license/creative-commons-cc0-1-0-universal).