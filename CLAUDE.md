# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Flask backend microservice for Grupo MPL that produces APIs for Industrial Production Goals ("Metas de Produção Industrial"). It connects to two kinds of data sources:

- A Postgres database (`src/connection/ConexaoPostgre.py`, via SQLAlchemy/psycopg2) — the app's own DB (schema `pcp`), plus a second Postgres server referred to as "WMS" (`conexaoEngineWMSSrv`).
- The company ERP, Consistem/CSW, running on InterSystems Caché, reached via JDBC (`src/connection/ConexaoERP.py`, using `jaydebeapi` + the bundled `src/connection/CacheDB.jar` driver, class `com.intersys.jdbc.CacheDriver`).

Domain vocabulary (from README.md), needed to read the model code:
- **Fase**: an industrial sector/cell that shares similar processes.
- **Plano**: the name/timeline of a "coleção" (collection) being planned; registered in a separate companion microservice.
- **Carga da Fase**: qty of pieces currently in a phase.
- **Fila da Fase**: qty of pieces about to arrive at the next phase.
- **Cronograma**: the phase-by-phase production schedule in business days.
- **Meta Total/Diária**: total/daily production quantity target per phase.

## Running the app

There is no build/lint/test tooling configured (no test suite, linter, or CI in this repo).

1. Create `_ambiente.env` in the **parent directory** of the project (i.e. one level above the repo root) with: `POSTGRES_PASSWORD_SRV1`, `POSTGRES_PASSWORD_SRV2`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_HOST_SRV1`, `POSTGRES_HOST_SRV2`, `POSTGRES_PORT`, `CSW_USER`, `CSW_PASSWORD`, `CSW_HOST`, `CAMINHO_PARQUET_FAT`, `PORTA_APLICACAO`.
2. Set `localProjeto` in `src/configApp/configApp.py` to the absolute path where the project is deployed — this is the base path used to locate `_ambiente.env` and the `dados/` folder at runtime.
3. Install deps: `pip install -r requirements.txt` (note: this file is currently saved as UTF-16, which breaks `pip install -r` on some setups — reading/regenerating it as UTF-8 fixes that).
4. Run: `python app_run.py` (reads `PORT` env var or falls back to `PORTA_APLICACAO`).

`ConexaoERP.py` executes a live test query against the CSW/Caché connection at **import time** (module-level code), so simply importing anything that pulls in the routes package will attempt a real ERP connection.

## Architecture

Three layers, all under `src/`:

- **`src/routes/`** — Flask Blueprints, one per feature area (e.g. `MetaFasesController.py`, `FaturamentoController.py`, `ProducaoFasesController.py`, `GastosCentroCusto.py`, `AcompanhamentoLeadTime.py`, `CronogramaFasesController.py`, `ContolePilotos.py`, `ContolePartes.py`). All are registered onto a single parent `routes_blueprint` in `src/routes/__init__.py`, which is in turn registered on the Flask `app` in `app_run.py`. Every route module defines its own local copy of a `token_required` decorator that checks `Authorization` header against a hardcoded token string (`a44pcp22`) — this is duplicated per file rather than shared, so if it's ever changed it must be changed in every route file consistently.
- **`src/models/`** — plain Python classes (not an ORM) that hold the actual business logic and SQL. Controllers instantiate a model class with query parameters via `__init__`, then call a method that returns a `pandas.DataFrame`. Route handlers then convert that DataFrame to `list[dict]` via manual `iterrows()` (the same boilerplate loop is repeated in nearly every route handler) and `jsonify` it. Key models: `MetaFases` (the core goals/metas engine, ~1000 lines), `OrdemProd`, `ProducaoFases`, `Produtos`, `PlanoClass`, `Cronograma`, `FaturamentoClass`, and the `*_CSW.py` models (`OP_CSW`, `Produto_CSW`, `Pedidos_CSW`, `GastosCentroCusto_CSW`, `Faccionista_CSW`, `Tags_csw`) which query the Caché ERP instead of Postgres.
- **`src/connection/`** — the two DB access modules described above, plus the JDBC driver jar.
- **`src/configApp/configApp.py`** — single global `localProjeto` path constant used across the codebase to locate `_ambiente.env` and the `dados/` directory.

### CSV "freeze" pattern (`dados/`)

Several expensive Postgres/CSW queries are deliberately cached to disk as CSV under `dados/` (and archived under `dados/backup/`) instead of being re-queried, for performance reasons (see README "Path 1.3" notes). Key files: `filaroteiroOP.csv` (phase queue/load snapshot), `analiseFaltaProgrFases_*.csv`, `analise_Plano_*Lote*.csv`, `Totais*.csv`, `meta_{plano}_{lote}_{data}.csv`. `MetaFases` accepts an `analiseCongelada` flag to switch between live SQL and reading these frozen CSVs. When touching `MetaFases.py` or `OrdemProd.py`, check whether a change needs to be reflected in both the live-query path and the CSV-backed path.

### CSW/Caché queries use raw string concatenation

Unlike the Postgres modules (which use parameterized queries via `pd.read_sql(..., params=...)`), the `*_CSW.py` models build SQL by concatenating Python strings/f-strings directly (e.g. `OP_CSW.ordemProd_csw_aberto`). Be careful with any user-supplied value flowing into these methods — follow the existing parameterization style used in the Postgres-facing code rather than propagating string concatenation when adding new CSW queries.

### Hardcoded/mutable business rules

`MetaFases.__init__` contains hardcoded conditionals keyed off specific `arrayCodLoteCsw` batch codes (e.g. `'25A04B'`, `'25L07A'`) that toggle `arrayTipoProducao` and `consideraFaltaProgr` for specific production seasons/collections. These are seasonal, one-off overrides tied to real production lots — don't assume they generalize, and expect new ones to be added/removed as collections change.
