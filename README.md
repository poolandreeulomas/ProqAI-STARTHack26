# ProQAi

![Demo](assets/SH26_Demo.gif)

Conversational procurement that turns natural-language requests into policy-compliant supplier recommendations and auditable order decisions.

Deployed version: https://proqai.shop/

Developed by:
  - Arjun Singh (MsC Finance, University of Zurich)
  - Daniel Kaminski (EYP, Dublin)
  - Tonio Hasler (Finance & Physics, Frankfurt and Goethe)
  - Pol Andreu (Telecom & AI, UPC and TU Wien)


## Problem

32 million hours are lost every year in procurement friction. That is 46 human lifetimes, or roughly 3,650 years spent on forms, approvals, and system navigation instead of productive work.

Traditional procurement tools optimize for compliance workflows, not user experience:

- Employees must navigate complex category trees and policy rules.
- Request quality is inconsistent because inputs are often incomplete.
- Approval cycles take days or weeks.
- Teams bypass systems to get work done (credit cards, direct supplier calls).

These workarounds create real business risk:

- Up to 16% of negotiated savings can be lost.
- Compliance gaps increase audit exposure.
- Finance teams handle mismatched invoices and manual reconciliations.

The core question behind ProQAi is simple: what if procurement felt like a conversation instead of a form?

## Overview

ProQAi is a current working version of a conversational procurement engine. A user writes what they need, and the system converts that into a structured request, validates it against policy, ranks suppliers, and returns a transparent recommendation with an audit trail.

### End-to-end flow

1. User submits a message (example: "10 laptops for a new office in Zurich, budget CHF 15,000").
2. Workflow parser extracts key fields (category, quantity, country, budget, currency, and constraints).
3. If critical information is missing, ProQAi asks follow-up questions instead of guessing.
4. Supplier engine filters eligible suppliers using policy and capability constraints.
5. Suppliers are ranked with a deterministic weighted scoring model.
6. Escalations are triggered when rules require additional approval.
7. Results are returned with clear rationale and audit-ready output.

### Demo paths represented in this version

- Smooth case:
  - Clear input request
  - Instant policy check and ranked shortlist
  - Transparent confidence and recommendation output
  - Exportable audit evidence

- Hard case:
  - Vague input request
  - Clarification loop for missing required fields
  - Budget-threshold escalation to supervisor approval
  - Supervisor-facing decision context and status tracking

### Why this architecture matters

- Deterministic core for low-latency, predictable behavior
- Modular policy model for quick enterprise adaptation
- Full traceability for audit and governance
- Learning-oriented scoring calibration through historical award analysis

## Installation

This repository contains a Python backend and references two git submodules (`data` and `frontend`). Initialize submodules first so required datasets and UI code are available.

### Prerequisites

- Python 3.12+
- Node.js 18+ (for frontend, if present)
- Git (with submodule support)

### 1) Clone and initialize submodules

```bash
git clone <your-repo-url>
cd proq-ai
git submodule update --init --recursive
```

### 2) Backend setup

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Optional environment config for LLM-assisted parsing:

```bash
copy .env.example .env
```

Then set values in `.env`:

- `MOONSHOT_API_KEY`
- `MOONSHOT_BASE_URL` (default in example file)
- `MOONSHOT_MODEL` (default in example file)

### 3) Run backend API

```bash
python -m uvicorn app:app --reload --port 8000 --log-level debug
```

API endpoints:

- `POST /api/workflow` - conversational request parsing + clarifications + recommendation output
- `POST /api/match` - direct supplier engine matching on structured payload

### 4) (Optional) Run frontend

If `frontend/` is populated via submodule:

```bash
cd frontend
npm install
npm run dev
```

### 5) Utility scripts

```bash
python scripts/fit_scoring_weights.py
python scripts/validate_engine.py
python scripts/escalation_stats.py
```

### 6) Vercel deployment

The project is wired for Vercel with:

- Serverless FastAPI entrypoint in `api/index.py`
- Frontend build from `frontend/`
- API rewrite to `/api/index.py`

Deploy with:

```bash
vercel
```

## Stack

### Core backend

- FastAPI
- Pydantic
- Python 3.12+

### Procurement engine

- Deterministic supplier ranking and policy evaluation (`supplier_engine.py`)
- Conversational request workflow with clarification loop (`request_workflow.py`)
- Rule-based escalations and recommendation status outputs

### Data and governance

- CSV/JSON datasets for suppliers, pricing, categories, historical awards, and policies
- Audit-friendly outputs (`outputs.json`, `validate_report.json`, `escalation_report.json`)

### Learning and analytics

- Scikit-learn logistic regression for weight fitting (`scripts/fit_scoring_weights.py`)
- Validation and escalation statistics scripts for model quality tracking

### Deployment

- Vercel serverless backend (`api/index.py`)
- Vercel static frontend build pipeline (`vercel.json`)

## Impact

ProQAi is designed to shift procurement from a bottleneck function to a strategic decision layer.

### Business value

- Reduced process friction by replacing form-heavy flows with conversational input
- Higher policy compliance through guided capture and deterministic validation
- Faster procurement cycles via automated ranking and escalation handling
- Stronger audit readiness with one-click traceability of decisions and checks

### Market relevance

- Procurement software market is estimated around $9B today and projected above $20B by 2034.
- 73% of businesses are already adopting AI in procurement workflows.

The opportunity is not whether AI procurement happens, but which solution becomes trusted first for compliant, explainable decisions at scale.

### Closing vision

Procurement should not slow companies down. It should move them forward.

ProQAi turns procurement into an intelligent conversation that preserves compliance, improves decision quality, and unlocks real operational speed.
