# Deploy the PIB API on Render

The application supports SQLite for local development and PostgreSQL in
production. It selects PostgreSQL automatically whenever `DATABASE_URL` is set.
The included `render.yaml` creates one web service, a managed PostgreSQL
database, a daily 04:00 IST ingestion job, an hourly PIB reconciliation job,
and a 15-minute recovery job.

## 1. Test locally

```powershell
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python -m pytest -q
.venv\Scripts\python dashboard.py
```

Open these URLs:

- `http://127.0.0.1:5000/api/v1` - endpoint directory
- `http://127.0.0.1:5000/api/v1/pib` - latest available PIB JSON
- `http://127.0.0.1:5000/api/v1/pib?date=2026-08-11` - PIB JSON by date
- `http://127.0.0.1:5000/api/v1/pib/health` - discovery, hydration, and dataset health
- `http://127.0.0.1:5000/api/v1/healthz` - service/database readiness

## 2. Deploy the Blueprint

1. Commit this project to a Git repository and push it to GitHub or GitLab.
2. In Render, choose **New > Blueprint** and select the repository.
3. Render reads `render.yaml` and proposes the web service, PostgreSQL database,
   and cron jobs. Review the plan and create the resources.
4. Enter `PERPLEXITY_API_KEY` when Render requests the unsynchronized secret.
5. Wait for the web service health check to pass.

Render supplies the PostgreSQL connection string through `DATABASE_URL`; do not
copy a database password into source control. The web process binds to Render's
`PORT` using Gunicorn. Cron expressions in `render.yaml` are UTC, so
`30 22 * * *` is 04:00 IST on the following calendar day.

## 3. Copy existing SQLite data (optional)

Use the external PostgreSQL URL shown in the Render database page from your
local PowerShell session:

```powershell
$env:DATABASE_URL='postgresql://USER:PASSWORD@HOST/DATABASE'
.venv\Scripts\python migrate_sqlite_to_postgres.py --source .\news.db
Remove-Item Env:DATABASE_URL
```

The migration initializes the PostgreSQL schema and uses conflict-safe inserts,
so rerunning it does not duplicate rows. Treat the external URL as a secret.

## 4. Verify production

Replace `YOUR-SERVICE` with the assigned Render hostname:

```text
https://YOUR-SERVICE.onrender.com/api/v1/healthz
https://YOUR-SERVICE.onrender.com/api/v1/pib
https://YOUR-SERVICE.onrender.com/api/v1/pib?date=YYYY-MM-DD
https://YOUR-SERVICE.onrender.com/api/v1/pib/completeness?date=YYYY-MM-DD
https://YOUR-SERVICE.onrender.com/api/v1/pib/health
```

Only consume a date as final when its JSON has `complete: true`. That gate
requires verified discovery, healthy listing parsing, a fresh source check,
matching expected/discovered/ready counts, zero missing full-text records, and
no unresolved warning or critical PIB flags.

## Operational notes

- PostgreSQL is the durable store. Render web-service filesystems are not used
  for production news data.
- The hourly reconciliation detects late or backdated PIB releases and updates
  existing dates idempotently.
- Missing full text remains represented by the article row itself and is retried
  by the recovery job; it cannot silently fall out of an in-memory queue.
- `/api/v1/pib/health` deliberately separates source discovery health, content
  hydration health, circuit-breaker state, and whether the dataset is final.
- Uploaded PDFs currently use local storage. Configure durable object storage
  before relying on PDF persistence in a horizontally scaled Render deployment.

