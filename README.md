# Crisis-Management-Web-App

## Local demo data

To populate the SQLite database with sample Incident, Crisis, and Miscellaneous records for the current year/week:

```text
python scripts/seed_db.py
python app.py
```

Open http://localhost:8005 and switch between **Incidents**, **Crises**, and **Miscellaneous** for the current week.

Re-running the seed script skips existing demo records. To replace demo data only:

```text
python scripts/seed_db.py --force
```

Demo record IDs use the `*-DEMO-*` prefix (for example `INC-DEMO-001`, `CRZ-DEMO-001`, `MISC-DEMO-001`).
