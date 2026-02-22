# Medzone Agency Manager

A lightweight management app for the Medzone college agency. Track students, courses, staff, and fees in one place.

## Features
- Authentication with roles (admin, staff)
- Student pipeline stages and student tracking
- Dedicated fees tab with payments ledger and balances
- Document uploads per student
- Notifications queue (email/SMS)
- Reports and CSV exports
- Calendar export (.ics)
- Live updates: fees changes refresh other browsers

## Run locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open the app in your browser using the local address shown in the terminal.

## Run with WSGI (Gunicorn)
```bash
source .venv/bin/activate
pip install -r requirements.txt
gunicorn wsgi:app
```

## Deploy on Render (recommended)
1. Create a new Web Service and connect this repo.
2. Render will auto-detect `render.yaml`.
3. Deploy and open the generated URL.

Environment variables used:
- `SECRET_KEY`
- `DATABASE_PATH` (SQLite disk path, paid Render only)
- `SUPABASE_DB_URL` (Supabase Postgres connection string)
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`

After deploying:
1. Open the Render URL.
2. Log in with the default admin and change the password.
3. Create additional users as needed.

If you are on Render Free, use `SUPABASE_DB_URL` instead of `DATABASE_PATH` so data persists.

## Default login
- Username: `admin`
- Password: `medzone123`

Change the password after first login by updating the user record in the database.
