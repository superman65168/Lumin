from __future__ import annotations

import csv
import io
import json
import os
import queue
import sqlite3
from datetime import datetime
from functools import wraps
from typing import Any

try:
    import psycopg
    from psycopg.rows import dict_row
    PSYCOPG_DRIVER = "psycopg"
except ImportError:
    psycopg = None
    dict_row = None
    PSYCOPG_DRIVER = None

if PSYCOPG_DRIVER is None:
    try:
        import psycopg2
        from psycopg2.extras import DictCursor
        PSYCOPG_DRIVER = "psycopg2"
    except ImportError:  # pragma: no cover - optional for SQLite-only usage
        psycopg2 = None
        DictCursor = None

from flask import (
    Flask,
    Response,
    abort,
    g,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DATABASE_PATH", os.path.join(BASE_DIR, "data.db"))
DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
USE_POSTGRES = bool(DB_URL)
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
ALLOWED_EXTENSIONS = {"pdf", "png", "jpg", "jpeg", "doc", "docx"}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev")
event_subscribers: list[queue.Queue[str]] = []
STUDENT_STAGES = [
    "Lead",
    "Consultation",
    "Applied",
    "Offer Received",
    "Visa",
    "Enrolled",
]
FEES_BY_COUNTRY = {
    "Georgia": {
        "East West University": 3900,
        "Central University of Europe (Kutasi)": 4500,
        "Central University of Europe (Tbilisi)": 5500,
        "Grigol Robakidze University": 5500,
        "SEU - Georgian National University": 5900,
        "Caucasus University": 6000,
        "David Tviidiani Medical University": 6000,
        "European University": 6500,
        "University of Georgia": 6500,
        "Petre Shotadze Tbilisi Medical Academy": 7000,
        "Alte University": 5500,
        "Kutaisi University": 4500,
    },
    "Uzbekistan": {
        "Tashkent State Medical University": 3500,
        "Bukhara State Medical Institute": 3200,
        "Samarkand State Medical University": 3500,
        "Fergana State Medical Institute": 3500,
        "Andijan State Medical Institute": 3500,
        "Urgench State Medical Institute": 3400,
        "Gulistan State Medical Institute": 2800,
        "Asian International University": 2600,
        "Bukhara Innovative Education & Medicine University": 2400,
    },
    "Kyrgyzstan": {
        "Asian Medical Institute": 2500,
        "Bishkek International Medical University": 3500,
        "International School of Medicine": 5000,
        "International Medical University": 4000,
        "Jalal-Abad State Medical University": 3500,
        "Kyrgyz Russian Slavic University": 5400,
        "Kyrgyz State Medical Academy": 4000,
        "Osh State Medical University": 3500,
    },
}
UNIVERSITIES_BY_COUNTRY = {
    country: sorted(list(universities.keys()))
    for country, universities in FEES_BY_COUNTRY.items()
}

os.makedirs(UPLOAD_DIR, exist_ok=True)


class PostgresAdapter:
    def __init__(self, conn: Any, driver: str) -> None:
        self.conn = conn
        self.driver = driver

    def _prepare_query(self, query: str) -> str:
        query = query.replace("last_insert_rowid()", "lastval()")
        return query.replace("?", "%s")

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> Any:
        prepared = self._prepare_query(query)
        cursor = self.conn.cursor()
        cursor.execute(prepared, params or None)
        return cursor

    def executemany(self, query: str, seq: list[tuple[Any, ...]]) -> Any:
        prepared = self._prepare_query(query)
        cursor = self.conn.cursor()
        cursor.executemany(prepared, seq)
        return cursor

    def executescript(self, script: str) -> None:
        statements = [stmt.strip() for stmt in script.split(";") if stmt.strip()]
        for statement in statements:
            self.execute(statement)

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def get_db() -> Any:
    if "db" not in g:
        if USE_POSTGRES:
            if PSYCOPG_DRIVER is None:
                raise RuntimeError(
                    "psycopg or psycopg2 is required for SUPABASE_DB_URL connections."
                )
            if PSYCOPG_DRIVER == "psycopg":
                conn = psycopg.connect(DB_URL, sslmode="require", row_factory=dict_row)
                g.db = PostgresAdapter(conn, "psycopg")
            else:
                conn = psycopg2.connect(DB_URL, sslmode="require", cursor_factory=DictCursor)
                g.db = PostgresAdapter(conn, "psycopg2")
        else:
            g.db = sqlite3.connect(DB_PATH)
            g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(_: Exception | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    db = get_db()
    if USE_POSTGRES:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS students (
                id SERIAL PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                status TEXT,
                stage TEXT,
                fees_due REAL,
                fees_paid REAL,
                country TEXT,
                university TEXT,
                fee_reminder_enabled INTEGER DEFAULT 0,
                last_fee_notice_at TEXT
            );

            CREATE TABLE IF NOT EXISTS courses (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                code TEXT,
                duration_months INTEGER,
                fee REAL
            );

            CREATE TABLE IF NOT EXISTS staff (
                id SERIAL PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                role TEXT,
                email TEXT,
                phone TEXT
            );

            CREATE TABLE IF NOT EXISTS admissions (
                id SERIAL PRIMARY KEY,
                student_id INTEGER,
                program TEXT NOT NULL,
                intake TEXT,
                status TEXT,
                notes TEXT,
                FOREIGN KEY (student_id) REFERENCES students (id)
            );

            CREATE TABLE IF NOT EXISTS documents (
                id SERIAL PRIMARY KEY,
                student_id INTEGER,
                filename TEXT NOT NULL,
                stored_name TEXT NOT NULL,
                uploaded_at TEXT NOT NULL,
                FOREIGN KEY (student_id) REFERENCES students (id)
            );

            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                student_id INTEGER,
                amount REAL NOT NULL,
                method TEXT,
                paid_on TEXT,
                note TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (student_id) REFERENCES students (id)
            );

            CREATE TABLE IF NOT EXISTS communications (
                id SERIAL PRIMARY KEY,
                student_id INTEGER,
                channel TEXT NOT NULL,
                subject TEXT,
                notes TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (student_id) REFERENCES students (id)
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                due_date TEXT,
                status TEXT NOT NULL,
                assigned_to INTEGER,
                notes TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (assigned_to) REFERENCES staff (id)
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id SERIAL PRIMARY KEY,
                student_id INTEGER,
                type TEXT NOT NULL,
                recipient TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                category TEXT,
                FOREIGN KEY (student_id) REFERENCES students (id)
            );

            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY,
                user_id INTEGER,
                action TEXT NOT NULL,
                entity TEXT NOT NULL,
                entity_id INTEGER,
                details TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (id)
            );
            """
        )
        db.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS fees_paid REAL")
        db.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS fees_due REAL")
        db.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS stage TEXT")
        db.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS country TEXT")
        db.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS university TEXT")
        db.execute(
            "ALTER TABLE students ADD COLUMN IF NOT EXISTS fee_reminder_enabled INTEGER DEFAULT 0"
        )
        db.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS last_fee_notice_at TEXT")
        db.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS student_id INTEGER")
        db.execute("ALTER TABLE notifications ADD COLUMN IF NOT EXISTS category TEXT")
    else:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS students (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                status TEXT,
                stage TEXT,
                fees_due REAL,
                fees_paid REAL,
                country TEXT,
                university TEXT,
                fee_reminder_enabled INTEGER DEFAULT 0,
                last_fee_notice_at TEXT
            );

            CREATE TABLE IF NOT EXISTS courses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                code TEXT,
                duration_months INTEGER,
                fee REAL
            );

            CREATE TABLE IF NOT EXISTS staff (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                role TEXT,
                email TEXT,
                phone TEXT
            );

            CREATE TABLE IF NOT EXISTS admissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER,
                program TEXT NOT NULL,
                intake TEXT,
                status TEXT,
                notes TEXT,
                FOREIGN KEY (student_id) REFERENCES students (id)
            );

            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER,
                filename TEXT NOT NULL,
                stored_name TEXT NOT NULL,
                uploaded_at TEXT NOT NULL,
                FOREIGN KEY (student_id) REFERENCES students (id)
            );

            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER,
                amount REAL NOT NULL,
                method TEXT,
                paid_on TEXT,
                note TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (student_id) REFERENCES students (id)
            );

            CREATE TABLE IF NOT EXISTS communications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER,
                channel TEXT NOT NULL,
                subject TEXT,
                notes TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (student_id) REFERENCES students (id)
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                due_date TEXT,
                status TEXT NOT NULL,
                assigned_to INTEGER,
                notes TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (assigned_to) REFERENCES staff (id)
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER,
                type TEXT NOT NULL,
                recipient TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                category TEXT,
                FOREIGN KEY (student_id) REFERENCES students (id)
            );

            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT NOT NULL,
                entity TEXT NOT NULL,
                entity_id INTEGER,
                details TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (id)
            );
            """
        )
        columns = db.execute("PRAGMA table_info(students)").fetchall()
        column_names = {column[1] for column in columns}
        if "fees_paid" not in column_names:
            db.execute("ALTER TABLE students ADD COLUMN fees_paid REAL")
        if "fees_due" not in column_names:
            db.execute("ALTER TABLE students ADD COLUMN fees_due REAL")
        if "stage" not in column_names:
            db.execute("ALTER TABLE students ADD COLUMN stage TEXT")
        if "country" not in column_names:
            db.execute("ALTER TABLE students ADD COLUMN country TEXT")
        if "university" not in column_names:
            db.execute("ALTER TABLE students ADD COLUMN university TEXT")
        if "fee_reminder_enabled" not in column_names:
            db.execute("ALTER TABLE students ADD COLUMN fee_reminder_enabled INTEGER DEFAULT 0")
        if "last_fee_notice_at" not in column_names:
            db.execute("ALTER TABLE students ADD COLUMN last_fee_notice_at TEXT")

        notification_columns = db.execute("PRAGMA table_info(notifications)").fetchall()
        notification_names = {column[1] for column in notification_columns}
        if "student_id" not in notification_names:
            db.execute("ALTER TABLE notifications ADD COLUMN student_id INTEGER")
        if "category" not in notification_names:
            db.execute("ALTER TABLE notifications ADD COLUMN category TEXT")

    user_count = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if user_count == 0:
        db.execute(
            "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
            (
                "admin",
                generate_password_hash("medzone123"),
                "admin",
                datetime.utcnow().isoformat(),
            ),
        )
    db.commit()


with app.app_context():
    init_db()


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def get_last_insert_id(db: Any) -> int:
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


def get_current_user() -> Any | None:
    user_id = session.get("user_id")
    if not user_id:
        return None
    db = get_db()
    return db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


@app.before_request
def load_user() -> None:
    g.user = get_current_user()


@app.before_request
def require_login() -> Any:
    if request.endpoint in {"login", "static"}:
        return None
    if g.user is None:
        return redirect(url_for("login"))
    maybe_queue_fee_reminders()
    return None


def login_required(view: Any) -> Any:
    @wraps(view)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if g.user is None and request.endpoint not in {"login", "static"}:
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped


def log_action(action: str, entity: str, entity_id: int | None, details: str | None = None) -> None:
    db = get_db()
    db.execute(
        "INSERT INTO audit_logs (user_id, action, entity, entity_id, details, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (
            g.user["id"] if g.user else None,
            action,
            entity,
            entity_id,
            details,
            datetime.utcnow().isoformat(),
        ),
    )
    db.commit()


def maybe_queue_fee_reminders() -> None:
    db = get_db()
    now = datetime.utcnow()
    rows = db.execute(
        """
        SELECT students.id, students.first_name, students.last_name, students.email,
               students.fees_due, students.last_fee_notice_at,
               COALESCE(SUM(payments.amount), 0) as paid_total
        FROM students
        LEFT JOIN payments ON payments.student_id = students.id
        WHERE students.fee_reminder_enabled = 1
          AND students.email IS NOT NULL AND students.email != ''
        GROUP BY students.id
        """
    ).fetchall()

    queued_any = False
    for row in rows:
        fees_due = row["fees_due"] or 0
        paid_total = row["paid_total"] or 0
        balance = fees_due - paid_total
        if balance <= 0:
            continue

        last_notice = row["last_fee_notice_at"]
        if last_notice:
            try:
                last_dt = datetime.fromisoformat(last_notice)
                if (now - last_dt).total_seconds() < 4 * 24 * 60 * 60:
                    continue
            except ValueError:
                pass

        message = (
            f"Hello {row['first_name']}, your pending fees balance is ${balance:.2f}. "
            "Please complete the payment at your earliest convenience."
        )
        db.execute(
            "INSERT INTO notifications (student_id, type, recipient, message, status, created_at, category) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                "Email",
                row["email"],
                message,
                "Queued",
                now.isoformat(),
                "fee_reminder",
            ),
        )
        db.execute(
            "UPDATE students SET last_fee_notice_at = ? WHERE id = ?",
            (now.isoformat(), row["id"]),
        )
        queued_any = True

    if queued_any:
        db.commit()


def role_required(role: str) -> Any:
    def decorator(view: Any) -> Any:
        @wraps(view)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if g.user is None or g.user["role"] != role:
                abort(403)
            return view(*args, **kwargs)

        return wrapped

    return decorator


def publish_event(event_name: str) -> None:
    for subscriber in list(event_subscribers):
        subscriber.put(event_name)


@app.route("/events")
def events() -> Response:
    def stream() -> Any:
        subscriber: queue.Queue[str] = queue.Queue()
        event_subscribers.append(subscriber)
        try:
            while True:
                event_name = subscriber.get()
                yield f"event: {event_name}\ndata: updated\n\n"
        finally:
            event_subscribers.remove(subscriber)

    return Response(stream(), mimetype="text/event-stream")


@app.route("/login", methods=["GET", "POST"])
def login() -> str:
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            return redirect(url_for("index"))
        return render_template("login.html", error="Invalid username or password.")

    return render_template("login.html", error=None)


@app.route("/logout")
def logout() -> str:
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def index() -> str:
    db = get_db()
    counts = {
        "students": db.execute("SELECT COUNT(*) FROM students").fetchone()[0],
        "courses": db.execute("SELECT COUNT(*) FROM courses").fetchone()[0],
        "staff": db.execute("SELECT COUNT(*) FROM staff").fetchone()[0],
    }
    return render_template("index.html", counts=counts)


@app.route("/students")
def students_list() -> str:
    db = get_db()
    students = db.execute(
        "SELECT * FROM students ORDER BY id DESC"
    ).fetchall()
    return render_template("students_list.html", students=students, stages=STUDENT_STAGES)


@app.route("/students/new", methods=["GET", "POST"])
def students_new() -> str:
    if request.method == "POST":
        form = request.form
        first_name = form.get("first_name", "").strip()
        last_name = form.get("last_name", "").strip()
        email = form.get("email", "").strip()
        phone = form.get("phone", "").strip()
        status = form.get("status", "").strip()
        stage = form.get("stage", "").strip()
        country = form.get("country", "").strip()
        university = form.get("university", "").strip()
        fees_due = FEES_BY_COUNTRY.get(country, {}).get(university)

        if not first_name or not last_name:
            return render_template(
                "students_form.html",
                student=form.to_dict(),
                error="First name and last name are required.",
                stages=STUDENT_STAGES,
                universities_by_country=json.dumps(UNIVERSITIES_BY_COUNTRY),
                fees_by_country=json.dumps(FEES_BY_COUNTRY),
            )

        db = get_db()
        db.execute(
            "INSERT INTO students (first_name, last_name, email, phone, status, stage, country, university, fees_due) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (first_name, last_name, email, phone, status, stage, country, university, fees_due),
        )
        db.commit()
        publish_event("students_updated")
        log_action("create", "student", get_last_insert_id(db))
        return redirect(url_for("students_list"))

    return render_template(
        "students_form.html",
        student=None,
        error=None,
        stages=STUDENT_STAGES,
        universities_by_country=json.dumps(UNIVERSITIES_BY_COUNTRY),
        fees_by_country=json.dumps(FEES_BY_COUNTRY),
    )


@app.route("/students/<int:student_id>/edit", methods=["GET", "POST"])
def students_edit(student_id: int) -> str:
    db = get_db()
    student = db.execute(
        "SELECT * FROM students WHERE id = ?", (student_id,)
    ).fetchone()
    if student is None:
        abort(404)

    if request.method == "POST":
        form = request.form
        first_name = form.get("first_name", "").strip()
        last_name = form.get("last_name", "").strip()
        email = form.get("email", "").strip()
        phone = form.get("phone", "").strip()
        status = form.get("status", "").strip()
        stage = form.get("stage", "").strip()
        country = form.get("country", "").strip()
        university = form.get("university", "").strip()
        fees_due = FEES_BY_COUNTRY.get(country, {}).get(university)

        if not first_name or not last_name:
            return render_template(
                "students_form.html",
                student=form.to_dict(),
                error="First name and last name are required.",
                stages=STUDENT_STAGES,
                universities_by_country=json.dumps(UNIVERSITIES_BY_COUNTRY),
                fees_by_country=json.dumps(FEES_BY_COUNTRY),
            )

        db.execute(
            "UPDATE students SET first_name = ?, last_name = ?, email = ?, phone = ?, status = ?, stage = ?, country = ?, university = ?, fees_due = ? WHERE id = ?",
            (first_name, last_name, email, phone, status, stage, country, university, fees_due, student_id),
        )
        db.commit()
        publish_event("students_updated")
        log_action("update", "student", student_id)
        return redirect(url_for("students_list"))

    return render_template(
        "students_form.html",
        student=dict(student),
        error=None,
        stages=STUDENT_STAGES,
        universities_by_country=json.dumps(UNIVERSITIES_BY_COUNTRY),
        fees_by_country=json.dumps(FEES_BY_COUNTRY),
    )


@app.route("/students/<int:student_id>/delete", methods=["POST"])
def students_delete(student_id: int) -> str:
    db = get_db()
    db.execute("DELETE FROM students WHERE id = ?", (student_id,))
    db.commit()
    publish_event("students_updated")
    log_action("delete", "student", student_id)
    return redirect(url_for("students_list"))


@app.route("/courses")
def courses_list() -> str:
    db = get_db()
    courses = db.execute("SELECT * FROM courses ORDER BY id DESC").fetchall()
    return render_template("courses_list.html", courses=courses)


@app.route("/courses/new", methods=["GET", "POST"])
def courses_new() -> str:
    if request.method == "POST":
        form = request.form
        name = form.get("name", "").strip()
        code = form.get("code", "").strip()
        duration_months = form.get("duration_months", "").strip()
        fee = form.get("fee", "").strip()

        if not name:
            return render_template(
                "courses_form.html", course=form.to_dict(), error="Name is required."
            )

        db = get_db()
        db.execute(
            "INSERT INTO courses (name, code, duration_months, fee) VALUES (?, ?, ?, ?)",
            (
                name,
                code or None,
                int(duration_months) if duration_months else None,
                float(fee) if fee else None,
            ),
        )
        db.commit()
        log_action("create", "course", get_last_insert_id(db))
        return redirect(url_for("courses_list"))

    return render_template("courses_form.html", course=None, error=None)


@app.route("/courses/<int:course_id>/edit", methods=["GET", "POST"])
def courses_edit(course_id: int) -> str:
    db = get_db()
    course = db.execute("SELECT * FROM courses WHERE id = ?", (course_id,)).fetchone()
    if course is None:
        abort(404)

    if request.method == "POST":
        form = request.form
        name = form.get("name", "").strip()
        code = form.get("code", "").strip()
        duration_months = form.get("duration_months", "").strip()
        fee = form.get("fee", "").strip()

        if not name:
            return render_template(
                "courses_form.html", course=form.to_dict(), error="Name is required."
            )

        db.execute(
            "UPDATE courses SET name = ?, code = ?, duration_months = ?, fee = ? WHERE id = ?",
            (
                name,
                code or None,
                int(duration_months) if duration_months else None,
                float(fee) if fee else None,
                course_id,
            ),
        )
        db.commit()
        log_action("update", "course", course_id)
        return redirect(url_for("courses_list"))

    return render_template("courses_form.html", course=dict(course), error=None)


@app.route("/courses/<int:course_id>/delete", methods=["POST"])
def courses_delete(course_id: int) -> str:
    db = get_db()
    db.execute("DELETE FROM courses WHERE id = ?", (course_id,))
    db.commit()
    log_action("delete", "course", course_id)
    return redirect(url_for("courses_list"))


@app.route("/staff")
def staff_list() -> str:
    db = get_db()
    staff = db.execute("SELECT * FROM staff ORDER BY id DESC").fetchall()
    return render_template("staff_list.html", staff=staff)


@app.route("/staff/new", methods=["GET", "POST"])
def staff_new() -> str:
    if request.method == "POST":
        form = request.form
        first_name = form.get("first_name", "").strip()
        last_name = form.get("last_name", "").strip()
        role = form.get("role", "").strip()
        email = form.get("email", "").strip()
        phone = form.get("phone", "").strip()

        if not first_name or not last_name:
            return render_template(
                "staff_form.html",
                staff=form.to_dict(),
                error="First name and last name are required.",
            )

        db = get_db()
        db.execute(
            "INSERT INTO staff (first_name, last_name, role, email, phone) VALUES (?, ?, ?, ?, ?)",
            (first_name, last_name, role, email, phone),
        )
        db.commit()
        log_action("create", "staff", get_last_insert_id(db))
        return redirect(url_for("staff_list"))

    return render_template("staff_form.html", staff=None, error=None)


@app.route("/staff/<int:staff_id>/edit", methods=["GET", "POST"])
def staff_edit(staff_id: int) -> str:
    db = get_db()
    staff_member = db.execute(
        "SELECT * FROM staff WHERE id = ?", (staff_id,)
    ).fetchone()
    if staff_member is None:
        abort(404)

    if request.method == "POST":
        form = request.form
        first_name = form.get("first_name", "").strip()
        last_name = form.get("last_name", "").strip()
        role = form.get("role", "").strip()
        email = form.get("email", "").strip()
        phone = form.get("phone", "").strip()

        if not first_name or not last_name:
            return render_template(
                "staff_form.html",
                staff=form.to_dict(),
                error="First name and last name are required.",
            )

        db.execute(
            "UPDATE staff SET first_name = ?, last_name = ?, role = ?, email = ?, phone = ? WHERE id = ?",
            (first_name, last_name, role, email, phone, staff_id),
        )
        db.commit()
        log_action("update", "staff", staff_id)
        return redirect(url_for("staff_list"))

    return render_template("staff_form.html", staff=dict(staff_member), error=None)


@app.route("/staff/<int:staff_id>/delete", methods=["POST"])
def staff_delete(staff_id: int) -> str:
    db = get_db()
    db.execute("DELETE FROM staff WHERE id = ?", (staff_id,))
    db.commit()
    log_action("delete", "staff", staff_id)
    return redirect(url_for("staff_list"))


@app.route("/admissions")
def admissions_list() -> str:
    db = get_db()
    admissions = db.execute(
        """
        SELECT admissions.*, students.first_name, students.last_name
        FROM admissions
        LEFT JOIN students ON admissions.student_id = students.id
        ORDER BY admissions.id DESC
        """
    ).fetchall()
    return render_template("admissions_list.html", admissions=admissions)


@app.route("/fees")
def fees_list() -> str:
    db = get_db()
    status_filter = request.args.get("status", "").strip()
    unpaid_only = request.args.get("unpaid", "") == "1"
    min_paid_raw = request.args.get("min_paid", "").strip()
    max_paid_raw = request.args.get("max_paid", "").strip()

    where_clauses: list[str] = []
    having_clauses: list[str] = []
    params: list[Any] = []

    if status_filter:
        where_clauses.append("status = ?")
        params.append(status_filter)
    if unpaid_only:
        having_clauses.append("paid_total = 0")

    if min_paid_raw:
        try:
            min_paid = float(min_paid_raw)
            having_clauses.append("paid_total >= ?")
            params.append(min_paid)
        except ValueError:
            pass

    if max_paid_raw:
        try:
            max_paid = float(max_paid_raw)
            having_clauses.append("paid_total <= ?")
            params.append(max_paid)
        except ValueError:
            pass

    query = (
        "SELECT students.id, students.first_name, students.last_name, students.email, "
        "students.status, students.fees_due, COALESCE(SUM(payments.amount), 0) as paid_total "
        "FROM students LEFT JOIN payments ON payments.student_id = students.id"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " GROUP BY students.id"
    if having_clauses:
        query += " HAVING " + " AND ".join(having_clauses)
    query += " ORDER BY students.last_name, students.first_name"

    students = db.execute(query, params).fetchall()
    statuses = db.execute(
        "SELECT DISTINCT status FROM students WHERE status IS NOT NULL AND status != '' ORDER BY status"
    ).fetchall()

    filters = {
        "status": status_filter,
        "unpaid": unpaid_only,
        "min_paid": min_paid_raw,
        "max_paid": max_paid_raw,
    }

    return render_template(
        "fees_list.html", students=students, statuses=statuses, filters=filters
    )


@app.route("/fees/<int:student_id>/edit", methods=["GET", "POST"])
def fees_edit(student_id: int) -> str:
    db = get_db()
    student = db.execute(
        "SELECT id, first_name, last_name, fees_due FROM students WHERE id = ?",
        (student_id,),
    ).fetchone()
    if student is None:
        abort(404)

    if request.method == "POST":
        form = request.form
        action = form.get("action", "")
        if action == "update_due":
            fees_due = form.get("fees_due", "").strip()
            db.execute(
                "UPDATE students SET fees_due = ? WHERE id = ?",
                (float(fees_due) if fees_due else None, student_id),
            )
            db.commit()
            publish_event("fees_updated")
            log_action("update", "fees_due", student_id)
            return redirect(url_for("fees_edit", student_id=student_id))

        if action == "add_payment":
            amount_raw = form.get("amount", "").strip()
            method = form.get("method", "").strip()
            paid_on = form.get("paid_on", "").strip()
            note = form.get("note", "").strip()

            if amount_raw:
                db.execute(
                    "INSERT INTO payments (student_id, amount, method, paid_on, note, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        student_id,
                        float(amount_raw),
                        method or None,
                        paid_on or None,
                        note or None,
                        datetime.utcnow().isoformat(),
                    ),
                )
                db.commit()
                publish_event("fees_updated")
                log_action("create", "payment", get_last_insert_id(db))

            return redirect(url_for("fees_edit", student_id=student_id))

    payments = db.execute(
        "SELECT * FROM payments WHERE student_id = ? ORDER BY created_at DESC",
        (student_id,),
    ).fetchall()
    totals = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE student_id = ?",
        (student_id,),
    ).fetchone()[0]
    balance = (student["fees_due"] or 0) - totals

    return render_template(
        "fees_form.html",
        student=dict(student),
        payments=payments,
        total_paid=totals,
        balance=balance,
        error=None,
    )


@app.route("/admissions/new", methods=["GET", "POST"])
def admissions_new() -> str:
    db = get_db()
    students = db.execute(
        "SELECT id, first_name, last_name FROM students ORDER BY first_name"
    ).fetchall()

    if request.method == "POST":
        form = request.form
        student_id = form.get("student_id", "").strip()
        program = form.get("program", "").strip()
        intake = form.get("intake", "").strip()
        status = form.get("status", "").strip()
        notes = form.get("notes", "").strip()

        if not program:
            return render_template(
                "admissions_form.html",
                admission=form.to_dict(),
                students=students,
                error="Program is required.",
            )

        db.execute(
            "INSERT INTO admissions (student_id, program, intake, status, notes) VALUES (?, ?, ?, ?, ?)",
            (int(student_id) if student_id else None, program, intake, status, notes),
        )
        db.commit()
        log_action("create", "admission", get_last_insert_id(db))
        return redirect(url_for("admissions_list"))

    return render_template(
        "admissions_form.html", admission=None, students=students, error=None
    )


@app.route("/admissions/<int:admission_id>/edit", methods=["GET", "POST"])
def admissions_edit(admission_id: int) -> str:
    db = get_db()
    admission = db.execute(
        "SELECT * FROM admissions WHERE id = ?", (admission_id,)
    ).fetchone()
    if admission is None:
        abort(404)

    students = db.execute(
        "SELECT id, first_name, last_name FROM students ORDER BY first_name"
    ).fetchall()

    if request.method == "POST":
        form = request.form
        student_id = form.get("student_id", "").strip()
        program = form.get("program", "").strip()
        intake = form.get("intake", "").strip()
        status = form.get("status", "").strip()
        notes = form.get("notes", "").strip()

        if not program:
            return render_template(
                "admissions_form.html",
                admission=form.to_dict(),
                students=students,
                error="Program is required.",
            )

        db.execute(
            "UPDATE admissions SET student_id = ?, program = ?, intake = ?, status = ?, notes = ? WHERE id = ?",
            (int(student_id) if student_id else None, program, intake, status, notes, admission_id),
        )
        db.commit()
        log_action("update", "admission", admission_id)
        return redirect(url_for("admissions_list"))

    return render_template(
        "admissions_form.html", admission=dict(admission), students=students, error=None
    )


@app.route("/admissions/<int:admission_id>/delete", methods=["POST"])
def admissions_delete(admission_id: int) -> str:
    db = get_db()
    db.execute("DELETE FROM admissions WHERE id = ?", (admission_id,))
    db.commit()
    log_action("delete", "admission", admission_id)
    return redirect(url_for("admissions_list"))


@app.route("/documents")
def documents_list() -> str:
    db = get_db()
    documents = db.execute(
        """
        SELECT documents.*, students.first_name, students.last_name
        FROM documents
        LEFT JOIN students ON documents.student_id = students.id
        ORDER BY documents.uploaded_at DESC
        """
    ).fetchall()
    students = db.execute(
        "SELECT id, first_name, last_name FROM students ORDER BY first_name"
    ).fetchall()
    return render_template("documents_list.html", documents=documents, students=students)


@app.route("/documents/upload", methods=["POST"])
def documents_upload() -> str:
    db = get_db()
    student_id = request.form.get("student_id", "").strip()
    upload = request.files.get("file")
    if upload and allowed_file(upload.filename):
        safe_name = secure_filename(upload.filename)
        stored_name = f"{datetime.utcnow().timestamp()}_{safe_name}"
        upload.save(os.path.join(UPLOAD_DIR, stored_name))
        db.execute(
            "INSERT INTO documents (student_id, filename, stored_name, uploaded_at) VALUES (?, ?, ?, ?)",
            (
                int(student_id) if student_id else None,
                safe_name,
                stored_name,
                datetime.utcnow().isoformat(),
            ),
        )
        db.commit()
        log_action("create", "document", get_last_insert_id(db))
    return redirect(url_for("documents_list"))


@app.route("/documents/<int:document_id>/download")
def documents_download(document_id: int) -> Response:
    db = get_db()
    document = db.execute(
        "SELECT stored_name, filename FROM documents WHERE id = ?",
        (document_id,),
    ).fetchone()
    if document is None:
        abort(404)
    return send_from_directory(UPLOAD_DIR, document["stored_name"], as_attachment=True, download_name=document["filename"])


@app.route("/documents/<int:document_id>/delete", methods=["POST"])
def documents_delete(document_id: int) -> str:
    db = get_db()
    document = db.execute(
        "SELECT stored_name FROM documents WHERE id = ?",
        (document_id,),
    ).fetchone()
    if document:
        try:
            os.remove(os.path.join(UPLOAD_DIR, document["stored_name"]))
        except FileNotFoundError:
            pass
    db.execute("DELETE FROM documents WHERE id = ?", (document_id,))
    db.commit()
    log_action("delete", "document", document_id)
    return redirect(url_for("documents_list"))




@app.route("/notifications")
def notifications_list() -> str:
    db = get_db()
    show_paid = request.args.get("show_paid", "") == "1"
    notifications = db.execute(
        """
        SELECT notifications.*, students.first_name, students.last_name
        FROM notifications
        LEFT JOIN students ON notifications.student_id = students.id
        ORDER BY notifications.created_at DESC
        """
    ).fetchall()
    query = (
        "SELECT students.id, students.first_name, students.last_name, students.email, "
        "students.fees_due, COALESCE(SUM(payments.amount), 0) as paid_total "
        "FROM students LEFT JOIN payments ON payments.student_id = students.id "
        "GROUP BY students.id"
    )
    if not show_paid:
        query += " HAVING (COALESCE(students.fees_due, 0) - COALESCE(SUM(payments.amount), 0)) > 0"
    query += " ORDER BY students.first_name"
    students = db.execute(query).fetchall()
    return render_template(
        "notifications_list.html",
        notifications=notifications,
        students=students,
        show_paid=show_paid,
    )


@app.route("/notifications/new", methods=["POST"])
def notifications_new() -> str:
    db = get_db()
    notif_type = request.form.get("type", "").strip() or "Email"
    student_id = request.form.get("student_id", "").strip()
    recipient = request.form.get("recipient", "").strip()
    message = request.form.get("message", "").strip()
    enable_reminders = request.form.get("fee_reminder", "") == "on"

    if student_id:
        student = db.execute(
            "SELECT id, email FROM students WHERE id = ?",
            (int(student_id),),
        ).fetchone()
        if student:
            recipient = student["email"] or recipient

    if recipient and message:
        db.execute(
            "INSERT INTO notifications (student_id, type, recipient, message, status, created_at, category) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                int(student_id) if student_id else None,
                notif_type,
                recipient,
                message,
                "Queued",
                datetime.utcnow().isoformat(),
                "manual",
            ),
        )
        db.commit()
        log_action("create", "notification", get_last_insert_id(db))

    if student_id and enable_reminders:
        db.execute(
            "UPDATE students SET fee_reminder_enabled = 1, last_fee_notice_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), int(student_id)),
        )
        db.commit()
    return redirect(url_for("notifications_list"))


@app.route("/reports")
def reports() -> str:
    db = get_db()
    totals = db.execute(
        "SELECT COUNT(*) FROM students"
    ).fetchone()[0]
    payments_total = db.execute("SELECT COALESCE(SUM(amount), 0) FROM payments").fetchone()[0]
    fees_due_total = db.execute("SELECT COALESCE(SUM(fees_due), 0) FROM students").fetchone()[0]
    balance_total = fees_due_total - payments_total
    return render_template(
        "reports.html",
        totals=totals,
        payments_total=payments_total,
        fees_due_total=fees_due_total,
        balance_total=balance_total,
    )


@app.route("/users", methods=["GET", "POST"])
@role_required("admin")
def users_list() -> str:
    db = get_db()
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        role = request.form.get("role", "staff").strip() or "staff"
        if username and password:
            db.execute(
                "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
                (
                    username,
                    generate_password_hash(password),
                    role,
                    datetime.utcnow().isoformat(),
                ),
            )
            db.commit()
            log_action("create", "user", get_last_insert_id(db))
        return redirect(url_for("users_list"))

    users = db.execute("SELECT id, username, role, created_at FROM users").fetchall()
    return render_template("users_list.html", users=users)


@app.route("/audit")
@role_required("admin")
def audit_logs() -> str:
    db = get_db()
    logs = db.execute(
        """
        SELECT audit_logs.*, users.username
        FROM audit_logs
        LEFT JOIN users ON audit_logs.user_id = users.id
        ORDER BY audit_logs.created_at DESC
        LIMIT 200
        """
    ).fetchall()
    return render_template("audit_logs.html", logs=logs)


@app.route("/exports")
def exports() -> str:
    return render_template("exports.html")


def csv_response(rows: list[dict[str, Any]], filename: str) -> Response:
    buffer = io.StringIO()
    if rows:
        writer = csv.DictWriter(buffer, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    output = buffer.getvalue()
    response = Response(output, mimetype="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response


@app.route("/export/students.csv")
def export_students() -> Response:
    db = get_db()
    rows = db.execute(
        "SELECT id, first_name, last_name, email, phone, status, stage, fees_due FROM students"
    ).fetchall()
    return csv_response([dict(row) for row in rows], "students.csv")


@app.route("/export/fees.csv")
def export_fees() -> Response:
    db = get_db()
    rows = db.execute(
        """
        SELECT students.id, students.first_name, students.last_name, students.fees_due,
               COALESCE(SUM(payments.amount), 0) as paid_total
        FROM students LEFT JOIN payments ON payments.student_id = students.id
        GROUP BY students.id
        """
    ).fetchall()
    formatted = []
    for row in rows:
        record = dict(row)
        record["balance"] = (record.get("fees_due") or 0) - (record.get("paid_total") or 0)
        formatted.append(record)
    return csv_response(formatted, "fees.csv")


@app.route("/export/payments.csv")
def export_payments() -> Response:
    db = get_db()
    rows = db.execute(
        """
        SELECT payments.id, students.first_name, students.last_name, payments.amount,
               payments.method, payments.paid_on, payments.note, payments.created_at
        FROM payments LEFT JOIN students ON payments.student_id = students.id
        ORDER BY payments.created_at DESC
        """
    ).fetchall()
    return csv_response([dict(row) for row in rows], "payments.csv")




@app.route("/calendar")
def calendar_view() -> str:
    db = get_db()
    tasks = db.execute(
        """
        SELECT tasks.*, staff.first_name, staff.last_name
        FROM tasks
        LEFT JOIN staff ON tasks.assigned_to = staff.id
        ORDER BY tasks.due_date IS NULL, tasks.due_date
        """
    ).fetchall()
    return render_template("calendar.html", tasks=tasks)


@app.route("/calendar.ics")
def calendar_export() -> Response:
    db = get_db()
    tasks = db.execute(
        "SELECT id, title, due_date FROM tasks WHERE due_date IS NOT NULL"
    ).fetchall()
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Medzone//Agency Calendar//EN",
    ]
    for task in tasks:
        due = task["due_date"].replace("-", "")
        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:task-{task['id']}@medzone",
                f"DTSTART;VALUE=DATE:{due}",
                f"SUMMARY:{task['title']}",
                "END:VEVENT",
            ]
        )
    lines.append("END:VCALENDAR")
    response = Response("\n".join(lines), mimetype="text/calendar")
    response.headers["Content-Disposition"] = "attachment; filename=medzone_calendar.ics"
    return response


if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "5000")),
        debug=os.environ.get("FLASK_DEBUG") == "1",
    )
