from __future__ import annotations

from functools import wraps
from typing import Any, Callable

from flask import g, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from db import get_conn


AUTH_SCHEMA = "auth"
SESSION_USER_ID_KEY = "auth_user_id"

ROLE_DEFINITIONS = {
    "super_admin": {
        "label": "Super Admin",
        "modules": {"spp", "mzdovy"},
        "manage_users": True,
    },
    "admin": {
        "label": "Administrator",
        "modules": {"spp", "mzdovy"},
        "manage_users": True,
    },
    "finance_hr": {
        "label": "Finance / HR",
        "modules": {"mzdovy"},
        "manage_users": False,
    },
    "stavba": {
        "label": "Stavba",
        "modules": {"spp"},
        "manage_users": False,
    },
}


class AuthStore:
    def has_users(self) -> bool:
        with get_conn(schema=AUTH_SCHEMA) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE is_active = TRUE LIMIT 1")
            return cur.fetchone() is not None

    def list_users(self) -> list[dict[str, Any]]:
        with get_conn(schema=AUTH_SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, username, full_name, role, is_active, created_at, updated_at
                FROM users
                ORDER BY LOWER(username), username
                """
            )
            return [dict(row) for row in cur.fetchall()]

    def get_user_by_id(self, user_id: int) -> dict[str, Any] | None:
        with get_conn(schema=AUTH_SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, username, full_name, role, is_active, created_at, updated_at
                FROM users
                WHERE id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def get_user_for_login(self, username: str) -> dict[str, Any] | None:
        with get_conn(schema=AUTH_SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, username, full_name, role, is_active, password_hash, created_at, updated_at
                FROM users
                WHERE LOWER(username) = LOWER(%s)
                """,
                (username,),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    def create_user(self, *, username: str, full_name: str, password: str, role: str) -> int:
        with get_conn(schema=AUTH_SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users(username, full_name, password_hash, role, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, TRUE, NOW()::text, NOW()::text)
                RETURNING id
                """,
                (
                    username.strip(),
                    full_name.strip(),
                    generate_password_hash(password),
                    role,
                ),
            )
            return int(cur.fetchone()["id"])

    def update_user(
        self,
        user_id: int,
        *,
        full_name: str | None = None,
        role: str | None = None,
        is_active: bool | None = None,
        password: str | None = None,
    ) -> None:
        updates: list[str] = []
        values: list[Any] = []
        if full_name is not None:
            updates.append("full_name = %s")
            values.append(full_name.strip())
        if role is not None:
            updates.append("role = %s")
            values.append(role)
        if is_active is not None:
            updates.append("is_active = %s")
            values.append(bool(is_active))
        if password:
            updates.append("password_hash = %s")
            values.append(generate_password_hash(password))
        updates.append("updated_at = NOW()::text")
        values.append(user_id)
        with get_conn(schema=AUTH_SCHEMA) as conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE users SET {', '.join(updates)} WHERE id = %s",
                tuple(values),
            )

    def delete_user(self, user_id: int) -> None:
        with get_conn(schema=AUTH_SCHEMA) as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))


store = AuthStore()


def role_exists(role: str | None) -> bool:
    return bool(role and role in ROLE_DEFINITIONS)


def role_label(role: str | None) -> str:
    return ROLE_DEFINITIONS.get(role or "", {}).get("label", "Unknown")


def can_access_module(user: dict[str, Any] | None, module_name: str) -> bool:
    if not user:
        return False
    modules = ROLE_DEFINITIONS.get(user.get("role") or "", {}).get("modules", set())
    return module_name in modules


def can_manage_users(user: dict[str, Any] | None) -> bool:
    if not user:
        return False
    return bool(ROLE_DEFINITIONS.get(user.get("role") or "", {}).get("manage_users"))


def current_user() -> dict[str, Any] | None:
    user = getattr(g, "current_user", None)
    if user is not None:
        return user
    user_id = session.get(SESSION_USER_ID_KEY)
    if not user_id:
        g.current_user = None
        return None
    user = store.get_user_by_id(int(user_id))
    if not user or not user.get("is_active"):
        logout_user()
        g.current_user = None
        return None
    g.current_user = user
    return user


def login_user(user: dict[str, Any]) -> None:
    session[SESSION_USER_ID_KEY] = int(user["id"])
    session.permanent = True
    g.current_user = dict(user)


def logout_user() -> None:
    session.pop(SESSION_USER_ID_KEY, None)
    g.current_user = None


def authenticate(username: str, password: str) -> dict[str, Any] | None:
    user = store.get_user_for_login(username.strip())
    if not user or not user.get("is_active"):
        return None
    if not check_password_hash(user["password_hash"], password):
        return None
    user.pop("password_hash", None)
    return user


def _is_api_request() -> bool:
    return request.path.startswith("/api/") or request.path.startswith("/mzdovy/api/")


def _redirect_target() -> str:
    next_url = request.args.get("next", "").strip()
    if next_url.startswith("/") and not next_url.startswith("//"):
        return next_url
    return url_for("root")


def unauthorized_response(message: str = "Přihlaste se prosím.") -> Any:
    if _is_api_request():
        return jsonify({"error": message}), 401
    return redirect(url_for("login_page", next=request.path))


def forbidden_response(message: str = "Na tuto část platformy nemáte přístup.") -> Any:
    if _is_api_request():
        return jsonify({"error": message}), 403
    return render_template("access_denied.html", title="Přístup odepřen", message=message), 403


def login_required(view: Callable) -> Callable:
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user():
            return unauthorized_response()
        return view(*args, **kwargs)

    return wrapped


def require_roles(*roles: str) -> Callable:
    def decorator(view: Callable) -> Callable:
        @wraps(view)
        def wrapped(*args, **kwargs):
            user = current_user()
            if not user:
                return unauthorized_response()
            if user.get("role") not in roles:
                return forbidden_response()
            return view(*args, **kwargs)

        return wrapped

    return decorator


def require_module(module_name: str) -> Any | None:
    user = current_user()
    if not user:
        return unauthorized_response()
    if not can_access_module(user, module_name):
        return forbidden_response()
    return None
