from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from siamquantum.config import settings


class SupabaseError(RuntimeError):
    """Raised when a Supabase API call fails."""


@dataclass
class SupabaseUser:
    id: str
    email: str | None
    created_at: str | None
    raw: dict[str, Any]

    @property
    def user_metadata(self) -> dict[str, Any]:
        value = self.raw.get("user_metadata")
        return value if isinstance(value, dict) else {}

    @property
    def app_metadata(self) -> dict[str, Any]:
        value = self.raw.get("app_metadata")
        return value if isinstance(value, dict) else {}

    @property
    def display_name(self) -> str | None:
        meta = self.user_metadata
        return (
            _as_text(meta.get("full_name"))
            or _as_text(meta.get("name"))
            or _as_text(meta.get("user_name"))
        )

    @property
    def avatar_url(self) -> str | None:
        meta = self.user_metadata
        return _as_text(meta.get("avatar_url")) or _as_text(meta.get("picture"))


def _as_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _configured_setting(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def supabase_enabled() -> bool:
    return bool(
        _configured_setting(settings.supabase_url)
        and _configured_setting(settings.supabase_publishable_key)
        and _configured_setting(settings.supabase_secret_key)
    )


def require_supabase() -> None:
    if not supabase_enabled():
        raise SupabaseError(
            "Supabase is not configured. Set SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY, and SUPABASE_SECRET_KEY."
        )


def _headers(
    *,
    api_key: str,
    access_token: str | None = None,
    prefer: str | None = None,
    content_type: str = "application/json",
) -> dict[str, str]:
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {access_token or api_key}",
        "Content-Type": content_type,
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


def _request(
    method: str,
    path: str,
    *,
    api_key: str,
    access_token: str | None = None,
    params: dict[str, Any] | None = None,
    json_body: Any | None = None,
    prefer: str | None = None,
) -> Any:
    require_supabase()
    url = settings.supabase_url.rstrip("/") + path
    try:
        response = httpx.request(
            method,
            url,
            headers=_headers(api_key=api_key, access_token=access_token, prefer=prefer),
            params=params,
            json=json_body,
            timeout=20.0,
        )
    except httpx.HTTPError as exc:
        raise SupabaseError(f"Supabase request could not be completed: {exc}") from exc
    if response.status_code >= 400:
        detail = response.text.strip()
        raise SupabaseError(f"Supabase request failed ({response.status_code}): {detail}")
    if not response.content:
        return None
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type:
        return response.json()
    return response.text


def verify_user_access_token(access_token: str) -> SupabaseUser:
    payload = _request(
        "GET",
        "/auth/v1/user",
        api_key=settings.supabase_publishable_key,
        access_token=access_token,
    )
    if not isinstance(payload, dict) or "id" not in payload:
        raise SupabaseError("Supabase user payload was invalid.")
    return SupabaseUser(
        id=str(payload["id"]),
        email=_as_text(payload.get("email")),
        created_at=_as_text(payload.get("created_at")),
        raw=payload,
    )


def rest_select(
    table: str,
    *,
    select: str = "*",
    filters: dict[str, str] | None = None,
    access_token: str | None = None,
    use_service_role: bool = False,
    single: bool = False,
    limit: int | None = None,
    order: str | None = None,
) -> list[dict[str, Any]] | dict[str, Any] | None:
    params: dict[str, Any] = {"select": select}
    if filters:
        params.update(filters)
    if limit is not None:
        params["limit"] = str(limit)
    if order:
        params["order"] = order
    rows = _request(
        "GET",
        f"/rest/v1/{table}",
        api_key=settings.supabase_secret_key if use_service_role else settings.supabase_publishable_key,
        access_token=access_token if not use_service_role else None,
        params=params,
    )
    if single:
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows if isinstance(rows, dict) else None
    return rows if isinstance(rows, list) else []


def rest_insert(
    table: str,
    payload: dict[str, Any] | list[dict[str, Any]],
    *,
    access_token: str | None = None,
    use_service_role: bool = False,
    on_conflict: str | None = None,
) -> list[dict[str, Any]]:
    params = {"on_conflict": on_conflict} if on_conflict else None
    rows = _request(
        "POST",
        f"/rest/v1/{table}",
        api_key=settings.supabase_secret_key if use_service_role else settings.supabase_publishable_key,
        access_token=access_token if not use_service_role else None,
        params=params,
        json_body=payload,
        prefer="resolution=merge-duplicates,return=representation" if on_conflict else "return=representation",
    )
    return rows if isinstance(rows, list) else []


def rest_update(
    table: str,
    payload: dict[str, Any],
    *,
    filters: dict[str, str],
    access_token: str | None = None,
    use_service_role: bool = False,
) -> list[dict[str, Any]]:
    rows = _request(
        "PATCH",
        f"/rest/v1/{table}",
        api_key=settings.supabase_secret_key if use_service_role else settings.supabase_publishable_key,
        access_token=access_token if not use_service_role else None,
        params=filters,
        json_body=payload,
        prefer="return=representation",
    )
    return rows if isinstance(rows, list) else []


def rest_delete(
    table: str,
    *,
    filters: dict[str, str],
    access_token: str | None = None,
    use_service_role: bool = False,
) -> list[dict[str, Any]]:
    rows = _request(
        "DELETE",
        f"/rest/v1/{table}",
        api_key=settings.supabase_secret_key if use_service_role else settings.supabase_publishable_key,
        access_token=access_token if not use_service_role else None,
        params=filters,
        prefer="return=representation",
    )
    return rows if isinstance(rows, list) else []


def ensure_profile_for_user(access_token: str, user: SupabaseUser) -> dict[str, Any]:
    existing = rest_select(
        "profiles",
        filters={"id": f"eq.{user.id}"},
        access_token=access_token,
        single=True,
    )
    display_name = user.display_name
    avatar_url = user.avatar_url
    email = user.email
    if not existing:
        rows = rest_insert(
            "profiles",
            {
                "id": user.id,
                "email": email,
                "display_name": display_name,
                "avatar_url": avatar_url,
            },
            access_token=access_token,
        )
        return rows[0] if rows else {}

    patch: dict[str, Any] = {}
    if email and not existing.get("email"):
        patch["email"] = email
    if display_name and not existing.get("display_name"):
        patch["display_name"] = display_name
    if avatar_url and not existing.get("avatar_url"):
        patch["avatar_url"] = avatar_url
    if patch:
        rows = rest_update(
            "profiles",
            patch,
            filters={"id": f"eq.{user.id}"},
            access_token=access_token,
        )
        return rows[0] if rows else existing
    return existing


def is_admin_profile(profile: dict[str, Any] | None) -> bool:
    return bool(profile and str(profile.get("role") or "").strip().lower() == "admin")


def current_profile(access_token: str, user_id: str) -> dict[str, Any] | None:
    row = rest_select(
        "profiles",
        filters={"id": f"eq.{user_id}"},
        access_token=access_token,
        single=True,
    )
    return row if isinstance(row, dict) else None


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", value.strip().lower())
    return text.strip("-")


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def quote_filter(value: str) -> str:
    return quote(value, safe="")
