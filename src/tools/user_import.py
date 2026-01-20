import json
import random
import string
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import requests
import gitlab
import gitlab.v4.objects

from pyforgejo import AuthenticatedClient
from pyforgejo.api.admin import admin_create_public_key, admin_create_user
from pyforgejo.api.user import user_get, user_list_keys
from pyforgejo.models.create_key_option import CreateKeyOption
from pyforgejo.models.create_user_option import CreateUserOption

from .fg_migration import fg_print

@lru_cache(maxsize=10000)
def gitlab_email_for_user_id(gitlab_api: gitlab.Gitlab, user_id: int) -> Optional[str]:
    try:
        u = gitlab_api.users.get(user_id)
    except Exception:
        return None

    for attr in ("email", "public_email"):
        v = getattr(u, attr, None)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


@lru_cache(maxsize=10000)
def gitlab_email_for_username(gitlab_api: gitlab.Gitlab, username: str) -> Optional[str]:
    username = (username or "").strip()
    if not username:
        return None

    try:
        users = gitlab_api.users.list(username=username)
        if users:
            uid = getattr(users[0], "id", None)
            if isinstance(uid, int):
                return gitlab_email_for_user_id(gitlab_api, uid)
    except Exception:
        pass

    try:
        users = gitlab_api.users.list(search=username)
        for u in users or []:
            if (getattr(u, "username", "") or "").strip() == username:
                uid = getattr(u, "id", None)
                if isinstance(uid, int):
                    return gitlab_email_for_user_id(gitlab_api, uid)
    except Exception:
        pass

    return None

def get_user_keys(fg_client: AuthenticatedClient, username: str) -> List[Dict]:
    key_response: requests.Response = user_list_keys.sync_detailed(username, client=fg_client)
    if key_response.status_code.name == "OK":
        return json.loads(key_response.content)
    status_code = key_response.status_code.name
    fg_print.error(
        f"Failed to load user keys for user {username}! {status_code}",
        f"failed to load user keys for user {username}",
    )
    return []


def user_key_exists(fg_client: AuthenticatedClient, username: str, keyname: str) -> bool:
    existing_keys = get_user_keys(fg_client, username)
    if existing_keys:
        existing_key = next((item for item in existing_keys if item.get("title") == keyname), None)
        if existing_key is not None:
            fg_print.warning(f"Public key {keyname} already exists for user {username}, skipping!")
            return True
        print(f"Public key {keyname} does not exist for user {username}, importing!")
        return False
    print(f"No public keys for user {username}, importing!")
    return False


def _mk_tmp_password() -> str:
    rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
    return f"Tmp1!{rnd_str}"


def ensure_user_exists(
        fg_client: AuthenticatedClient,
        username: str,
        *,
        full_name: Optional[str] = None,
        email: Optional[str] = None,
        notify: bool = False,
        reason: str = "",
) -> Tuple[Optional[Dict], Optional[str]]:
    username = (username or "").strip()
    if not username:
        return None, None

    resp = user_get.sync_detailed(username, client=fg_client)
    if resp.status_code.name == "OK":
        try:
            return json.loads(resp.content), None
        except Exception:
            return None, None

    tmp_password = _mk_tmp_password()
    tmp_email = (email or "").strip() or f"{username}@noemail-git.local"
    tmp_full_name = (full_name or "").strip() or username

    body = CreateUserOption(
        email=tmp_email,
        full_name=tmp_full_name,
        login_name=username,
        password=tmp_password,
        send_notify=notify,
        source_id=0,
        username=username,
    )

    import_response = admin_create_user.sync_detailed(body=body, client=fg_client)
    if import_response.status_code.name == "CREATED":
        suffix = f" ({reason})" if reason else ""
        fg_print.info(f"User {username} created{suffix}, temporary password: {tmp_password}")
        resp2 = user_get.sync_detailed(username, client=fg_client)
        if resp2.status_code.name == "OK":
            try:
                return json.loads(resp2.content), tmp_password
            except Exception:
                return None, tmp_password
        return None, tmp_password

    msg = ""
    try:
        msg = json.loads(import_response.content).get("message") or ""
    except Exception:
        msg = import_response.text or ""

    fg_print.error(f"Failed to create user {username}: {msg}", f"failed to create user {username}")
    return None, None


def ensure_importer_user(fg_client: AuthenticatedClient, *, notify: bool = False) -> Tuple[Optional[Dict], Optional[str]]:
    return ensure_user_exists(
        fg_client,
        "forgejo-importer",
        full_name="forgejo-importer",
        email="forgejo-importer@noemail-git.local",
        notify=notify,
        reason="needed for issue fallback author",
    )


def import_user_keys(
        fg_client: AuthenticatedClient,
        keys: List[gitlab.v4.objects.UserKey],
        username: str,
) -> None:
    for key in keys:
        if not user_key_exists(fg_client, username, key.title):
            import_response: requests.Response = admin_create_public_key.sync_detailed(
                username=username,
                body=CreateKeyOption(
                    key=key.key,
                    read_only=True,
                    title=key.title,
                ),
                client=fg_client,
            )
            if import_response.status_code.name == "CREATED":
                fg_print.info(f"Public key {key.title} imported!")
            else:
                msg = ""
                try:
                    msg = json.loads(import_response.content).get("message") or ""
                except Exception:
                    msg = import_response.text or ""
                fg_print.error(
                    f"Public key {key.title} import failed: {msg}",
                    f"failed to import key {key.title} for user {username}",
                )


def import_one_gitlab_user(
        gitlab_api: gitlab.Gitlab,
        fg_client: AuthenticatedClient,
        user: gitlab.v4.objects.User,
        *,
        notify: bool = False,
) -> None:
    username = (getattr(user, "username", "") or "").strip()
    if not username:
        return

    print(f"Importing user {username}...")

    try:
        user_full = gitlab_api.users.get(user.id)
    except Exception as e:
        fg_print.error(
            f"Failed to fetch full user {username} ({getattr(user, 'id', None)}): {e}",
            f"failed to fetch full user {username}",
        )
        user_full = user

    tmp_email = f"{username}@noemail-git.local"
    if hasattr(user_full, "email") and isinstance(getattr(user_full, "email"), str) and user_full.email.strip():
        tmp_email = user_full.email.strip()

    full_name = getattr(user_full, "name", None)
    ensure_user_exists(
        fg_client,
        username,
        full_name=full_name or username,
        email=tmp_email,
        notify=notify,
        reason="import from gitlab",
    )

    try:
        keys = user_full.keys.list(all=True)
    except Exception as e:
        fg_print.error(
            f"Failed to load keys for user {username}: {e}",
            f"failed to load keys for user {username}",
        )
        keys = []

    import_user_keys(fg_client, keys, username)
