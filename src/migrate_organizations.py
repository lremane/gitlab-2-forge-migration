import json
import re
from typing import List, Optional

import requests
import gitlab
import gitlab.v4.objects

from pyforgejo import AuthenticatedClient
from pyforgejo.api.organization import org_create, org_get, org_list_teams
from pyforgejo.models.create_org_option import CreateOrgOption

from tools.fg_migration import fg_print
from forgejo_http import ForgejoHttp
from tools.user_import import ensure_user_exists, gitlab_email_for_user_id, gitlab_email_for_username


def name_clean(name: str) -> str:
    new_name = name.replace(" ", "_")
    new_name = re.sub(r"[^a-zA-Z0-9_\.-]", "-", new_name)
    if new_name.lower() == "plugins":
        return f"{new_name}-user"
    return new_name


def organization_exists(fg_client: AuthenticatedClient, orgname: str) -> bool:
    resp: requests.Response = org_get.sync_detailed(orgname, client=fg_client)
    if resp.status_code.name == "OK":
        fg_print.warning(f"Group {orgname} already exists in Forgejo, skipping!")
        return True
    print(f"Group {orgname} not found in Forgejo, importing!")
    return False


def get_teams(fg_client: AuthenticatedClient, orgname: str) -> List:
    resp: requests.Response = org_list_teams.sync_detailed(orgname, client=fg_client)
    if resp.status_code.name == "OK":
        return json.loads(resp.content)
    msg = json.loads(resp.content).get("errors")
    fg_print.error(f"Failed to load existing teams for organization {orgname}! {msg}")
    return []


def get_team_members(fg_http: ForgejoHttp, teamid: int) -> List:
    resp: requests.Response = fg_http.get(f"/teams/{teamid}/members", timeout=10)
    if resp.ok:
        return resp.json()
    fg_print.error(f"Failed to load existing members for team {teamid}! {resp.text}")
    return []


def member_exists(fg_http: ForgejoHttp, username: str, teamid: int) -> bool:
    existing_members = get_team_members(fg_http, teamid)
    existing_member = next((m for m in existing_members if m.get("username") == username), None)
    if existing_member:
        fg_print.warning(f"Member {username} is already in team {teamid}, skipping!")
        return True
    print(f"Member {username} is not in team {teamid}, importing!")
    return False


def _resolve_gitlab_member_email(gitlab_api: gitlab.Gitlab, member: object) -> Optional[str]:
    uid = getattr(member, "id", None)
    if isinstance(uid, int):
        em = gitlab_email_for_user_id(gitlab_api, uid)
        if em:
            return em.strip()

    username = (getattr(member, "username", "") or "").strip()
    if username:
        em = gitlab_email_for_username(gitlab_api, username)
        if em:
            return em.strip()

    return None


def _import_group_members(
        gitlab_api: gitlab.Gitlab,
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        members: List[gitlab.v4.objects.GroupMember],
        group: gitlab.v4.objects.Group,
):
    clean_group_name = name_clean(group.name)
    existing_teams = get_teams(fg_client, clean_group_name)
    if not existing_teams:
        fg_print.error(
            f"Failed to import members to group {clean_group_name}: no teams found!",
            f"failed to import members to group {clean_group_name}: no teams found",
        )
        return

    team_id = existing_teams[0]["id"]
    first_team_name = existing_teams[0]["name"]
    print(f"Organization teams fetched, importing users to first team: {first_team_name}")

    for member in members:
        username = (getattr(member, "username", "") or "").strip()
        if not username:
            continue

        email = _resolve_gitlab_member_email(gitlab_api, member) or f"{username}@noemail-git.local"
        u_obj, _ = ensure_user_exists(
            fg_client,
            username,
            full_name=username,
            email=email,
            notify=False,
            reason="needed for org membership import",
        )
        if not u_obj:
            continue

        if not member_exists(fg_http, username, team_id):
            resp: requests.Response = fg_http.put(
                f"/teams/{team_id}/members/{username}",
                timeout=10,
            )
            if resp.ok:
                fg_print.info(f"Member {username} added to group {clean_group_name}!")
            else:
                fg_print.error(
                    f"Failed to add member {username} to group {clean_group_name}! {resp.status_code} {resp.text}",
                    f"failed to add member {username} to group {clean_group_name}",
                )


def _import_groups(
        gitlab_api: gitlab.Gitlab,
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        groups: List[gitlab.v4.objects.Group],
):
    print(f"Found {len(groups)} gitlab groups")
    for group in groups:
        members: List[gitlab.v4.objects.GroupMember] = group.members.list(all=True)

        clean_group_name = name_clean(group.name)
        print(f"Importing group {clean_group_name}...")
        print(f"Found {len(members)} gitlab members for group {clean_group_name}")

        if not organization_exists(fg_client, clean_group_name):
            resp: requests.Response = org_create.sync_detailed(
                body=CreateOrgOption(
                    description=group.description,
                    full_name=group.full_name,
                    location="",
                    username=clean_group_name,
                    website="",
                ),
                client=fg_client,
            )
            if resp.status_code.name == "CREATED":
                fg_print.info(f"Group {clean_group_name} imported!")
            else:
                msg = json.loads(resp.content).get("message")
                fg_print.error(
                    f"Group {clean_group_name} import failed: {msg}",
                    f"failed to import group {clean_group_name}",
                )

        _import_group_members(gitlab_api, fg_client, fg_http, members, group)


def import_groups(gitlab_api: gitlab.Gitlab, fg_client: AuthenticatedClient, fg_http: ForgejoHttp):
    groups: List[gitlab.v4.objects.Group] = gitlab_api.groups.list(all=True)
    print(f"Found {len(groups)} gitlab groups as user {gitlab_api.user.username}")
    _import_groups(gitlab_api, fg_client, fg_http, groups)
