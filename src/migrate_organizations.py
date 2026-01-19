import json
import re
from typing import List

import requests
import gitlab
import gitlab.v4.objects

from pyforgejo import AuthenticatedClient
from pyforgejo.api.admin import admin_create_user
from pyforgejo.api.organization import org_create, org_get, org_list_teams
from pyforgejo.api.user import user_get
from pyforgejo.models.create_org_option import CreateOrgOption
from pyforgejo.models.create_user_option import CreateUserOption

from tools.fg_migration import fg_print
from forgejo_http import ForgejoHttp


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


def _ensure_user_exists_by_username(fg_client: AuthenticatedClient, username: str) -> bool:
    if not username:
        return False

    resp: requests.Response = user_get.sync_detailed(username, client=fg_client)
    if resp.status_code.name == "OK":
        return True

    tmp_email = f"{username}@noemail-git.local"
    body = CreateUserOption(
        email=tmp_email,
        full_name=username,
        login_name=username,
        password="Tmp1!ChangeMe12345",
        send_notify=False,
        source_id=0,
        username=username,
    )
    create_resp: requests.Response = admin_create_user.sync_detailed(body=body, client=fg_client)
    if create_resp.status_code.name == "CREATED":
        fg_print.info(f"User {username} created (needed for org membership import)")
        return True

    msg = json.loads(create_resp.content).get("message")
    fg_print.error(f"Failed to create user {username}: {msg}", f"failed to create user {username}")
    return False


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
        if not member.username:
            continue

        if not _ensure_user_exists_by_username(fg_client, member.username):
            continue

        if not member_exists(fg_http, member.username, team_id):
            resp: requests.Response = fg_http.put(
                f"/teams/{team_id}/members/{member.username}",
                timeout=10,
            )
            if resp.ok:
                fg_print.info(f"Member {member.username} added to group {clean_group_name}!")
            else:
                fg_print.error(
                    f"Failed to add member {member.username} to group {clean_group_name}! {resp.status_code} {resp.text}",
                    f"failed to add member {member.username} to group {clean_group_name}",
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
