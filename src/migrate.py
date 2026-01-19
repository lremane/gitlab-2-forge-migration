"""
Usage: migrate.py [--users] [--groups] [--projects] [--all] [--notify] [--migrate-from-csv=<csv>]
       migrate.py --help

Migration script to import projects, users, groups, from Gitlab to Forgejo.

Options
  -h, --help                 Show this screen
  --users                    migrate users
  --groups                   migrate groups
  --projects                 migrate projects
  --all                      migrate all
  --notify                   send notification to users
  --migrate-from-csv=<csv>   only migrate projects listed in CSV (uses only the url column)
"""

import os
import json
import re
import random
import string
import configparser
from typing import Dict
from typing import List
from typing import Optional

from docopt import docopt
import requests
import dateutil.parser

import gitlab  # pip install python-gitlab
import gitlab.v4.objects

from pyforgejo import AuthenticatedClient
from pyforgejo.api.admin import admin_create_public_key, admin_create_user
from pyforgejo.api.miscellaneous import get_version
from pyforgejo.api.organization import org_create, org_get
from pyforgejo.api.repository import repo_get
from pyforgejo.api.user import user_get, user_list_keys
from pyforgejo.models.create_key_option import CreateKeyOption
from pyforgejo.models.create_org_option import CreateOrgOption
from pyforgejo.models.create_user_option import CreateUserOption

from fg_migration import fg_print
from forgejo_http import ForgejoHttp
from tools.csv_input_reader import InputCsvReader
from migrate_organizations import import_groups

SCRIPT_VERSION = "0.9"

#######################
# CONFIG SECTION START
#######################
if not os.path.exists("../.migrate.ini"):
    print("Please create .migrate.ini as explained in the README!")
    os.sys.exit()

config = configparser.RawConfigParser()
config.read("../.migrate.ini")
GITLAB_URL = config.get("migrate", "gitlab_url")
GITLAB_TOKEN = config.get("migrate", "gitlab_token")
GITLAB_ADMIN_USER = config.get("migrate", "gitlab_admin_user")
GITLAB_ADMIN_PASS = config.get("migrate", "gitlab_admin_pass")
FORGEJO_URL = config.get("migrate", "forgejo_url")
FORGEJO_API_URL = f"{FORGEJO_URL}/api/v1"
FORGEJO_TOKEN = config.get("migrate", "forgejo_token")
FORGEJO_USER = config.get("migrate", "forgejo_admin_user")
FORGEJO_PASSWORD = config.get("migrate", "forgejo_admin_pass")
#######################
# CONFIG SECTION END
#######################


def main():
    _args = docopt(__doc__)
    args = {k.replace("--", ""): v for k, v in _args.items()}

    fg_print.print_color(
        fg_print.Bcolors.HEADER, "---=== Gitlab to Forgejo migration ===---"
    )
    print(f"Version: {SCRIPT_VERSION}")
    print()

    gl = gitlab.Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
    gl.auth()
    assert isinstance(gl.user, gitlab.v4.objects.CurrentUser)
    fg_print.info(f"Connected to Gitlab, version: {gl.version()[0]}")

    fg_client = AuthenticatedClient(base_url=FORGEJO_API_URL, token=FORGEJO_TOKEN)
    fg_ver = json.loads(get_version.sync_detailed(client=fg_client).content)["version"]
    fg_print.info(f"Connected to Forgejo, version: {fg_ver}")

    fg_http = ForgejoHttp(FORGEJO_API_URL, FORGEJO_TOKEN)

    try:
        if args["users"] or args["all"]:
            import_users(gl, fg_client, notify=bool(args["notify"]))
        if args["groups"] or args["all"]:
            import_groups(gl, fg_client, fg_http)
        if args["projects"] or args["all"]:
            import_projects(gl, fg_client, fg_http, csv_path=args.get("migrate-from-csv"))

        if (
                not args["users"]
                and not args["groups"]
                and not args["projects"]
                and not args["all"]
        ):
            print()
            fg_print.warning("No migration option(s) selected, nothing to do!")
            os.sys.exit()

        print()
        if fg_print.GLOBAL_ERROR_COUNT == 0:
            fg_print.success("Migration finished with no errors!")
        else:
            fg_print.error(f"Migration finished with {fg_print.GLOBAL_ERROR_COUNT} errors!")
            print("Failed elements:")
            print(*fg_print.GLOBAL_ERROR_LIST, sep="\n")
    finally:
        fg_http.close()


def _forgejo_owner_name(owner_obj: Dict) -> str:
    for k in ("username", "login", "name"):
        v = owner_obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    raise ValueError("owner object missing username/login/name")


def get_labels(fg_http: ForgejoHttp, owner: str, repo: str) -> List:
    existing_labels = []
    label_response: requests.Response = fg_http.get(f"/repos/{owner}/{repo}/labels")
    if label_response.ok:
        existing_labels = label_response.json()
    else:
        fg_print.error(
            f"Failed to load existing labels for project {repo}! {label_response.text}"
        )
    return existing_labels


def get_milestones(fg_http: ForgejoHttp, owner: str, repo: str) -> List:
    existing_milestones = []
    milestone_response: requests.Response = fg_http.get(
        f"/repos/{owner}/{repo}/milestones"
    )
    if milestone_response.ok:
        existing_milestones = milestone_response.json()
    else:
        fg_print.error(
            f"Failed to load existing milestones for project {repo}! {milestone_response.text}"
        )
    return existing_milestones


def get_issues(fg_http: ForgejoHttp, owner: str, repo: str) -> List:
    existing_issues = []
    issue_response: requests.Response = fg_http.get(
        f"/repos/{owner}/{repo}/issues", params={"state": "all", "page": -1}
    )
    if issue_response.ok:
        existing_issues = issue_response.json()
    else:
        fg_print.error(
            f"Failed to load existing issues for project {repo}! {issue_response.text}"
        )
    return existing_issues


def get_user_keys(fg_client: AuthenticatedClient, username: str) -> Dict:
    key_response: requests.Response = user_list_keys.sync_detailed(
        username, client=fg_client
    )
    if key_response.status_code.name == "OK":
        return json.loads(key_response.content)

    status_code = key_response.status_code.name
    fg_print.error(
        f"Failed to load user keys for user {username}! {status_code}",
        f"failed to load user keys for user {username}",
    )
    return []


def user_exists(fg_client: AuthenticatedClient, username: str) -> bool:
    user_response: requests.Response = user_get.sync_detailed(username, client=fg_client)
    if user_response.status_code.name == "OK":
        fg_print.warning(f"User {username} already exists in Forgejo, skipping!")
        return True
    print(f"User {username} not found in Forgejo, importing!")
    return False


def user_key_exists(fg_client: AuthenticatedClient, username: str, keyname: str) -> bool:
    existing_keys = get_user_keys(fg_client, username)
    if existing_keys:
        existing_key = next(
            (item for item in existing_keys if item.get("title") == keyname), None
        )
        if existing_key is not None:
            fg_print.warning(
                f"Public key {keyname} already exists for user {username}, skipping!"
            )
            return True
        print(f"Public key {keyname} does not exist for user {username}, importing!")
        return False
    print(f"No public keys for user {username}, importing!")
    return False


def collaborator_exists(
        fg_http: ForgejoHttp, owner: str, repo: str, username: str
) -> bool:
    collaborator_response: requests.Response = fg_http.get(
        f"/repos/{owner}/{repo}/collaborators/{username}"
    )
    if collaborator_response.ok:
        fg_print.warning(
            f"Collaborator {username} already exists in Forgejo, skipping!"
        )
        return True
    print(f"Collaborator {username} not found in Forgejo, importing!")
    return False


def repo_exists(fg_client: AuthenticatedClient, owner: str, repo: str) -> bool:
    repo_response: requests.Response = repo_get.sync_detailed(
        owner=owner, repo=repo, client=fg_client
    )
    if repo_response.status_code.name == "OK":
        fg_print.warning(f"Project {repo} already exists in Forgejo, skipping!")
        return True
    print(f"Project {repo} not found in Forgejo, importing!")
    return False


def label_exists(
        fg_http: ForgejoHttp, owner: str, repo: str, labelname: str
) -> bool:
    existing_labels = get_labels(fg_http, owner, repo)
    if existing_labels:
        existing_label = next(
            (item for item in existing_labels if item.get("name") == labelname), None
        )
        if existing_label is not None:
            fg_print.warning(
                f"Label {labelname} already exists in project {repo}, skipping!"
            )
            return True
        print(f"Label {labelname} does not exist in project {repo}, importing!")
        return False
    print(f"No labels in project {repo}, importing!")
    return False


def milestone_exists(
        fg_http: ForgejoHttp, owner: str, repo: str, milestone: str
) -> bool:
    existing_milestones = get_milestones(fg_http, owner, repo)
    if existing_milestones:
        existing_milestone = next(
            (item for item in existing_milestones if item.get("title") == milestone),
            None,
        )
        if existing_milestone is not None:
            fg_print.warning(
                f"Milestone {milestone} already exists in project {repo}, skipping!"
            )
            return True
        print(f"Milestone {milestone} does not exist in project {repo}, importing!")
        return False
    print(f"No milestones in project {repo}, importing!")
    return False


def issue_exists(fg_http: ForgejoHttp, owner: str, repo: str, issue: str) -> bool:
    existing_issues = get_issues(fg_http, owner, repo)
    if existing_issues:
        existing_issue = next(
            (item for item in existing_issues if item.get("title") == issue), None
        )
        if existing_issue is not None:
            fg_print.warning(
                f"Issue {issue} already exists in project {repo}, skipping!"
            )
            return True
        print(f"Issue {issue} does not exist in project {repo}, importing!")
        return False
    print(f"No issues in project {repo}, importing!")
    return False


def _ensure_owner_exists(
        fg_client: AuthenticatedClient, project: gitlab.v4.objects.Project
) -> Optional[Dict]:
    ns = project.namespace or {}
    ns_path = ns.get("path") or ns.get("name") or ""
    ns_name = ns.get("name") or ns_path
    ns_kind = (ns.get("kind") or "").lower()

    if not ns_path:
        return None

    resp = user_get.sync_detailed(ns_path, client=fg_client)
    if resp.status_code.name == "OK":
        return json.loads(resp.content)

    org_candidate = name_clean(ns_path)
    resp = org_get.sync_detailed(org_candidate, client=fg_client)
    if resp.status_code.name == "OK":
        return json.loads(resp.content)

    if ns_kind == "group":
        import_response = org_create.sync_detailed(
            body=CreateOrgOption(
                description="",
                full_name=ns_name,
                location="",
                username=name_clean(ns_path),
                website="",
            ),
            client=fg_client,
        )
        if import_response.status_code.name == "CREATED":
            fg_print.info(f"Group {name_clean(ns_path)} created (needed for project import)!")
            resp = org_get.sync_detailed(name_clean(ns_path), client=fg_client)
            if resp.status_code.name == "OK":
                return json.loads(resp.content)
        msg = json.loads(import_response.content).get("message")
        fg_print.error(f"Failed to create group {name_clean(ns_path)}: {msg}", f"failed to create group {name_clean(ns_path)}")
        return None

    rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
    tmp_password = f"Tmp1!{rnd_str}"
    tmp_email = f"{ns_path}@noemail-git.local"
    body = CreateUserOption(
        email=tmp_email,
        full_name=ns_name,
        login_name=ns_path,
        password=tmp_password,
        send_notify=False,
        source_id=0,
        username=ns_path,
    )
    import_response = admin_create_user.sync_detailed(body=body, client=fg_client)
    if import_response.status_code.name == "CREATED":
        fg_print.info(f"User {ns_path} created (needed for project import), temporary password: {tmp_password}")
        resp = user_get.sync_detailed(ns_path, client=fg_client)
        if resp.status_code.name == "OK":
            return json.loads(resp.content)
    msg = json.loads(import_response.content).get("message")
    fg_print.error(f"Failed to create user {ns_path}: {msg}", f"failed to create user {ns_path}")
    return None


def _ensure_user_exists_by_username(
        fg_client: AuthenticatedClient, username: str
) -> Optional[Dict]:
    if not username:
        return None

    resp = user_get.sync_detailed(username, client=fg_client)
    if resp.status_code.name == "OK":
        return json.loads(resp.content)

    rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
    tmp_password = f"Tmp1!{rnd_str}"
    tmp_email = f"{username}@noemail-git.local"
    body = CreateUserOption(
        email=tmp_email,
        full_name=username,
        login_name=username,
        password=tmp_password,
        send_notify=False,
        source_id=0,
        username=username,
    )
    import_response = admin_create_user.sync_detailed(body=body, client=fg_client)
    if import_response.status_code.name == "CREATED":
        fg_print.info(f"User {username} created (needed for collaborator import), temporary password: {tmp_password}")
        resp = user_get.sync_detailed(username, client=fg_client)
        if resp.status_code.name == "OK":
            return json.loads(resp.content)

    msg = json.loads(import_response.content).get("message")
    fg_print.error(f"Failed to create user {username}: {msg}", f"failed to create user {username}")
    return None

def _ensure_collaborator_with_permission(
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        owner: str,
        repo: str,
        username: str,
        permission: str = "read",
) -> None:
    if not username:
        return

    _ensure_user_exists_by_username(fg_client, username)

    if collaborator_exists(fg_http, owner, repo, username):
        return

    import_response: requests.Response = fg_http.put(
        f"/repos/{owner}/{repo}/collaborators/{username}",
        json={"permission": permission},
    )
    if import_response.ok:
        fg_print.info(f"Collaborator {username} added to {owner}/{repo} (needed for issue author/assignee)!")
    else:
        fg_print.error(
            f"Failed to add collaborator {username} to {owner}/{repo}: {import_response.status_code} {import_response.text}",
            f"failed to add collaborator {username} to {owner}/{repo}",
        )

def get_user_or_group(fg_client: AuthenticatedClient, project: gitlab.v4.objects.Project) -> Optional[Dict]:
    owner = _ensure_owner_exists(fg_client, project)
    if owner is None:
        ns = project.namespace or {}
        ns_path = ns.get("path") or ns.get("name") or ""
        ns_name = ns.get("name") or ns_path
        fg_print.error(
            f"Failed to load or create user/org for namespace {ns_name} ({ns_path})",
            f"failed to load or create user/org {ns_name}",
        )
    return owner


def _import_project_labels(
        fg_http: ForgejoHttp,
        labels: List[gitlab.v4.objects.ProjectLabel],
        owner: str,
        repo: str,
):
    for label in labels:
        if not label_exists(fg_http, owner, repo, label.name):
            import_response: requests.Response = fg_http.post(
                f"/repos/{owner}/{repo}/labels",
                json={
                    "name": label.name,
                    "color": label.color,
                    "description": label.description,
                },
            )
            if import_response.ok:
                fg_print.info(f"Label {label.name} imported!")
            else:
                fg_print.error(
                    f"Label {label.name} import failed: {import_response.text}"
                )


def _import_project_milestones(
        fg_http: ForgejoHttp,
        milestones: List[gitlab.v4.objects.ProjectMilestone],
        owner: str,
        repo: str,
):
    for milestone in milestones:
        if not milestone_exists(fg_http, owner, repo, milestone.title):
            due_date = None
            if milestone.due_date is not None and milestone.due_date != "":
                due_date = dateutil.parser.parse(milestone.due_date).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )

            import_response: requests.Response = fg_http.post(
                f"/repos/{owner}/{repo}/milestones",
                json={
                    "description": milestone.description,
                    "due_on": due_date,
                    "title": milestone.title,
                },
            )
            if import_response.ok:
                fg_print.info(f"Milestone {milestone.title} imported!")
                existing_milestone = import_response.json()

                if existing_milestone:
                    update_response: requests.Response = fg_http.patch(
                        f"/repos/{owner}/{repo}/milestones/{existing_milestone['id']}",
                        json={
                            "description": milestone.description,
                            "due_on": due_date,
                            "title": milestone.title,
                            "state": milestone.state,
                        },
                    )
                    if update_response.ok:
                        fg_print.info(f"Milestone {milestone.title} updated!")
                    else:
                        fg_print.error(
                            f"Milestone {milestone.title} update failed: {update_response.text}"
                        )
            else:
                fg_print.error(
                    f"Milestone {milestone.title} import failed: {import_response.text}"
                )


def _import_project_issues(
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        issues: List[gitlab.v4.objects.ProjectIssue],
        owner: str,
        repo: str,
):
    existing_milestones = get_milestones(fg_http, owner, repo)
    existing_labels = get_labels(fg_http, owner, repo)

    for issue in issues:
        if issue_exists(fg_http, owner, repo, issue.title):
            continue

        author_username = None
        try:
            if getattr(issue, "author", None) and isinstance(issue.author, dict):
                author_username = (issue.author.get("username") or "").strip() or None
        except Exception:
            author_username = None

        if not author_username:
            author_username = "forgejo-importer"

        _ensure_user_exists_by_username(fg_client, author_username)
        _ensure_collaborator_with_permission(fg_client, fg_http, owner, repo, author_username, permission="read")

        due_date = ""
        if issue.due_date is not None:
            due_date = dateutil.parser.parse(issue.due_date).strftime("%Y-%m-%dT%H:%M:%SZ")

        assignee = None
        if issue.assignee is not None and isinstance(issue.assignee, dict):
            assignee = (issue.assignee.get("username") or "").strip() or None

        assignees: List[str] = []
        try:
            for tmp_assignee in getattr(issue, "assignees", []) or []:
                if isinstance(tmp_assignee, dict):
                    u = (tmp_assignee.get("username") or "").strip()
                    if u:
                        assignees.append(u)
        except Exception:
            assignees = []

        if assignee:
            _ensure_user_exists_by_username(fg_client, assignee)
            _ensure_collaborator_with_permission(fg_client, fg_http, owner, repo, assignee, permission="read")

        for u in assignees:
            _ensure_user_exists_by_username(fg_client, u)
            _ensure_collaborator_with_permission(fg_client, fg_http, owner, repo, u, permission="read")

        milestone = None
        if issue.milestone is not None and isinstance(issue.milestone, dict):
            existing_milestone = next(
                (item for item in existing_milestones if item.get("title") == issue.milestone.get("title")),
                None,
            )
            if existing_milestone:
                milestone = existing_milestone.get("id")

        label_ids: List[int] = []
        try:
            for label in getattr(issue, "labels", []) or []:
                existing_label = next((item for item in existing_labels if item.get("name") == label), None)
                if existing_label and existing_label.get("id") is not None:
                    label_ids.append(existing_label["id"])
        except Exception:
            label_ids = []

        payload = {
            "assignee": assignee,
            "assignees": assignees,
            "body": issue.description,
            "closed": issue.state == "closed",
            "due_on": due_date,
            "labels": label_ids,
            "milestone": milestone,
            "title": issue.title,
        }

        import_response: requests.Response = fg_http.post(
            f"/repos/{owner}/{repo}/issues",
            json=payload,
            sudo=author_username,
        )

        if import_response.ok:
            fg_print.info(f"Issue {issue.title} imported as {author_username}!")
            continue

        txt = import_response.text or ""
        if "Assignee does not exist" in txt or "assignee" in txt.lower():
            payload_fallback = dict(payload)
            payload_fallback["assignee"] = None
            payload_fallback["assignees"] = []

            import_response_2: requests.Response = fg_http.post(
                f"/repos/{owner}/{repo}/issues",
                json=payload_fallback,
                sudo=author_username,
            )
            if import_response_2.ok:
                fg_print.warning(f"Issue {issue.title} imported as {author_username}, but assignees were dropped due to Forgejo validation.")
            else:
                fg_print.error(f"Issue {issue.title} import failed: {import_response_2.text}", f"failed to import issue {issue.title}")
            continue

        fg_print.error(f"Issue {issue.title} import failed: {txt}", f"failed to import issue {issue.title}")


def _import_project_repo(
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        project: gitlab.v4.objects.Project,
        owner_obj: Dict,
):
    forgejo_owner = _forgejo_owner_name(owner_obj)
    proj_name = name_clean(project.name)

    if repo_exists(fg_client, forgejo_owner, proj_name):
        return

    private = project.visibility in ("private", "internal")

    payload = {
        "service": "gitlab",
        "clone_addr": project.http_url_to_repo,
        "repo_name": proj_name,
        "description": project.description or "",
        "private": private,
        "uid": owner_obj["id"],
        "auth_token": GITLAB_TOKEN,
        "issues": True,
        "labels": True,
        "milestones": True,
        "pull_requests": True,
        "releases": True,
        "wiki": True,
        "mirror": False,
    }

    timeout_seconds = 1800
    attempts = 3
    last_err: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            resp = fg_http.post("/repos/migrate", json=payload, timeout=timeout_seconds)
            if resp.ok:
                fg_print.info(f"Project {proj_name} imported (GitLab importer)!")
                return

            fg_print.error(
                f"Project {proj_name} import failed: {resp.status_code} {resp.text}",
                f"project {proj_name} import failed",
            )
            return

        except requests.exceptions.ReadTimeout as e:
            last_err = e

            try:
                repo_response: requests.Response = repo_get.sync_detailed(
                    owner=forgejo_owner, repo=proj_name, client=fg_client
                )
                if repo_response.status_code.name == "OK":
                    fg_print.warning(
                        f"Project {proj_name} migrate request timed out, but repo now exists in Forgejo (migration likely finished)."
                    )
                    return
            except Exception:
                pass

            if attempt < attempts:
                backoff_seconds = 5 * attempt
                fg_print.warning(
                    f"Project {proj_name} migrate request timed out (attempt {attempt}/{attempts}); retrying after {backoff_seconds}s."
                )
                import time
                time.sleep(backoff_seconds)
                continue

        except requests.exceptions.RequestException as e:
            last_err = e
            fg_print.error(
                f"Project {proj_name} import request failed: {type(e).__name__}: {e}",
                f"project {proj_name} import request failed",
            )
            return

    fg_print.error(
        f"Project {proj_name} import failed after {attempts} attempts due to timeouts: {last_err}",
        f"project {proj_name} import timed out",
    )


def _import_project_repo_collaborators(
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        forgejo_owner: str,
        forgejo_repo: str,
        collaborators: List[gitlab.v4.objects.ProjectMember],
):
    for collaborator in collaborators:
        if not collaborator.username:
            continue

        _ensure_user_exists_by_username(fg_client, collaborator.username)

        if not collaborator_exists(
                fg_http, forgejo_owner, forgejo_repo, collaborator.username
        ):
            permission = "read"
            if collaborator.access_level in (10, 20):
                permission = "read"
            elif collaborator.access_level == 30:
                permission = "write"
            elif collaborator.access_level in (40, 50):
                permission = "admin"
            else:
                fg_print.warning(
                    f"Unsupported access level {collaborator.access_level}, setting permissions to 'read'!"
                )

            import_response: requests.Response = fg_http.put(
                f"/repos/{forgejo_owner}/{forgejo_repo}/collaborators/{collaborator.username}",
                json={"permission": permission},
            )
            if import_response.ok:
                fg_print.info(f"Collaborator {collaborator.username} imported!")
            else:
                fg_print.error(
                    f"Collaborator {collaborator.username} import failed: {import_response.text}",
                    f"failed to import collaborator {collaborator.username} for {forgejo_owner}/{forgejo_repo}",
                )


def _import_users(
        fg_client: AuthenticatedClient, users: List[gitlab.v4.objects.User], notify: bool = False
):
    if not user_exists(fg_client, "forgejo-importer"):
        rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
        tmp_password = f"Tmp1!{rnd_str}"
        body = CreateUserOption(
            email="forgejo-importer@noemail-git.local",
            full_name="forgejo-importer",
            login_name="forgejo-importer",
            password=tmp_password,
            send_notify=False,
            source_id=0,
            username="forgejo-importer",
        )
        import_response: requests.Response = admin_create_user.sync_detailed(
            body=body, client=fg_client
        )
        if import_response.status_code.name == "CREATED":
            fg_print.info(f"User forgejo-importer imported, temporary password: {tmp_password}")
        else:
            msg = json.loads(import_response.content).get("message")
            fg_print.error(f"User forgejo-importer import failed: {msg}")

    for user in users:
        keys: List[gitlab.v4.objects.UserKey] = user.keys.list(all=True)

        print(f"Importing user {user.username}...")
        print(f"Found {len(keys)} public keys for user {user.username}")

        if not user_exists(fg_client, user.username):
            rnd_str = "".join(
                random.choices(string.ascii_uppercase + string.digits, k=10)
            )
            tmp_password = f"Tmp1!{rnd_str}"
            tmp_email = f"{user.username}@noemail-git.local"
            try:
                tmp_email = user.email
            except AttributeError:
                pass
            body = CreateUserOption(
                email=tmp_email,
                full_name=user.name,
                login_name=user.username,
                password=tmp_password,
                send_notify=notify,
                source_id=0,
                username=user.username,
            )
            import_response = admin_create_user.sync_detailed(body=body, client=fg_client)
            if import_response.status_code.name == "CREATED":
                fg_print.info(
                    f"User {user.username} imported, temporary password: {tmp_password}"
                )
            else:
                msg = json.loads(import_response.content).get("message")
                fg_print.error(
                    f"User {user.username} import failed: {msg}",
                    f"failed to import user {user.username}",
                )

        _import_user_keys(fg_client, keys, user)


def _import_user_keys(
        fg_client: AuthenticatedClient,
        keys: List[gitlab.v4.objects.UserKey],
        user: gitlab.v4.objects.User,
):
    for key in keys:
        if not user_key_exists(fg_client, user.username, key.title):
            import_response: requests.Response = admin_create_public_key.sync_detailed(
                username=user.username,
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
                msg = json.loads(import_response.content).get("message")
                fg_print.error(
                    f"Public key {key.title} import failed: {msg}",
                    f"failed to import key {key.title} for user {user.username}",
                )


def import_users(gitlab_api: gitlab.Gitlab, fg_client: AuthenticatedClient, notify=False):
    users_iter = gitlab_api.users.list(iterator=True, per_page=100)
    count = 0
    print(f"Loading users from GitLab as {gitlab_api.user.username}...")

    for user in users_iter:
        count += 1
        if count % 50 == 0:
            print(f"Fetched {count} users...")
        _import_users(fg_client, [user], notify)

    print(f"Done. Processed {count} users.")


def import_projects(
        gitlab_api: gitlab.Gitlab,
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        csv_path: str | None = None,
):
    if csv_path:
        eligible = _load_projects_from_csv(gitlab_api, csv_path)
    else:
        min_access_level = 30
        eligible = _load_eligible_membership_projects(gitlab_api, min_access_level)

    for idx, project in enumerate(eligible, start=1):
        _import_one_project_full(fg_client, fg_http, project, idx, len(eligible))


def _load_projects_from_csv(gitlab_api: gitlab.Gitlab, csv_path: str) -> List[gitlab.v4.objects.Project]:
    reader = InputCsvReader(GITLAB_URL)

    try:
        projects = reader.load_projects(
            csv_path=csv_path,
            get_project_by_full_path=gitlab_api.projects.get,
            warn=fg_print.warning,
        )
    except FileNotFoundError:
        fg_print.error(f"CSV file not found: {csv_path}", f"csv not found {csv_path}")
        return []
    except Exception as e:
        fg_print.error(f"Failed to read CSV {csv_path}: {e}", f"failed to read csv {csv_path}")
        return []

    print(f"Loaded {len(projects)} projects from CSV: {csv_path}", flush=True)
    return projects


def _load_eligible_membership_projects(
        gitlab_api: gitlab.Gitlab, min_access_level: int
) -> List[gitlab.v4.objects.Project]:
    print(f"Loading membership projects from GitLab as {gitlab_api.user.username}...", flush=True)

    projects_iter = gitlab_api.projects.list(
        iterator=True,
        per_page=100,
        membership=True,
        order_by="id",
        sort="asc",
    )

    eligible: List[gitlab.v4.objects.Project] = []
    fetched = 0
    checked = 0

    for project in projects_iter:
        fetched += 1
        if fetched % 25 == 0:
            print(f"Fetched {fetched} membership projects...", flush=True)

        full = _safe_get_full_project(gitlab_api, project.id)
        if full is None:
            continue

        checked += 1
        access = _extract_access_level(full)

        if access >= min_access_level:
            eligible.append(full)

        if checked % 25 == 0:
            print(
                f"Checked {checked} projects, eligible(write+) so far: {len(eligible)}",
                flush=True,
            )

    print(f"Done. Membership projects: {fetched}, eligible(write+): {len(eligible)}", flush=True)
    return eligible


def _safe_get_full_project(
        gitlab_api: gitlab.Gitlab, project_id: int
) -> gitlab.v4.objects.Project | None:
    try:
        return gitlab_api.projects.get(project_id)
    except Exception as e:
        fg_print.error(
            f"Failed to load project details for {project_id}: {e}",
            f"failed to load project details {project_id}",
        )
        return None


def _extract_access_level(project: gitlab.v4.objects.Project) -> int:
    perms = getattr(project, "permissions", None)
    if not isinstance(perms, dict):
        return 0

    p = perms.get("project_access") or {}
    g = perms.get("group_access") or {}
    pa = p.get("access_level") or 0
    ga = g.get("access_level") or 0

    try:
        return max(int(pa), int(ga))
    except Exception:
        return 0


def _import_one_project_full(
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        project: gitlab.v4.objects.Project,
        idx: int,
        total: int,
):
    gitlab_ns_display = project.namespace["name"]
    clean_repo = name_clean(project.name)

    print()
    fg_print.print_color(
        fg_print.Bcolors.HEADER,
        f"[{idx}/{total}] Project: {gitlab_ns_display}/{clean_repo}",
    )

    data = _load_project_gitlab_data(project, gitlab_ns_display, clean_repo)

    print(f"Found {len(data['collaborators'])} collaborators for project {clean_repo}", flush=True)
    print(f"Found {len(data['labels'])} labels for project {clean_repo}", flush=True)
    print(f"Found {len(data['milestones'])} milestones for project {clean_repo}", flush=True)
    print(f"Found {len(data['issues'])} issues for project {clean_repo}", flush=True)

    owner_obj = get_user_or_group(fg_client, project)
    if not owner_obj:
        fg_print.error(
            f"Failed to load project owner for project {clean_repo}",
            f"project {clean_repo} failed to load owner",
        )
        return

    forgejo_owner = _forgejo_owner_name(owner_obj)
    forgejo_repo = clean_repo

    _import_project_repo(fg_client, fg_http, project, owner_obj)

    _import_project_repo_collaborators(
        fg_client, fg_http, forgejo_owner, forgejo_repo, data["collaborators"]
    )
    _import_project_labels(fg_http, data["labels"], forgejo_owner, forgejo_repo)
    _import_project_milestones(fg_http, data["milestones"], forgejo_owner, forgejo_repo)
    _import_project_issues(fg_client, fg_http, data["issues"], forgejo_owner, forgejo_repo)


def _load_project_gitlab_data(
        project: gitlab.v4.objects.Project,
        proj_owner: str,
        clean_proj_name: str,
) -> Dict[str, list]:
    collaborators: List[gitlab.v4.objects.ProjectMember] = []
    labels: List[gitlab.v4.objects.ProjectLabel] = []
    milestones: List[gitlab.v4.objects.ProjectMilestone] = []
    issues: List[gitlab.v4.objects.ProjectIssue] = []

    try:
        collaborators = project.members.list(all=True)
    except Exception as e:
        fg_print.error(
            f"Failed to load collaborators for {proj_owner}/{clean_proj_name}: {e}",
            f"failed to load collaborators {proj_owner}/{clean_proj_name}",
        )

    try:
        labels = project.labels.list(all=True)
    except Exception as e:
        fg_print.error(
            f"Failed to load labels for {proj_owner}/{clean_proj_name}: {e}",
            f"failed to load labels {proj_owner}/{clean_proj_name}",
        )

    try:
        milestones = project.milestones.list(all=True)
    except Exception as e:
        fg_print.error(
            f"Failed to load milestones for {proj_owner}/{clean_proj_name}: {e}",
            f"failed to load milestones {proj_owner}/{clean_proj_name}",
        )

    try:
        issues = project.issues.list(all=True)
    except Exception as e:
        fg_print.error(
            f"Failed to load issues for {proj_owner}/{clean_proj_name}: {e}",
            f"failed to load issues {proj_owner}/{clean_proj_name}",
        )

    return {
        "collaborators": collaborators,
        "labels": labels,
        "milestones": milestones,
        "issues": issues,
    }


def name_clean(name):
    new_name = name.replace(" ", "_")
    new_name = re.sub(r"[^a-zA-Z0-9_\.-]", "-", new_name)

    if new_name.lower() == "plugins":
        return f"{new_name}-user"

    return new_name


if __name__ == "__main__":
    main()
