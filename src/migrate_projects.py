import json
import random
import re
import string
from typing import Dict, List, Optional

import dateutil.parser
import gitlab
import gitlab.v4.objects
import requests
from pyforgejo import AuthenticatedClient
from pyforgejo.api.admin import admin_create_user
from pyforgejo.api.organization import org_create, org_get
from pyforgejo.api.repository import repo_get
from pyforgejo.api.user import user_get
from pyforgejo.models.create_org_option import CreateOrgOption
from pyforgejo.models.create_user_option import CreateUserOption

import tools.migration_config as cfg
from forgejo_http import ForgejoHttp
from tools.csv_input_reader import InputCsvReader
from tools.fg_migration import fg_print
from tools.user_import import (
    ensure_importer_user,
    ensure_user_exists,
    gitlab_email_for_user_id,
    gitlab_email_for_username,
)


def name_clean(name):
    new_name = name.replace(" ", "_")
    new_name = re.sub(r"[^a-zA-Z0-9_\.-]", "-", new_name)

    if new_name.lower() == "plugins":
        return f"{new_name}-user"

    return new_name


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


def collaborator_exists(
        fg_http: ForgejoHttp, owner: str, repo: str, username: str
) -> bool:
    collaborator_response: requests.Response = fg_http.get(
        f"/repos/{owner}/{repo}/collaborators/{username}"
    )
    if collaborator_response.ok:
        fg_print.warning(f"Collaborator {username} already exists in Forgejo, skipping!")
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


def label_exists(fg_http: ForgejoHttp, owner: str, repo: str, labelname: str) -> bool:
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
            fg_print.warning(f"Issue {issue} already exists in project {repo}, skipping!")
            return True
        print(f"Issue {issue} does not exist in project {repo}, importing!")
        return False
    print(f"No issues in project {repo}, importing!")
    return False


def _ensure_owner_exists(
        gitlab_api: gitlab.Gitlab,
        fg_client: AuthenticatedClient,
        project: gitlab.v4.objects.Project,
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
            fg_print.info(
                f"Group {name_clean(ns_path)} created (needed for project import)!"
            )
            resp = org_get.sync_detailed(name_clean(ns_path), client=fg_client)
            if resp.status_code.name == "OK":
                return json.loads(resp.content)
        msg = json.loads(import_response.content).get("message")
        fg_print.error(
            f"Failed to create group {name_clean(ns_path)}: {msg}",
            f"failed to create group {name_clean(ns_path)}",
        )
        return None

    rnd_str = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
    tmp_password = f"Tmp1!{rnd_str}"
    gl_email = gitlab_email_for_username(gitlab_api, ns_path)
    tmp_email = (gl_email or "").strip() or f"{ns_path}@noemail-git.local"

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
        fg_print.info(
            f"User {ns_path} created (needed for project import), temporary password: {tmp_password}"
        )
        resp = user_get.sync_detailed(ns_path, client=fg_client)
        if resp.status_code.name == "OK":
            return json.loads(resp.content)
    msg = json.loads(import_response.content).get("message")
    fg_print.error(
        f"Failed to create user {ns_path}: {msg}", f"failed to create user {ns_path}"
    )
    return None


def _ensure_collaborator_with_permission(
        gitlab_api: gitlab.Gitlab,
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        owner: str,
        repo: str,
        username: str,
        permission: str = "read",
) -> None:
    if not username:
        return

    gl_email = gitlab_email_for_username(gitlab_api, username)
    ensure_user_exists(
        fg_client,
        username,
        full_name=username,
        email=(gl_email or "").strip() or f"{username}@noemail-git.local",
        notify=False,
        reason="needed for collaborator import",
    )

    if collaborator_exists(fg_http, owner, repo, username):
        return

    import_response: requests.Response = fg_http.put(
        f"/repos/{owner}/{repo}/collaborators/{username}",
        json={"permission": permission},
        sudo=owner,
    )
    if import_response.ok:
        fg_print.info(
            f"Collaborator {username} added to {owner}/{repo} (needed for issue author/assignee)!"
        )
    else:
        fg_print.error(
            f"Failed to add collaborator {username} to {owner}/{repo}: {import_response.status_code} {import_response.text}",
            f"failed to add collaborator {username} to {owner}/{repo}",
        )


def get_user_or_group(
        gitlab_api: gitlab.Gitlab,
        fg_client: AuthenticatedClient,
        project: gitlab.v4.objects.Project,
) -> Optional[Dict]:
    owner = _ensure_owner_exists(gitlab_api, fg_client, project)
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
                sudo=owner,
            )
            if import_response.ok:
                fg_print.info(f"Label {label.name} imported!")
            else:
                fg_print.error(f"Label {label.name} import failed: {import_response.text}")


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
                sudo=owner,
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
                        sudo=owner,
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
        gitlab_api: gitlab.Gitlab,
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        issues: List[gitlab.v4.objects.ProjectIssue],
        owner: str,
        repo: str,
):
    existing_milestones = get_milestones(fg_http, owner, repo)
    existing_labels = get_labels(fg_http, owner, repo)

    ensure_importer_user(fg_client, notify=False)

    for issue in issues:
        if issue_exists(fg_http, owner, repo, issue.title):
            continue

        author_username = None
        author_id = None
        try:
            if getattr(issue, "author", None) and isinstance(issue.author, dict):
                author_username = (issue.author.get("username") or "").strip() or None
                author_id = issue.author.get("id")
        except Exception:
            author_username = None
            author_id = None

        if not author_username:
            author_username = "forgejo-importer"

        author_email = None
        if author_username != "forgejo-importer":
            if isinstance(author_id, int):
                author_email = gitlab_email_for_user_id(gitlab_api, author_id)
            if not author_email:
                author_email = gitlab_email_for_username(gitlab_api, author_username)

        ensure_user_exists(
            fg_client,
            author_username,
            full_name=author_username,
            email=(author_email or "").strip()
                  or f"{author_username}@noemail-git.local",
            notify=False,
            reason="needed for issue author",
        )
        _ensure_collaborator_with_permission(
            gitlab_api,
            fg_client,
            fg_http,
            owner,
            repo,
            author_username,
            permission="read",
        )

        due_date = ""
        if issue.due_date is not None:
            due_date = dateutil.parser.parse(issue.due_date).strftime("%Y-%m-%dT%H:%M:%SZ")

        assignee = None
        assignee_id = None
        if issue.assignee is not None and isinstance(issue.assignee, dict):
            assignee = (issue.assignee.get("username") or "").strip() or None
            assignee_id = issue.assignee.get("id")

        assignees: List[str] = []
        assignees_ids: Dict[str, Optional[int]] = {}
        try:
            for tmp_assignee in getattr(issue, "assignees", []) or []:
                if isinstance(tmp_assignee, dict):
                    u = (tmp_assignee.get("username") or "").strip()
                    if u:
                        assignees.append(u)
                        tid = tmp_assignee.get("id")
                        assignees_ids[u] = tid if isinstance(tid, int) else None
        except Exception:
            assignees = []
            assignees_ids = {}

        if assignee:
            assignee_email = None
            if isinstance(assignee_id, int):
                assignee_email = gitlab_email_for_user_id(gitlab_api, assignee_id)
            if not assignee_email:
                assignee_email = gitlab_email_for_username(gitlab_api, assignee)

            ensure_user_exists(
                fg_client,
                assignee,
                full_name=assignee,
                email=(assignee_email or "").strip() or f"{assignee}@noemail-git.local",
                notify=False,
                reason="needed for issue assignee",
            )
            _ensure_collaborator_with_permission(
                gitlab_api,
                fg_client,
                fg_http,
                owner,
                repo,
                assignee,
                permission="read",
            )

        for u in assignees:
            uid = assignees_ids.get(u)
            u_email = None
            if isinstance(uid, int):
                u_email = gitlab_email_for_user_id(gitlab_api, uid)
            if not u_email:
                u_email = gitlab_email_for_username(gitlab_api, u)

            ensure_user_exists(
                fg_client,
                u,
                full_name=u,
                email=(u_email or "").strip() or f"{u}@noemail-git.local",
                notify=False,
                reason="needed for issue assignees",
            )
            _ensure_collaborator_with_permission(
                gitlab_api,
                fg_client,
                fg_http,
                owner,
                repo,
                u,
                permission="read",
            )

        milestone = None
        if issue.milestone is not None and isinstance(issue.milestone, dict):
            existing_milestone = next(
                (
                    item
                    for item in existing_milestones
                    if item.get("title") == issue.milestone.get("title")
                ),
                None,
            )
            if existing_milestone:
                milestone = existing_milestone.get("id")

        label_ids: List[int] = []
        try:
            for label in getattr(issue, "labels", []) or []:
                existing_label = next(
                    (item for item in existing_labels if item.get("name") == label),
                    None,
                )
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
                fg_print.warning(
                    f"Issue {issue.title} imported as {author_username}, but assignees were dropped due to Forgejo validation."
                )
            else:
                fg_print.error(
                    f"Issue {issue.title} import failed: {import_response_2.text}",
                    f"failed to import issue {issue.title}",
                )
            continue

        fg_print.error(
            f"Issue {issue.title} import failed: {txt}",
            f"failed to import issue {issue.title}",
        )


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
        "auth_token": cfg.GITLAB_TOKEN,
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
            resp = fg_http.post(
                "/repos/migrate",
                json=payload,
                timeout=timeout_seconds,
                sudo=forgejo_owner,
            )
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
        gitlab_api: gitlab.Gitlab,
        fg_client: AuthenticatedClient,
        fg_http: ForgejoHttp,
        forgejo_owner: str,
        forgejo_repo: str,
        collaborators: List[gitlab.v4.objects.ProjectMember],
):
    for collaborator in collaborators:
        username = (getattr(collaborator, "username", "") or "").strip()
        if not username:
            continue

        gl_email = None
        uid = getattr(collaborator, "id", None)
        if isinstance(uid, int):
            gl_email = gitlab_email_for_user_id(gitlab_api, uid)
        if not gl_email:
            gl_email = gitlab_email_for_username(gitlab_api, username)

        ensure_user_exists(
            fg_client,
            username,
            full_name=username,
            email=(gl_email or "").strip() or f"{username}@noemail-git.local",
            notify=False,
            reason="needed for collaborator import",
        )

        if not collaborator_exists(fg_http, forgejo_owner, forgejo_repo, username):
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
                f"/repos/{forgejo_owner}/{forgejo_repo}/collaborators/{username}",
                json={"permission": permission},
                sudo=forgejo_owner,
            )
            if import_response.ok:
                fg_print.info(f"Collaborator {username} imported!")
            else:
                fg_print.error(
                    f"Collaborator {username} import failed: {import_response.text}",
                    f"failed to import collaborator {username} for {forgejo_owner}/{forgejo_repo}",
                )


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
        _import_one_project_full(gitlab_api, fg_client, fg_http, project, idx, len(eligible))


def _load_projects_from_csv(
        gitlab_api: gitlab.Gitlab, csv_path: str
) -> List[gitlab.v4.objects.Project]:
    reader = InputCsvReader(cfg.GITLAB_URL)

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
    print(
        f"Loading membership projects from GitLab as {gitlab_api.user.username}...",
        flush=True,
    )

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

    print(
        f"Done. Membership projects: {fetched}, eligible(write+): {len(eligible)}",
        flush=True,
    )
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
        gitlab_api: gitlab.Gitlab,
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

    owner_obj = get_user_or_group(gitlab_api, fg_client, project)
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
        gitlab_api, fg_client, fg_http, forgejo_owner, forgejo_repo, data["collaborators"]
    )
    _import_project_labels(fg_http, data["labels"], forgejo_owner, forgejo_repo)
    _import_project_milestones(fg_http, data["milestones"], forgejo_owner, forgejo_repo)
    _import_project_issues(gitlab_api, fg_client, fg_http, data["issues"], forgejo_owner, forgejo_repo)


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
