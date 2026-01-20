from typing import List

import gitlab
from pyforgejo import AuthenticatedClient


def _import_users(
        gitlab_api: gitlab.Gitlab,
        fg_client: AuthenticatedClient,
        users: List[gitlab.v4.objects.User],
        notify: bool = False,
):
    from tools.user_import import ensure_importer_user, import_one_gitlab_user
    ensure_importer_user(fg_client, notify=False)
    for u in users:
        import_one_gitlab_user(gitlab_api, fg_client, u, notify=notify)


def import_users(gitlab_api: gitlab.Gitlab, fg_client: AuthenticatedClient, notify=False):
    users_iter = gitlab_api.users.list(iterator=True, per_page=100)
    count = 0
    print(f"Loading users from GitLab as {gitlab_api.user.username}...")

    for user in users_iter:
        count += 1
        if count % 50 == 0:
            print(f"Fetched {count} users...")
        _import_users(gitlab_api, fg_client, [user], notify)

    print(f"Done. Processed {count} users.")
