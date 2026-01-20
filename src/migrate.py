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

import json
import os

from docopt import docopt
import gitlab
import gitlab.v4.objects

from pyforgejo import AuthenticatedClient
from pyforgejo.api.miscellaneous import get_version

from migrate_users import import_users
from tools.fg_migration import fg_print
from forgejo_http import ForgejoHttp
from migrate_organizations import import_groups
import tools.migration_config as cfg

from migrate_projects import import_projects


def main():
    _args = docopt(__doc__)
    args = {k.replace("--", ""): v for k, v in _args.items()}

    fg_print.print_color(
        fg_print.Bcolors.HEADER, "---=== Gitlab to Forgejo migration ===---"
    )
    print()

    gl = gitlab.Gitlab(cfg.GITLAB_URL, private_token=cfg.GITLAB_TOKEN)
    gl.auth()
    assert isinstance(gl.user, gitlab.v4.objects.CurrentUser)
    fg_print.info(f"Connected to Gitlab, version: {gl.version()[0]}")

    fg_client = AuthenticatedClient(base_url=cfg.FORGEJO_API_URL, token=cfg.FORGEJO_TOKEN)
    fg_ver = json.loads(get_version.sync_detailed(client=fg_client).content)["version"]
    fg_print.info(f"Connected to Forgejo, version: {fg_ver}")

    fg_http = ForgejoHttp(cfg.FORGEJO_API_URL, cfg.FORGEJO_TOKEN)

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

if __name__ == "__main__":
    main()
