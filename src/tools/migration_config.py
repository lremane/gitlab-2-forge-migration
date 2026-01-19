import os
import configparser

_MIGRATE_INI_PATH = os.path.join(os.path.dirname(__file__), "../..", ".migrate.ini")

if not os.path.exists(_MIGRATE_INI_PATH):
    print("Please create .migrate.ini as explained in the README!")
    os.sys.exit()

config = configparser.RawConfigParser()
config.read(_MIGRATE_INI_PATH)

GITLAB_URL = config.get("migrate", "gitlab_url")
GITLAB_TOKEN = config.get("migrate", "gitlab_token")
GITLAB_ADMIN_USER = config.get("migrate", "gitlab_admin_user")
GITLAB_ADMIN_PASS = config.get("migrate", "gitlab_admin_pass")

FORGEJO_URL = config.get("migrate", "forgejo_url")
FORGEJO_API_URL = f"{FORGEJO_URL}/api/v1"
FORGEJO_TOKEN = config.get("migrate", "forgejo_token")
FORGEJO_USER = config.get("migrate", "forgejo_admin_user")
FORGEJO_PASSWORD = config.get("migrate", "forgejo_admin_pass")
