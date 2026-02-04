import configparser
import requests
import csv

config = configparser.ConfigParser()
config.read("../.migrate.ini")

if "migrate" not in config:
    raise RuntimeError("Missing [migrate] section in .migrate.ini")

raw_gitlab_url = config["migrate"].get("gitlab_url", "").strip()
token = config["migrate"].get("gitlab_token", "").strip()

if not raw_gitlab_url:
    raise RuntimeError("gitlab_url is not set")

if not token:
    raise RuntimeError("gitlab_token is not set")

if raw_gitlab_url.startswith("http://"):
    base_url = raw_gitlab_url.replace("http://", "https://", 1)
elif raw_gitlab_url.startswith("https://"):
    base_url = raw_gitlab_url
else:
    base_url = f"https://{raw_gitlab_url}"

api_url = f"{base_url.rstrip('/')}/api/v4"

headers = {"PRIVATE-TOKEN": token}

page = 1
per_page = 100

with open("../gitlab_repos.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["name", "url", "repo_size_bytes", "lfs_size_bytes"])

    while True:
        resp = requests.get(
            f"{api_url}/projects",
            headers=headers,
            params={
                # "membership": True,
                "statistics": True,
                "per_page": per_page,
                "page": page,
            },
            timeout=60,
        )
        resp.raise_for_status()
        projects = resp.json()

        if not projects:
            break

        for p in projects:
            name = p.get("path_with_namespace", "")
            url = p.get("web_url", "")
            stats = p.get("statistics") or {}
            size = stats.get("repository_size") # full Git history size (all blobs, all commits), but no LFS or artifacts.
            lfs_size = stats.get("lfs_objects_size", 0)
            writer.writerow([name, url, size, lfs_size])

        page += 1
