from __future__ import annotations

import csv
import os
import urllib.parse
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Sequence, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class CsvProjectRef:
    url: str
    full_path: str
    host: str


class InputCsvReader:
    def __init__(self, gitlab_url: str) -> None:
        self._cfg_host = urllib.parse.urlparse(gitlab_url).netloc.lower()

    def load_projects(
            self,
            csv_path: str,
            get_project_by_full_path: Callable[[str], T],
            warn: Callable[[str], None],
    ) -> List[T]:
        refs = self.read_projects(csv_path)

        for w in self.validate_hosts(refs):
            warn(w)

        projects: List[T] = []
        for ref in refs:
            try:
                projects.append(get_project_by_full_path(ref.full_path))
            except Exception as e:
                warn(f"Failed to load project for URL {ref.url} (path {ref.full_path}): {e}")

        return projects

    def read_project_urls(self, csv_path: str) -> List[str]:
        self._ensure_csv_exists(csv_path)

        urls: List[str] = []
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            headers = self._normalize_headers(reader.fieldnames)
            if "url" not in headers:
                raise ValueError(f"CSV {csv_path} must contain a header with a 'url' column")

            for row in reader:
                u = (row.get("url") or "").strip()
                if u:
                    urls.append(u)

        return urls

    def read_projects(self, csv_path: str) -> List[CsvProjectRef]:
        urls = self.read_project_urls(csv_path)
        out: List[CsvProjectRef] = []

        for u in urls:
            parsed = urllib.parse.urlparse(u.strip())
            host = (parsed.netloc or "").lower()

            full_path = self.extract_gitlab_full_path_from_url(u)
            if not full_path:
                raise ValueError(f"Could not parse GitLab project path from URL: {u}")

            out.append(CsvProjectRef(url=u, full_path=full_path, host=host))

        return out

    def validate_hosts(self, projects: Sequence[CsvProjectRef]) -> List[str]:
        warnings: List[str] = []
        if not self._cfg_host:
            return warnings

        for p in projects:
            if p.host and p.host != self._cfg_host:
                warnings.append(
                    f"CSV URL host {p.host} differs from configured gitlab_url host {self._cfg_host}: {p.url}"
                )
        return warnings

    def extract_gitlab_full_path_from_url(self, url: str) -> Optional[str]:
        try:
            parsed = urllib.parse.urlparse(url.strip())
            if not parsed.path:
                return None

            p = parsed.path.strip("/")
            if p.endswith(".git"):
                p = p[:-4]

            return p or None
        except Exception:
            return None

    def _ensure_csv_exists(self, csv_path: str) -> None:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

    def _normalize_headers(self, fieldnames: Optional[Iterable[str]]) -> List[str]:
        if not fieldnames:
            return []
        return [h.strip() for h in fieldnames]
