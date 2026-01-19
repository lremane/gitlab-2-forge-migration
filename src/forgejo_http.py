from typing import Optional

import requests


class ForgejoHttp:
    def __init__(self, api_url: str, token: str):
        self.api_url = api_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Authorization": f"token {token}",
            }
        )

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.api_url}{path}"

    def _merge_headers(self, headers: Optional[dict], sudo: Optional[str]) -> dict:
        merged = {}
        if headers:
            merged.update(headers)
        if sudo:
            merged["Sudo"] = sudo
        return merged

    def get(self, path: str, sudo: Optional[str] = None, **kwargs) -> requests.Response:
        headers = self._merge_headers(kwargs.pop("headers", None), sudo)
        return self.session.get(self._url(path), timeout=kwargs.pop("timeout", 30), headers=headers, **kwargs)

    def post(self, path: str, sudo: Optional[str] = None, **kwargs) -> requests.Response:
        headers = self._merge_headers(kwargs.pop("headers", None), sudo)
        return self.session.post(self._url(path), timeout=kwargs.pop("timeout", 30), headers=headers, **kwargs)

    def put(self, path: str, sudo: Optional[str] = None, **kwargs) -> requests.Response:
        headers = self._merge_headers(kwargs.pop("headers", None), sudo)
        return self.session.put(self._url(path), timeout=kwargs.pop("timeout", 30), headers=headers, **kwargs)

    def patch(self, path: str, sudo: Optional[str] = None, **kwargs) -> requests.Response:
        headers = self._merge_headers(kwargs.pop("headers", None), sudo)
        return self.session.patch(self._url(path), timeout=kwargs.pop("timeout", 30), headers=headers, **kwargs)

    def delete(self, path: str, sudo: Optional[str] = None, **kwargs) -> requests.Response:
        headers = self._merge_headers(kwargs.pop("headers", None), sudo)
        return self.session.delete(self._url(path), timeout=kwargs.pop("timeout", 30), headers=headers, **kwargs)

