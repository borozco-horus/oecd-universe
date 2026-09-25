from __future__ import annotations

import time
from dataclasses import dataclass

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from oecd_universe.provenance.artifacts import latest_for_url, register_bytes
from oecd_universe.settings import Settings


@dataclass(frozen=True)
class Acquired:
    content: bytes
    sha256: str
    relative_path: str
    not_modified: bool = False


class SourceClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = httpx.Client(
            headers={"User-Agent": settings.user_agent},
            timeout=httpx.Timeout(300.0, connect=30.0),
            follow_redirects=True,
        )

    def close(self) -> None:
        self.client.close()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=1, max=30),
        retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
        reraise=True,
    )
    def get(
        self,
        url: str,
        *,
        provider: str,
        file_name: str,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
        accept: str | None = None,
    ) -> Acquired:
        prior = latest_for_url(self.settings, url)
        headers: dict[str, str] = {}
        if accept:
            headers["Accept"] = accept
        if prior and prior.get("etag"):
            headers["If-None-Match"] = str(prior["etag"])
        if prior and prior.get("last_modified"):
            headers["If-Modified-Since"] = str(prior["last_modified"])
        response = self.client.get(url, headers=headers)
        if response.status_code == 304 and prior:
            path = self.settings.path(str(prior["relative_path"]))
            return Acquired(path.read_bytes(), str(prior["sha256"]), str(path), True)
        response.raise_for_status()
        path, digest = register_bytes(
            self.settings,
            provider=provider,
            source_url=url,
            content=response.content,
            file_name=file_name,
            media_type=response.headers.get("content-type", "application/octet-stream"),
            last_modified=response.headers.get("last-modified"),
            etag=response.headers.get("etag"),
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )
        time.sleep(self.settings.request_delay_seconds)
        return Acquired(response.content, digest, str(path), False)
