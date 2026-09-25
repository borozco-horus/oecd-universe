from __future__ import annotations

from oecd_universe.provenance.artifacts import read_artifacts, register_bytes, sha256_bytes
from oecd_universe.settings import Settings


def test_artifact_hash_and_immutable_deduplication(settings: Settings) -> None:
    content = b"scientific source bytes"
    first, digest = register_bytes(
        settings,
        provider="oecd",
        source_url="https://example.test/file",
        content=content,
        file_name="file.bin",
    )
    second, second_digest = register_bytes(
        settings,
        provider="oecd",
        source_url="https://example.test/file",
        content=content,
        file_name="file.bin",
    )
    assert digest == sha256_bytes(content) == second_digest
    assert first == second
    assert first.read_bytes() == content
    assert read_artifacts(settings).height == 1


def test_new_content_creates_new_artifact(settings: Settings) -> None:
    first, _ = register_bytes(
        settings,
        provider="oecd",
        source_url="https://example.test/file",
        content=b"v1",
        file_name="file.bin",
    )
    second, _ = register_bytes(
        settings,
        provider="oecd",
        source_url="https://example.test/file",
        content=b"v2",
        file_name="file.bin",
    )
    assert first != second
    assert first.read_bytes() == b"v1"
    assert read_artifacts(settings).height == 2
