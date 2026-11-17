"""Migrate public download links for MongoDB pset datasets to GCS.

Supports downloadLink as a string, list of strings, or list of dictionaries
with a 'link' key; preserves list shape and additional dictionary fields.
"""
from __future__ import annotations

import argparse
import copy
import logging
import os
from pathlib import PurePosixPath
from urllib.parse import quote, unquote, urlparse

import requests
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
LOG = logging.getLogger("migrate_links")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def valid_name(name: str) -> str:
    if (not isinstance(name, str) or not name or name != name.strip()
            or name in {".", ".."} or "/" in name or "\\" in name
            or any(ord(c) < 32 for c in name)):
        raise ValueError(f"Invalid dataset name: {name!r}")
    return name


def filename_from_url(url: str) -> str:
    if not isinstance(url, str):
        raise ValueError("Download URL must be a string")
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError(f"Invalid HTTPS URL: {url!r}")
    filename = unquote(PurePosixPath(parsed.path).name)
    if (not filename or filename in {".", ".."} or "/" in filename
            or "\\" in filename or any(ord(c) < 32 for c in filename)):
        raise ValueError(f"Invalid filename in URL: {url!r}")
    return filename


def public_url(bucket: str, name: str, subfolder: str, source_url: str) -> str:
    path = f"{name}/{subfolder}/{filename_from_url(source_url)}"
    return f"https://storage.googleapis.com/{bucket}/{quote(path, safe='/')}"


def is_our_public_url(bucket: str, url: str) -> bool:
    parsed = urlparse(url)
    return (parsed.scheme == "https" and parsed.netloc == "storage.googleapis.com"
            and parsed.path.startswith(f"/{bucket}/"))


def validate_public_url(session: requests.Session, url: str) -> None:
    try:
        response = session.head(url, timeout=(15, 60), allow_redirects=False)
    except requests.RequestException as exc:
        raise ValueError(f"Public URL check failed for {url}: {exc}") from exc
    if response.status_code != 200:
        raise ValueError(f"Public URL returned HTTP {response.status_code}: {url}")


def migrate_repo(name: str, repo: dict, bucket: str, session: requests.Session) -> dict:
    """Return a migrated copy or raise; do not modify the input repo."""
    updated = copy.deepcopy(repo)
    primary = repo.get("downloadLink")
    supporting = repo.get("csvLinks")
    if supporting is None:
        supporting = []
    if not isinstance(supporting, list):
        raise ValueError("csvLinks must be a list")

    # An already-migrated record retains original source links here.
    original_primary = repo.get("originalDownloadLink", primary)
    original_supporting = repo.get("originalCsvLinks", supporting)

    # Track all expected destination paths. Exact duplicate URL-to-path pairs
    # are tolerated, as in the ingestion script; collisions are rejected.
    path_sources: dict[str, str] = {}
    expected: set[str] = set()

    def target(url: str, folder: str) -> str:
        if not isinstance(url, str) or not url:
            raise ValueError("Empty or invalid download URL")
        destination = public_url(bucket, name, folder, url)
        # If already public, don't build a second path from its filename.
        if is_our_public_url(bucket, url):
            expected_prefix = f"https://storage.googleapis.com/{bucket}/{quote(name, safe='')}/{folder}/"
            if not url.startswith(expected_prefix):
                raise ValueError(f"Unexpected public destination path: {url}")
            destination = url
        path = urlparse(destination).path
        previous = path_sources.get(path)
        if previous is not None and previous != url:
            # A migration rerun may mix GCS and original URLs mapping to same
            # object; this is ambiguous and should be manually resolved.
            raise ValueError(f"Different source URLs map to same GCS path: {destination}")
        path_sources[path] = url
        expected.add(destination)
        return destination

    def convert_primary(value):
        if value is None or value == "":
            return value
        if isinstance(value, str):
            return target(value, "datasets")
        if isinstance(value, list):
            converted = []
            for item in value:
                if isinstance(item, str):
                    converted.append(target(item, "datasets"))
                elif isinstance(item, dict) and isinstance(item.get("link"), str):
                    new_item = copy.deepcopy(item)
                    new_item["link"] = target(item["link"], "datasets")
                    converted.append(new_item)
                else:
                    raise ValueError(f"Invalid downloadLink list entry: {item!r}")
            return converted
        raise ValueError(f"Unsupported downloadLink type: {type(value).__name__}")

    converted_primary = convert_primary(primary)
    converted_supporting = [target(url, "files") for url in supporting]
    if not expected:
        raise ValueError("No download URLs present")

    # Don't write to MongoDB unless ALL expected targets respond publicly.
    for url in sorted(expected):
        validate_public_url(session, url)

    if converted_primary != primary:
        updated.setdefault("originalDownloadLink", copy.deepcopy(original_primary))
        updated["downloadLink"] = converted_primary
    if converted_supporting != supporting:
        updated.setdefault("originalCsvLinks", copy.deepcopy(original_supporting))
        updated["csvLinks"] = converted_supporting
    return updated


def run(*, apply: bool, limit: int | None, only_name: str | None) -> None:
    uri = os.environ["MONGODB_URI"]
    db = os.environ["MONGODB_DATABASE"]
    collection_name = os.environ["MONGODB_COLLECTION"]
    bucket = os.environ["GCS_BUCKET"]

    stats = {"checked": 0, "ready": 0, "updated": 0, "unchanged": 0, "failed": 0}
    mongo = MongoClient(uri, serverSelectionTimeoutMS=15000)
    session = requests.Session()
    try:
        collection = mongo[db][collection_name]
        criteria = {"datasetType": "pset"}
        if only_name is not None:
            criteria["name"] = only_name
        cursor = collection.find(criteria, {"name": 1, "repositories": 1}).sort("_id", 1).batch_size(100)
        if limit is not None:
            cursor = cursor.limit(limit)
        for doc in cursor:
            stats["checked"] += 1
            name = doc.get("name")
            try:
                valid_name(name)
                repos = doc.get("repositories")
                if not isinstance(repos, list) or not repos or not isinstance(repos[0], dict):
                    raise ValueError("Missing or invalid repositories[0]")
                existing = repos[0]
                updated = migrate_repo(name, existing, bucket, session)
                if updated == existing:
                    stats["unchanged"] += 1
                    LOG.info("UNCHANGED | %s", name)
                elif not apply:
                    stats["ready"] += 1
                    LOG.info("READY | %s | %s", name, updated.get("downloadLink"))
                else:
                    result = collection.update_one(
                        {"_id": doc["_id"], "name": name,
                         "datasetType": "pset", "repositories.0": existing},
                        {"$set": {"repositories.0": updated}},
                    )
                    if result.modified_count != 1:
                        raise RuntimeError("Concurrent modification; no update applied")
                    stats["updated"] += 1
                    LOG.info("UPDATED | %s", name)
            except Exception as exc:
                stats["failed"] += 1
                LOG.error("FAILED | %s | %s", name, exc)
        LOG.info("SUMMARY | %s", stats)
        if stats["failed"]:
            raise RuntimeError(f"{stats['failed']} dataset(s) could not be migrated")
    finally:
        session.close()
        mongo.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate pset repository links to public GCS URLs")
    parser.add_argument("--apply", action="store_true", help="Write validated updates to MongoDB")
    parser.add_argument("--limit", type=int, help="Maximum number of matching documents")
    parser.add_argument("--name", help="Process one dataset by its unique name")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")
    run(apply=args.apply, limit=args.limit, only_name=args.name)


if __name__ == "__main__":
    main()
