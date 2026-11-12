
from __future__ import annotations

import argparse
import logging
import os
import random
import time

from concurrent.futures import (
    FIRST_COMPLETED,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass
from pathlib import PurePosixPath
from threading import local
from urllib.parse import unquote, urlparse

import requests

from dotenv import load_dotenv
from google.api_core.exceptions import (
    GoogleAPICallError,
    PreconditionFailed,
)
from google.cloud import storage
from pymongo import MongoClient


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

MONGO_URI = os.environ["MONGODB_URI"]
MONGO_DB = os.environ["MONGODB_DATABASE"]
MONGO_COLLECTION = os.environ["MONGODB_COLLECTION"]
GCS_BUCKET = os.environ["GCS_BUCKET"]

DATASET_TYPE = "pset"

# Performance configuration
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "12"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

GCS_CHUNK_SIZE = (
    int(os.getenv("CHUNK_SIZE_MB", "16"))
    * 1024 * 1024
)

HTTP_CHUNK_SIZE = (
    int(os.getenv("HTTP_CHUNK_SIZE_MB", "1"))
    * 1024 * 1024
)

# Network configuration
CONNECT_TIMEOUT = 30
READ_TIMEOUT = 300
GCS_TIMEOUT = 300

# Optional identifier for automated HTTP requests.
USER_AGENT = os.getenv(
    "HTTP_USER_AGENT",
    "BHKLab-Dataset-Ingestion/1.0 "
    "(+https://github.com/bhklab)"
)

# HTTP errors eligible for retry
RETRYABLE_HTTP_STATUS = {
    408, 429, 500, 502, 503, 504,
}


# ============================================================
# VALIDATION
# ============================================================

if MAX_WORKERS < 1:
    raise ValueError("MAX_WORKERS must be >= 1")

if MAX_RETRIES < 1:
    raise ValueError("MAX_RETRIES must be >= 1")

if GCS_CHUNK_SIZE <= 0:
    raise ValueError("CHUNK_SIZE_MB must be positive")

if GCS_CHUNK_SIZE % (256 * 1024) != 0:
    raise ValueError(
        "GCS chunk size must be a multiple of 256 KiB"
    )

if HTTP_CHUNK_SIZE < 1:
    raise ValueError("HTTP_CHUNK_SIZE_MB must be positive")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)-8s | "
        "%(threadName)s | %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

# Per-worker resources
_thread = local()


# ============================================================
# DATA STRUCTURES
# ============================================================

@dataclass(frozen=True)
class DownloadTask:
    dataset_name: str
    url: str
    gcs_path: str


@dataclass(frozen=True)
class TransferResult:
    status: str
    bytes_transferred: int = 0
    elapsed_seconds: float = 0.0


# ============================================================
# CLIENT MANAGEMENT
# ============================================================

def get_bucket():
    """
    Return a GCS bucket client local to the worker thread.

    Reusing the client reduces connection overhead.
    """

    if not hasattr(_thread, "bucket"):
        client = storage.Client()
        _thread.bucket = client.bucket(GCS_BUCKET)

    return _thread.bucket


def get_http_session():
    """
    Maintain an HTTP session for each worker.

    Connections may be reused when downloading multiple
    files from the same host.
    """

    if not hasattr(_thread, "session"):
        session = requests.Session()

        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept-Encoding": "identity",
        })

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=4,
            pool_maxsize=4,
            max_retries=0,
        )

        session.mount("https://", adapter)

        _thread.session = session

    return _thread.session


# ============================================================
# DATASET AND FILENAME VALIDATION
# ============================================================

def get_dataset_name(document: dict) -> str:
    """
    Preserve MongoDB's name field as the exact dataset
    directory name.
    """

    name = document.get("name")

    if not isinstance(name, str) or not name:
        raise ValueError("Dataset name is missing")

    if (
        name != name.strip()
        or name in {".", ".."}
        or "/" in name
        or "\\" in name
        or any(ord(char) < 32 for char in name)
    ):
        raise ValueError(
            f"Invalid dataset name: {name!r}"
        )

    return name


def filename_from_url(url: str) -> str:
    """
    Extract a filename without URL query parameters.

    Example:
      .../sampleMap.tsv?download=1
    becomes:
      sampleMap.tsv
    """

    parsed = urlparse(url)

    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError(
            f"Invalid HTTPS download URL: {url}"
        )

    filename = unquote(
        PurePosixPath(parsed.path).name
    )

    if (
        not filename
        or filename in {".", ".."}
        or "/" in filename
        or "\\" in filename
        or any(ord(char) < 32 for char in filename)
    ):
        raise ValueError(
            f"Invalid filename in URL: {url}"
        )

    return filename


# ============================================================
# EXTRACT DOWNLOAD TASKS
# ============================================================

def build_download_tasks(
    document: dict,
) -> list[DownloadTask]:
    """
    Only use repositories[0].

    downloadLink -> <name>/datasets/<filename>
    csvLinks     -> <name>/files/<filename>
    """

    dataset_name = get_dataset_name(document)

    repositories = (
        document.get("repositories") or []
    )

    if not repositories:
        logger.warning(
            "NO REPOSITORY | %s",
            dataset_name,
        )
        return []

    repository = repositories[0]

    if not isinstance(repository, dict):
        raise ValueError(
            f"Invalid repository for {dataset_name}"
        )

    tasks = []
    used_paths = {}

    def add_task(url: str, folder: str):
        if not isinstance(url, str) or not url:
            raise ValueError(
                f"Invalid download URL for {dataset_name}"
            )

        filename = filename_from_url(url)

        gcs_path = (
            f"{dataset_name}/{folder}/{filename}"
        )

        # Same URL and destination: ignore duplicate.
        # Different URLs and same destination: reject.
        if gcs_path in used_paths:
            if used_paths[gcs_path] == url:
                return

            raise ValueError(
                f"Filename collision: {gcs_path}"
            )

        used_paths[gcs_path] = url

        tasks.append(
            DownloadTask(
                dataset_name=dataset_name,
                url=url,
                gcs_path=gcs_path,
            )
        )

    # Primary dataset file
    download_link = repository.get(
        "downloadLink"
    )

    if download_link:
        if isinstance(download_link, list):
            for url in download_link:
                add_task(
                    url['link'] if isinstance(url, dict) else url,
                    "datasets",
                )
        
        else:
            add_task(
                download_link,
                "datasets",
            )

    # Supporting files
    csv_links = repository.get(
        "csvLinks"
    ) or []

    if not isinstance(csv_links, list):
        raise ValueError(
            f"csvLinks must be a list for {dataset_name}"
        )

    for url in csv_links:
        add_task(url, "files")

    return tasks


# ============================================================
# CHECK UNIQUE DATASET NAMES
# ============================================================

def validate_unique_dataset_names(collection):
    """
    Ensure no two pset documents have the same name.
    """

    duplicates = list(
        collection.aggregate([
            {
                "$match": {
                    "datasetType": DATASET_TYPE
                }
            },
            {
                "$group": {
                    "_id": "$name",
                    "count": {"$sum": 1},
                }
            },
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            },
            {"$limit": 10},
        ])
    )

    if duplicates:
        raise ValueError(
            f"Duplicate pset names found: {duplicates}"
        )


# ============================================================
# RETRY HELPERS
# ============================================================

def is_retryable(error: Exception) -> bool:
    """
    Identify transient HTTP and GCS errors.
    """

    if isinstance(error, requests.HTTPError):
        response = error.response

        return (
            response is not None
            and response.status_code
            in RETRYABLE_HTTP_STATUS
        )

    if isinstance(
        error,
        (requests.ConnectionError, requests.Timeout),
    ):
        return True

    if isinstance(error, GoogleAPICallError):
        status_code = getattr(error, "code", None)

        if hasattr(status_code, "value"):
            status_code = status_code.value

        return status_code in RETRYABLE_HTTP_STATUS

    return False


def retry_delay(attempt: int) -> float:
    """
    Exponential backoff with a small amount of jitter.
    """

    base_delay = min(2 ** attempt, 30)

    return base_delay + random.uniform(0, 1)


# ============================================================
# STREAM DOWNLOAD DIRECTLY TO GCS
# ============================================================

def stream_to_gcs(
    task: DownloadTask,
) -> TransferResult:
    """
    Stream HTTP -> GCS without storing an intermediate file.

    Measures end-to-end throughput for every completed file.
    """

    bucket = get_bucket()
    blob = bucket.blob(task.gcs_path)

    # Skip files already present in GCS
    if blob.exists():
        logger.info(
            "SKIPPED | %s",
            task.gcs_path,
        )

        return TransferResult(status="skipped")

    session = get_http_session()

    for attempt in range(1, MAX_RETRIES + 1):
        start_time = time.perf_counter()
        bytes_transferred = 0

        try:
            logger.info(
                "START | %s | attempt %d/%d",
                task.gcs_path,
                attempt,
                MAX_RETRIES,
            )

            with session.get(
                task.url,
                stream=True,
                timeout=(
                    CONNECT_TIMEOUT,
                    READ_TIMEOUT,
                ),
            ) as response:

                response.raise_for_status()

                content_type = response.headers.get(
                    "Content-Type",
                    "application/octet-stream",
                ).split(";")[0].strip()

                content_length = response.headers.get(
                    "Content-Length"
                )

                expected_size = None

                # Verify the response length when known
                # and no content encoding is applied.
                if (
                    content_length is not None
                    and response.headers.get(
                        "Content-Encoding", "identity"
                    ).lower() == "identity"
                ):
                    expected_size = int(content_length)

                # Streaming GCS resumable upload
                with blob.open(
                    mode="wb",
                    chunk_size=GCS_CHUNK_SIZE,
                    content_type=content_type,
                    if_generation_match=0,
                    timeout=GCS_TIMEOUT,
                ) as destination:

                    for chunk in response.iter_content(
                        chunk_size=HTTP_CHUNK_SIZE
                    ):
                        if not chunk:
                            continue

                        destination.write(chunk)

                        bytes_transferred += len(chunk)

                    # Prevent incomplete HTTP responses
                    # from being finalized in GCS.
                    if (
                        expected_size is not None
                        and bytes_transferred != expected_size
                    ):
                        raise IOError(
                            "Incomplete download: "
                            f"expected {expected_size} bytes, "
                            f"received {bytes_transferred}"
                        )

            elapsed = (
                time.perf_counter() - start_time
            )

            size_mb = (
                bytes_transferred / (1024 * 1024)
            )

            speed_mbps = (
                size_mb / max(elapsed, 0.001)
            )

            logger.info(
                "SUCCESS | %s | %.2f MB | "
                "%.2f sec | %.2f MB/s",
                task.gcs_path,
                size_mb,
                elapsed,
                speed_mbps,
            )

            return TransferResult(
                status="uploaded",
                bytes_transferred=bytes_transferred,
                elapsed_seconds=elapsed,
            )

        except PreconditionFailed:
            # Another worker/process completed the upload.
            logger.info(
                "SKIPPED | Object already created: %s",
                task.gcs_path,
            )

            return TransferResult(status="skipped")

        except Exception as exc:
            # Some GCS upload errors may be wrapped by
            # the underlying resumable-media library.
            retryable = (
                is_retryable(exc)
                or isinstance(exc, (OSError, IOError))
            )

            if not retryable or attempt == MAX_RETRIES:
                logger.error(
                    "FAILED | %s | %s",
                    task.gcs_path,
                    exc,
                )
                raise

            delay = retry_delay(attempt)

            logger.warning(
                "RETRY | %s | attempt %d/%d | "
                "waiting %.1f seconds | %s",
                task.gcs_path,
                attempt,
                MAX_RETRIES,
                delay,
                exc,
            )

            time.sleep(delay)

    raise RuntimeError(
        f"Transfer failed: {task.gcs_path}"
    )


# ============================================================
# MAIN INGESTION PIPELINE
# ============================================================

def run_pipeline(
    limit: int | None = None,
    dry_run: bool = False,
):
    """
    Read MongoDB pset documents and submit file
    downloads to a bounded thread pool.
    """

    pipeline_start = time.perf_counter()

    mongo = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=15000,
    )

    stats = {
        "datasets": 0,
        "submitted": 0,
        "uploaded": 0,
        "skipped": 0,
        "failed": 0,
    }

    total_bytes_uploaded = 0

    try:
        collection = mongo[
            MONGO_DB
        ][MONGO_COLLECTION]

        logger.info("=" * 60)
        logger.info("MONGODB -> GCS INGESTION")
        logger.info("Dataset type: %s", DATASET_TYPE)
        logger.info("Parallel workers: %d", MAX_WORKERS)
        logger.info(
            "GCS chunk size: %.0f MB",
            GCS_CHUNK_SIZE / (1024 * 1024),
        )
        logger.info(
            "HTTP chunk size: %.0f MB",
            HTTP_CHUNK_SIZE / (1024 * 1024),
        )
        logger.info("Bucket: %s", GCS_BUCKET)
        logger.info("=" * 60)

        validate_unique_dataset_names(collection)

        # Filter on MongoDB, not in Python.
        cursor = collection.find(
            {"datasetType": DATASET_TYPE},
            {
                "name": 1,
                "repositories": 1,
            },
        ).sort("_id", 1).batch_size(100)

        if limit is not None:
            cursor = cursor.limit(limit)

        def iter_download_tasks():
            for document in cursor:
                try:
                    tasks = build_download_tasks(
                        document
                    )

                except (ValueError, TypeError):
                    stats["failed"] += 1

                    logger.exception(
                        "INVALID DOCUMENT | %s",
                        document.get("_id"),
                    )
                    continue

                stats["datasets"] += 1

                logger.info(
                    "DATASET | %s | %d file(s)",
                    document["name"],
                    len(tasks),
                )

                for task in tasks:
                    yield task

        # ------------------------------------------------
        # DRY RUN
        # ------------------------------------------------

        if dry_run:
            for task in iter_download_tasks():
                logger.info(
                    "DRY RUN | %s -> gs://%s/%s",
                    task.url,
                    GCS_BUCKET,
                    task.gcs_path,
                )

                stats["submitted"] += 1

        # ------------------------------------------------
        # PARALLEL TRANSFERS
        # ------------------------------------------------

        else:
            pending = set()

            def collect_completed():
                nonlocal total_bytes_uploaded

                done, _ = wait(
                    pending,
                    return_when=FIRST_COMPLETED,
                )

                pending.difference_update(done)

                for future in done:
                    try:
                        result = future.result()

                        stats[result.status] += 1

                        if result.status == "uploaded":
                            total_bytes_uploaded += (
                                result.bytes_transferred
                            )

                    except Exception:
                        stats["failed"] += 1

                        logger.exception(
                            "TRANSFER FAILED"
                        )

            with ThreadPoolExecutor(
                max_workers=MAX_WORKERS,
                thread_name_prefix="GCSWorker",
            ) as executor:

                for task in iter_download_tasks():
                    future = executor.submit(
                        stream_to_gcs,
                        task,
                    )

                    pending.add(future)
                    stats["submitted"] += 1

                    # Prevent unbounded queue growth.
                    if len(pending) >= MAX_WORKERS * 4:
                        collect_completed()

                while pending:
                    collect_completed()

        # ------------------------------------------------
        # FINAL SUMMARY
        # ------------------------------------------------

        total_elapsed = (
            time.perf_counter() - pipeline_start
        )

        total_mb = (
            total_bytes_uploaded / (1024 * 1024)
        )

        aggregate_speed = (
            total_mb / max(total_elapsed, 0.001)
        )

        logger.info("=" * 60)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 60)

        logger.info(
            "Datasets processed: %d",
            stats["datasets"],
        )

        logger.info(
            "Files submitted: %d",
            stats["submitted"],
        )

        logger.info(
            "Files uploaded: %d",
            stats["uploaded"],
        )

        logger.info(
            "Files skipped: %d",
            stats["skipped"],
        )

        logger.info(
            "Failures: %d",
            stats["failed"],
        )

        logger.info(
            "Total uploaded: %.2f MB",
            total_mb,
        )

        logger.info(
            "Total elapsed: %.2f seconds",
            total_elapsed,
        )

        logger.info(
            "Average aggregate throughput: %.2f MB/s",
            aggregate_speed,
        )

        logger.info("=" * 60)

        if stats["failed"] > 0:
            raise RuntimeError(
                f"Ingestion completed with "
                f"{stats['failed']} failure(s)"
            )

    finally:
        mongo.close()


# ============================================================
# COMMAND LINE INTERFACE
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Parallel streaming of MongoDB pset "
            "datasets into Google Cloud Storage"
        )
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of pset documents",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview destination paths without uploading",
    )

    args = parser.parse_args()

    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be positive")

    run_pipeline(
        limit=args.limit,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
