# MongoDB Dataset Backup to Google Cloud Storage

A Pixi-managed Python ingestion pipeline that reads dataset metadata from MongoDB and streams downloadable dataset assets into Google Cloud Storage (GCS). It processes **only documents with `datasetType: "pset"`**, uploads files concurrently, and avoids storing intermediate downloads on the local filesystem.

## How it works

1. Connect to the configured MongoDB database and collection.
2. Check for duplicate `name` values among `pset` documents (the script stops if duplicates are found).
3. Query only documents matching `{ "datasetType": "pset" }`.
4. For each document, use `name` as the top-level GCS prefix and inspect **only `repositories[0]`**.
5. Stream the first repository's `downloadLink` (if present) into `<name>/datasets/<filename>`.
6. Stream each URL in `repositories[0].csvLinks` (if present) into `<name>/files/<filename>`.
7. Perform multiple transfers in parallel, skip destination objects that already exist, retry transient failures, and log transfer speeds and an overall summary.

### GCS object layout

For a MongoDB document such as:

```json
{
  "datasetType": "pset",
  "name": "HDD_v3.1",
  "repositories": [
    {
      "downloadLink": "https://zenodo.org/records/23824194/files/HDD_v3.1.RDS?download=1",
      "csvLinks": [
        "https://zenodo.org/records/23824194/files/sampleMap.tsv?download=1",
        "https://zenodo.org/records/23824194/files/colData.tsv?download=1"
      ]
    }
  ]
}
```

The destination objects would be:

```text
gs://YOUR_BUCKET/
└── HDD_v3.1/
    ├── datasets/
    │   └── HDD_v3.1.RDS
    └── files/
        ├── sampleMap.tsv
        └── colData.tsv
```

GCS uses object names/prefixes rather than conventional filesystem directories. Two datasets **may have files with the same filename** because their full object paths differ (e.g., `GDSC/files/sampleMap.tsv` and `PRISM/files/sampleMap.tsv`). The script preserves filenames derived from the URL path and excludes query parameters such as `?download=1`.

> **Important:** The example `HDD_v3.1` document previously shown has `datasetType: "annotations"`; it would **not** be processed unless the field were `"pset"`. The example above demonstrates the required `pset` shape, not a claim that the original record is eligible.

## Prerequisites

- macOS or Linux (the supplied Pixi workspace targets `osx-arm64`, `osx-64`, and `linux-64`)
- [Pixi](https://pixi.sh/) installed
- Network access to MongoDB and the source URLs (for example, Zenodo)
- An existing GCS bucket
- Google Cloud credentials with permission to **read object metadata** and **create objects** in that bucket (e.g. bucket-level `roles/storage.objectUser`, or equivalent custom permissions)
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) for convenient local authentication

## Project layout

```text
orcestra_dataset_backup/
├── pixi.toml
├── pixi.lock
├── .env                 # local secrets; do not commit
├── .gitignore
└── src/
    └── mongo_to_gcs.py
```

## 1. Clone the project and install dependencies

```bash
git clone https://github.com/bhklab/dataset-backup-gcs.git
cd dataset-backup-gcs
pixi install
```

The Pixi workspace should include Python and these packages: `pymongo`, `google-cloud-storage`, `requests`, and `python-dotenv`. It should also define an `ingest` task that runs `python src/mongo_to_gcs.py`.

If the project does not yet have a `pixi.toml`, create one with:

```toml
[workspace]
name = "mongo-gcs-ingestion"
version = "0.1.0"
description = "Parallel MongoDB pset ingestion into GCS"
channels = ["conda-forge"]
platforms = ["osx-arm64", "osx-64", "linux-64"]

[dependencies]
python = ">=3.12,<3.13"

[pypi-dependencies]
pymongo = ">=4.0,<5"
google-cloud-storage = ">=3.0,<4"
requests = ">=2.32,<3"
python-dotenv = ">=1.0,<2"

[tasks]
ingest = "python src/mongo_to_gcs.py"
```

Then run `pixi install`. **Commit `pixi.toml` and `pixi.lock`** to make environments reproducible.

## 2. Configure environment variables

Create a `.env` file at the repository root:

```dotenv
# MongoDB
MONGODB_URI=mongodb+srv://USERNAME:PASSWORD@YOUR_CLUSTER.mongodb.net/
MONGODB_DATABASE=YOUR_DATABASE
MONGODB_COLLECTION=YOUR_COLLECTION

# Google Cloud Storage (bucket name only, no gs:// prefix)
GCS_BUCKET=YOUR_BUCKET

# Performance: starting point for a 16 GB MacBook Pro
MAX_WORKERS=12
CHUNK_SIZE_MB=16
HTTP_CHUNK_SIZE_MB=1
MAX_RETRIES=3

# Identify the transfer client to data hosts
HTTP_USER_AGENT=BHKLab-Dataset-Ingestion/1.0 (+https://github.com/bhklab)
```

Use a MongoDB account with read access to the collection.

Ensure `.gitignore` includes:

```gitignore
.env
.pixi/
__pycache__/
*.pyc
```

If `.env` was already tracked, `git rm --cached .env` stops tracking the current file, but **does not erase previously committed secrets from Git history**; rotate exposed credentials and clean the history before publishing.

## 3. Authenticate to Google Cloud

For local development, install the [Google Cloud CLI](https://cloud.google.com/sdk/docs/install), then run:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

The script uses **Application Default Credentials (ADC)** through `google-cloud-storage`. `gcloud config set project` and ADC are separate settings; ensure the authenticated identity can access the destination bucket.

### Verify connectivity

From the repository root:

```bash
pixi run python -c 'from dotenv import load_dotenv; load_dotenv(); import os; from pymongo import MongoClient; c=MongoClient(os.environ["MONGODB_URI"], serverSelectionTimeoutMS=15000); print("MongoDB ping:", c.admin.command("ping")); from google.cloud import storage; print("GCS bucket exists:", storage.Client().bucket(os.environ["GCS_BUCKET"]).exists())'
```

You should see a successful MongoDB ping and `GCS bucket exists: True`.

## 4. Run the ingestion

All commands are run from the repository root.

**Preview the first eligible dataset (no downloads or uploads):**

```bash
pixi run ingest --limit 1 --dry-run
```

**Upload the first eligible dataset:**

```bash
pixi run ingest --limit 1
```

**Preview all eligible datasets:**

```bash
pixi run ingest --dry-run
```

**Upload all eligible `pset` datasets:**

```bash
pixi run ingest
```

**Test an alternate concurrency/chunk-size configuration without modifying `.env`:**

```bash
MAX_WORKERS=16 CHUNK_SIZE_MB=32 pixi run ingest
```

`--limit` limits the number of matching **MongoDB documents**, not the number of files. A document can contribute multiple files. The script reads the entire collection to check for duplicate `pset` names before applying `--limit` to ingestion.

### Example logs

```text
INFO | DATASET | GDSC_2020 | 3 file(s)
INFO | START | GDSC_2020/datasets/GDSC.rds | attempt 1/3
INFO | SUCCESS | GDSC_2020/datasets/GDSC.rds | 850.00 MB | 71.00 sec | 11.97 MB/s
INFO | SKIPPED | GDSC_2020/files/sampleMap.tsv
INFO | INGESTION SUMMARY
INFO | Datasets processed: 40
INFO | Files submitted: 64
INFO | Files uploaded: 62
INFO | Files skipped: 1
INFO | Failures: 2
```

Numbers above are illustrative. The script reports per-file size, elapsed time, and end-to-end speed, plus aggregate metrics.

## Upload behavior and safeguards

| Case                                                | Behavior                                                |
| --------------------------------------------------- | ------------------------------------------------------- |
| `datasetType != "pset"`                             | Excluded by MongoDB query                               |
| Two `pset` documents have the same `name`           | Preflight validation raises an error                    |
| Dataset has no repository                           | No file tasks generated; warning may appear             |
| Multiple repositories                               | Only the first repository (`repositories[0]`) is used   |
| Missing `downloadLink`                              | No primary dataset file task                            |
| Missing/empty `csvLinks`                            | No supporting file tasks                                |
| Same filename in **different datasets**             | Allowed; object paths differ                            |
| Same destination and URL repeated in one repository | Duplicate task ignored                                  |
| Different URLs map to the **same destination**      | Rejected to avoid accidental replacement                |
| Object already exists in GCS                        | Skipped; source content is **not** compared for updates |
| Transient download/upload error                     | Retries the full file with backoff, up to `MAX_RETRIES` |
| Permanent error, such as invalid URL / 404          | Reported as failure; fix source metadata                |

A **MongoDB unique index** on `name` is recommended if uniqueness must be guaranteed when other applications write new documents. The script's preflight check is not a substitute for a database constraint.

### No intermediate files, but local network traffic

The script reads remote files in small HTTP chunks and writes them through GCS resumable uploads. **It does not save the downloaded dataset files to your MacBook's disk.** However, when run locally, the bytes still travel **through your MacBook's RAM and internet connection**. Its upload bandwidth can therefore limit overall throughput. Running the same pipeline on a Google Cloud VM or Cloud Run Job moves that network traffic off your laptop.

### Performance tuning

Default settings are a starting point rather than a universal optimum:

| Variable             | Default | Purpose                             |
| -------------------- | ------- | ----------------------------------- |
| `MAX_WORKERS`        | `12`    | Number of concurrent file transfers |
| `CHUNK_SIZE_MB`      | `16`    | GCS resumable upload buffer size    |
| `HTTP_CHUNK_SIZE_MB` | `1`     | Amount read per HTTP iteration      |
| `MAX_RETRIES`        | `3`     | Maximum attempts per file           |

## Troubleshooting

**GCS `403 Forbidden` / bucket access errors**  
Run `gcloud auth application-default login` and check the ADC identity's bucket permissions. Confirm the bucket name is correct and contains no `gs://` prefix.

**`404 Not Found` from a download URL**  
The source URL may be incorrect, renamed, or unavailable. Verify the relevant MongoDB `repositories[0].downloadLink` or `csvLinks` entry. Re-running without correcting a permanently invalid URL will not solve it.

**`IncompleteRead`, timeouts, or throttling (`429`)**  
The script retries transient transfer failures. If they recur, try lowering `MAX_WORKERS` (e.g., 8 or 4). Large-file retries restart the file, rather than resuming the remote HTTP download from the last received byte.

**A dataset is logged with `0 file(s)`**  
Inspect `repositories[0]` in MongoDB. The first repository may be absent or may not contain a usable `downloadLink` or `csvLinks`.

**Files are already in GCS but source files changed**  
The script intentionally skips existing object paths; it does not compare checksums or update them. An explicit refresh/overwrite mode with source verification would be needed for ongoing synchronization.

**Some uploads fail but others finish**  
The worker pool continues processing other tasks and reports failures at the end. Fix the offending source metadata and rerun; successful files will be skipped.

## Operational notes and limitations

- Source URLs must be usable HTTPS links. The current script assumes MongoDB's URLs are trusted; for untrusted inputs, add an allowed-host list and validate redirects to prevent access to internal endpoints.
- The file name is taken from the URL path. Distinct URLs pointing to the same path name within a single destination folder are treated as a collision.
- A GCS object-existence check only confirms an object is present; it does **not** verify it matches the current upstream bytes. For strong data integrity, compare source hashes (where available) or use a validated manifest.
- Concurrent uploads can increase throughput, but may also trigger rate limits on Zenodo or other source servers. Respect host rate limits and terms.
- Memory use increases with `MAX_WORKERS × CHUNK_SIZE_MB`, plus HTTP and SDK overhead.
- Some datasets may have no download links and thus produce no GCS objects. Review `0 file(s)` logs separately from transfer failures.

## Useful links

- [Pixi documentation](https://pixi.sh/latest/)
- [Google Cloud Storage Python client](https://cloud.google.com/python/docs/reference/storage/latest)
- [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
- [Cloud Storage IAM roles](https://cloud.google.com/storage/docs/access-control/iam-roles)
- [MongoDB Python driver](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/)

---

**Typical workflow:** `pixi install` → configure `.env` → authenticate with ADC → `pixi run ingest --limit 1 --dry-run` → `pixi run ingest --limit 1` → `pixi run ingest`.
