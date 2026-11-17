# MongoDB Dataset Backup to Google Cloud Storage

A Pixi-managed Python workflow for backing up MongoDB dataset assets to Google Cloud Storage (GCS), then migrating MongoDB download links to verified public GCS URLs. Both scripts process **only documents with `datasetType: "pset"`** and use **only `repositories[0]`**. The ingestion script streams data without local intermediate files; the migration script changes MongoDB only when explicitly instructed.

## How it works

1. Connect to the configured MongoDB database and collection.
2. Check for duplicate `name` values among `pset` documents (the script stops if duplicates are found).
3. Query only documents matching `{ "datasetType": "pset" }`.
4. For each document, use `name` as the top-level GCS prefix and inspect **only `repositories[0]`**.
5. Stream the first repository's `downloadLink` (a URL, a list of URLs, or a list of objects with a `link` key) into `<name>/datasets/<filename>`.
6. Stream each URL in `repositories[0].csvLinks` (if present) into `<name>/files/<filename>`.
7. Perform multiple transfers in parallel, skip destination objects that already exist, retry transient failures, and log transfer speeds and an overall summary.
8. Separately, run `update_download_links.py` to validate public GCS objects and optionally replace repository download URLs while retaining the original source links.

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
    ├── mongo_to_gcs.py
    └── update_download_links.py
```

## 1. Clone the project and install dependencies

```bash
git clone https://github.com/bhklab/dataset-backup-gcs.git
cd dataset-backup-gcs
pixi install
```

The Pixi workspace should include Python and these packages: `pymongo`, `google-cloud-storage`, `requests`, and `python-dotenv`. It should also define `ingest` and `migrate-links` tasks for the two scripts.

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
migrate-links = "python src/update_download_links.py"
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

# Performance: configurable defaults
MAX_WORKERS=12
CHUNK_SIZE_MB=16
HTTP_CHUNK_SIZE_MB=1
MAX_RETRIES=3

# Identify the transfer client to data hosts
HTTP_USER_AGENT=BHKLab-Dataset-Ingestion/1.0 (+https://github.com/bhklab)
```

Use a MongoDB account with read access for ingestion and **read/write access for the URL migration**. If credentials contain reserved URI characters, URL-encode them. Never paste real connection strings into issues or commit them to Git.

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

If needed, associate Application Default Credentials with your billing/quota project:

```bash
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
```

The script uses **Application Default Credentials (ADC)** through `google-cloud-storage`. `gcloud config set project` and ADC are separate settings; ensure the authenticated identity can access the destination bucket. On Cloud Run or a VM, prefer an attached service account rather than a downloaded JSON key.

### Verify connectivity

From the repository root:

```bash
pixi run python -c 'from dotenv import load_dotenv; load_dotenv(); import os; from pymongo import MongoClient; c=MongoClient(os.environ["MONGODB_URI"], serverSelectionTimeoutMS=15000); print("MongoDB ping:", c.admin.command("ping")); from google.cloud import storage; print("GCS bucket exists:", storage.Client().bucket(os.environ["GCS_BUCKET"]).exists())'
```

You should see a successful MongoDB ping and `GCS bucket exists: True`.

## 4.1 Run the ingestion

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

## 4.2 Migrate download links to public GCS URLs

After ingestion, `src/update_download_links.py` can update each eligible MongoDB document so clients download published files from GCS instead of the original source host. **This is a separate operation from uploading**, and it defaults to a preview with **no MongoDB writes**.

### Prerequisites for publication

- Confirm that every object being published is approved for unauthenticated access. The bucket used for migration must be publicly readable, and public URLs must return HTTP `200` without credentials.
- With uniform bucket-level access, granting `allUsers` the bucket-level `roles/storage.objectViewer` role permits public reads **and object listing**, not writes. Public Access Prevention must not be enforced by an applicable policy. Do not grant public write permissions.
- Ensure the MongoDB account has permission to update repository documents; take a collection backup before running `--apply`.
- Check that the ingestion job has already uploaded the expected objects. The migration will **not** upload missing files.

A typical public object URL looks like:

```text
https://storage.googleapis.com/YOUR_BUCKET/Example_Dataset/datasets/part1.rds
```

For names containing special characters, the script URL-encodes the object path.

### Supported MongoDB fields and formats

The migration processes only `datasetType: "pset"` documents. It uses `name` as the top-level prefix and examines **only `repositories[0]`**.

| Repository field | Accepted source format               | Destination                                                          |
| ---------------- | ------------------------------------ | -------------------------------------------------------------------- |
| `downloadLink`   | Single HTTPS URL string              | `<name>/datasets/<filename>`                                         |
| `downloadLink`   | List of HTTPS URL strings            | One object per URL under `<name>/datasets/`                          |
| `downloadLink`   | List of objects with a `link` string | Each object's `link` is updated; other object properties stay intact |
| `csvLinks`       | List of HTTPS URL strings            | One object per URL under `<name>/files/`                             |

Query strings such as `?download=1` are not included in object filenames. Identical filenames across **different datasets** are allowed. Different source URLs that would produce the **same path within one dataset and folder** are rejected rather than overwriting one another.

### What changes in MongoDB

For a repository with multiple primary downloads, the migration preserves the original list shape and the extra metadata in each entry:

**Before:**

```json
{
  "name": "Example_Dataset",
  "datasetType": "pset",
  "repositories": [
    {
      "downloadLink": [
        {
          "link": "https://zenodo.org/records/123/files/part1.rds",
          "type": "expression"
        },
        {
          "link": "https://zenodo.org/records/123/files/part2.rds",
          "type": "metadata"
        }
      ],
      "csvLinks": ["https://zenodo.org/records/123/files/sampleMap.tsv"]
    }
  ]
}
```

**After:** (abbreviated example using `YOUR_BUCKET`)

```json
{
  "name": "Example_Dataset",
  "datasetType": "pset",
  "repositories": [
    {
      "downloadLink": [
        {
          "link": "https://storage.googleapis.com/YOUR_BUCKET/Example_Dataset/datasets/part1.rds",
          "type": "expression"
        },
        {
          "link": "https://storage.googleapis.com/YOUR_BUCKET/Example_Dataset/datasets/part2.rds",
          "type": "metadata"
        }
      ],
      "csvLinks": [
        "https://storage.googleapis.com/YOUR_BUCKET/Example_Dataset/files/sampleMap.tsv"
      ],
      "originalDownloadLink": [
        {
          "link": "https://zenodo.org/records/123/files/part1.rds",
          "type": "expression"
        },
        {
          "link": "https://zenodo.org/records/123/files/part2.rds",
          "type": "metadata"
        }
      ],
      "originalCsvLinks": ["https://zenodo.org/records/123/files/sampleMap.tsv"]
    }
  ]
}
```

The original URLs are stored **inside the same first repository**, in `repositories[0].originalDownloadLink` and `repositories[0].originalCsvLinks`. They are copied when their corresponding fields change, and existing `original*` values are retained on reruns. If no change is needed, no new backup field is created.

### Commands

Run these from the project root:

```bash
# Preview a specific dataset; makes no MongoDB changes
pixi run migrate-links --name "Example_Dataset"

# Preview the first matching pset document
pixi run migrate-links --limit 1

# Preview all pset documents
pixi run migrate-links

# After taking a MongoDB backup, update one named dataset
pixi run migrate-links --name "Example_Dataset" --apply

# Apply the migration to all eligible documents
pixi run migrate-links --apply
```

The `--name` option selects a dataset by its unique MongoDB `name`, `--limit` caps the number of matching documents, and `--apply` enables writes. Without `--apply`, the script checks destinations and logs `READY`, `UNCHANGED`, or `FAILED` without updating MongoDB.

### Validation and safety

- For each document, **every** expected public GCS URL is checked using an unauthenticated HTTP `HEAD` request. The document is updated only if all checks return `200`.
- If any source URL is invalid, a GCS object is missing or private, or two different source URLs map to the same object path, **that document is not updated**. Other eligible documents can still be processed.
- MongoDB updates use an optimistic concurrency condition on the existing `repositories[0]` object to avoid overwriting a concurrent edit.
- Already migrated URLs are recognized and validated, so the migration can be rerun. Existing source-link backups are preserved.
- The migration checks public accessibility, **not** file hashes or byte-for-byte integrity. It doesn't ensure the public bucket remains readable if IAM policies change later.
- If any documents fail validation or updating, the script reports failures and exits with an error; inspect the log and fix missing objects or invalid metadata before retrying.

### Important: keep ingestion pointed at original sources

Once `downloadLink` and `csvLinks` contain GCS URLs, rerunning ingestion against a missing destination could otherwise attempt to retrieve the missing file **from the same bucket**. Update `mongo_to_gcs.py` to prefer preserved original sources, retaining its handling for single URLs and lists of strings or `{ "link": "..." }` objects:

```python
# In build_download_tasks(), use the source values instead of published URLs.
download_link = repository.get("originalDownloadLink")
if download_link is None:
    download_link = repository.get("downloadLink")

csv_links = repository.get("originalCsvLinks")
if csv_links is None:
    csv_links = repository.get("csvLinks") or []
```

This is a **recommended change to the ingestion script**; running the migration alone does not modify `mongo_to_gcs.py`. For future records, keep source and published URLs separate so repairs always use the original source.

## Upload behavior and safeguards

| Case                                                | Behavior                                                        |
| --------------------------------------------------- | --------------------------------------------------------------- |
| `datasetType != "pset"`                             | Excluded by MongoDB query                                       |
| Two `pset` documents have the same `name`           | Preflight validation raises an error                            |
| Dataset has no repository                           | No file tasks generated; warning may appear                     |
| Multiple repositories                               | Only the first repository (`repositories[0]`) is used           |
| Missing `downloadLink`                              | No primary dataset file task                                    |
| Multiple primary `downloadLink` entries             | Each URL (or object `link`) gets its own file under `datasets/` |
| Missing/empty `csvLinks`                            | No supporting file tasks                                        |
| Same filename in **different datasets**             | Allowed; object paths differ                                    |
| Same destination and URL repeated in one repository | Duplicate task ignored                                          |
| Different URLs map to the **same destination**      | Rejected to avoid accidental replacement                        |
| Object already exists in GCS                        | Skipped; source content is **not** compared for updates         |
| Transient download/upload error                     | Retries the full file with backoff, up to `MAX_RETRIES`         |
| Permanent error, such as invalid URL / 404          | Reported as failure; fix source metadata                        |

A **MongoDB unique index** on `name` is recommended if uniqueness must be guaranteed when other applications write new documents. The script's preflight check is not a substitute for a database constraint.

### No intermediate files, but network traffic through the execution host

The script reads remote files in small HTTP chunks and writes them through GCS resumable uploads. **It does not save downloaded dataset files to the local filesystem.** File data still travels through the network connection and memory of the machine running the script, so available upload bandwidth can limit throughput. Running the pipeline on a Google Cloud VM or Cloud Run Job uses the cloud execution environment’s network connection instead.

### Performance tuning

Default settings are a starting point rather than a universal optimum:

| Variable             | Default | Purpose                             |
| -------------------- | ------- | ----------------------------------- |
| `MAX_WORKERS`        | `12`    | Number of concurrent file transfers |
| `CHUNK_SIZE_MB`      | `16`    | GCS resumable upload buffer size    |
| `HTTP_CHUNK_SIZE_MB` | `1`     | Amount read per HTTP iteration      |
| `MAX_RETRIES`        | `3`     | Maximum attempts per file           |

For performance tuning, compare 8, 12, and 16 workers and 16–32 MB GCS chunks, monitoring aggregate throughput, memory usage, and failures. More workers will not help if the execution host’s upload bandwidth is saturated or the source server throttles connections. **GCS chunk sizes must be multiples of 256 KiB.** Benchmark against different not-yet-uploaded files, as existing objects are skipped.

## Migration troubleshooting

**`FAILED` because an object returns `404` or `403`:** Confirm that the file exists at the expected full GCS path and is publicly readable. The migrator leaves that entire document unchanged until all its targets validate.

**`FAILED` due to a filename collision:** Two different links within one dataset resolve to the same `<name>/datasets/<filename>` or `<name>/files/<filename>`. Rename/reorganize upstream files or adjust the ingestion naming convention and migration together.

**`UNCHANGED` on rerun:** The repository already contains the intended GCS links. The script still validates their accessibility.

**MongoDB permission error during `--apply`:** The migration requires an identity with update permission on the collection, unlike the read-only ingestion workflow.

**Ingestion stops repairing files after migration:** Make the original-source preference change above so `mongo_to_gcs.py` reads `originalDownloadLink` and `originalCsvLinks` when available.

## Troubleshooting

**`KeyError: 'MONGODB_URI'` (or another environment variable)**  
Verify `.env` exists in the repository root and that it contains all required keys. Run `pixi run` from the root directory.

**MongoDB connection or authentication failure**  
Check your URI, credentials, Atlas network allowlist / VPN requirements, and the account's read permissions. Confirm the `MONGODB_DATABASE` and `MONGODB_COLLECTION` values.

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

**Typical workflow:** `pixi install` → configure `.env` → authenticate with ADC → `pixi run ingest --limit 1 --dry-run` → `pixi run ingest` → verify public GCS access → back up MongoDB → `pixi run migrate-links` → `pixi run migrate-links --apply`.
