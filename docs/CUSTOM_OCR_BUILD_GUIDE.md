# Custom OCR Parser — Build & Deployment Guide

This document covers the end-to-end commands to build, push, and deploy the Tellius custom OCR parser images for Airbyte file-based source connectors.

## Prerequisites

- Docker Desktop with `buildx` enabled
- AWS CLI configured with access to ECR (`956938892320.dkr.ecr.us-east-1.amazonaws.com`)
- The forked Airbyte repo checked out locally

## Repository Structure

```
airbyte/
├── docker-images/
│   ├── Dockerfile.python-connector              # Airbyte's generic connector Dockerfile
│   ├── Dockerfile.python-connector-base-tellius  # Custom base image Dockerfile
│   └── tellius-ocr-parser/                       # Custom OCR parser package
│       ├── pyproject.toml
│       └── tellius_ocr_parser/
│           ├── __init__.py                       # Auto-patching logic
│           └── parser.py                         # CustomOCRParser implementation
└── airbyte-integrations/
    └── connectors/
        ├── source-gcs/                           # GCS source connector
        ├── source-s3/                            # S3 source connector
        ├── source-google-drive/                  # Google Drive source connector
        └── source-azure-blob-storage/            # Azure Blob Storage source connector
```

## Image Lineage

```
docker.io/airbyte/python-connector-base:4.0.2     (Airbyte official base)
         │
         ▼
tellius/python-connector-base:4.0.2                (Custom base — adds tellius-ocr-parser + .pth)
         │
         ▼
airbyte:source-<connector>-tellius-mistral-ocr-poc-X   (Connector built on custom base)
```

---

## Step 1: Build the Custom Base Image

This image extends Airbyte's official `python-connector-base` with the `tellius-ocr-parser` package and a `.pth` file that auto-patches the CDK's `default_parsers` at Python startup.

```bash
cd /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/docker-images

docker buildx build --platform linux/amd64 \
  -t tellius/python-connector-base:4.0.2 \
  -f Dockerfile.python-connector-base-tellius \
  --load \
  .
```

**What this does:**
- Starts from `airbyte/python-connector-base:4.0.2`
- Copies and installs the `tellius-ocr-parser` pip package (python-docx, python-pptx, requests)
- Creates a `.pth` file in Python's site-packages that triggers `import tellius_ocr_parser` at interpreter startup
- The import replaces `UnstructuredParser` with `CustomOCRParser` in the CDK's `default_parsers` dict
- At runtime, `CustomOCRParser` checks the middleware service for Mistral OCR config and routes accordingly

**Note:** This image stays in your local Docker cache. It is consumed at build time by the connector image — it does NOT need to be pushed to ECR.

---

## Step 2: Build Connector Images

Each connector is built on top of the custom base image using the same `Dockerfile.python-connector`. The only differences are the `CONNECTOR_NAME` build arg and the build context directory.

**Build args (same for all connectors):**
- `BASE_IMAGE=tellius/python-connector-base:4.0.2` — points to our custom base (from Step 1)
- `CONNECTOR_NAME=<connector-name>` — tells the generic Dockerfile which connector to install. **This is required** — omitting it causes `exit code 127` errors at runtime.

### GCS (Google Cloud Storage)

```bash
docker buildx build --platform linux/amd64 --no-cache --load \
  --tag 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:source-gcs-tellius-mistral-ocr-poc-6 \
  -f /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/docker-images/Dockerfile.python-connector \
  --build-arg BASE_IMAGE=tellius/python-connector-base:4.0.2 \
  --build-arg CONNECTOR_NAME=source-gcs \
  /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/airbyte-integrations/connectors/source-gcs
```

### S3 (Amazon S3)

```bash
docker buildx build --platform linux/amd64 --no-cache --load \
  --tag 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:source-s3-tellius-mistral-ocr-poc-1 \
  -f /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/docker-images/Dockerfile.python-connector \
  --build-arg BASE_IMAGE=tellius/python-connector-base:4.0.2 \
  --build-arg CONNECTOR_NAME=source-s3 \
  /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/airbyte-integrations/connectors/source-s3
```

### Google Drive

```bash
docker buildx build --platform linux/amd64 --no-cache --load \
  --tag 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:source-google-drive-tellius-mistral-ocr-poc-1 \
  -f /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/docker-images/Dockerfile.python-connector \
  --build-arg BASE_IMAGE=tellius/python-connector-base:4.0.2 \
  --build-arg CONNECTOR_NAME=source-google-drive \
  /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/airbyte-integrations/connectors/source-google-drive
```

### Azure Blob Storage

```bash
docker buildx build --platform linux/amd64 --no-cache --load \
  --tag 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:source-azure-blob-storage-tellius-mistral-ocr-poc-1 \
  -f /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/docker-images/Dockerfile.python-connector \
  --build-arg BASE_IMAGE=tellius/python-connector-base:4.0.2 \
  --build-arg CONNECTOR_NAME=source-azure-blob-storage \
  /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/airbyte-integrations/connectors/source-azure-blob-storage
```

---

## Step 3: Push to ECR

### Login to ECR

```bash
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 956938892320.dkr.ecr.us-east-1.amazonaws.com
```

### Push connector images

```bash
# GCS
docker push 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:source-gcs-tellius-mistral-ocr-poc-6

# S3
docker push 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:source-s3-tellius-mistral-ocr-poc-1

# Google Drive
docker push 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:source-google-drive-tellius-mistral-ocr-poc-1

# Azure Blob Storage
docker push 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:source-azure-blob-storage-tellius-mistral-ocr-poc-1
```

---

## Step 4: Deploy in Airbyte

### Option A: New Source Definition (recommended for testing)

1. Create a new source definition in Airbyte pointing to the custom image tag
2. Create a new source using that definition
3. Create a connection and configure the stream with **"Unstructured Document Format"** and any parsing strategy (the custom parser ignores the strategy field)

### Option B: Update Existing Source

Update the `actor_definition_version` table in the Airbyte DB:

```sql
UPDATE actor_definition_version
SET docker_image_tag = 'source-gcs-tellius-OCR-1.8.0-2'
WHERE actor_definition_id = '<your-source-definition-id>';
```

---

## Step 5: Verify

After triggering a sync, check the pod logs for confirmation:

```bash
# Find the replication job pod
kubectl get pods -n unstructuredkaiya --sort-by=.metadata.creationTimestamp | tail -5

# Check logs (replace pod name)
kubectl logs replication-job-<JOB_ID>-attempt-0 -n unstructuredkaiya --all-containers | grep -i "tellius"
```

**Expected log lines:**
```
Tellius CustomOCRParser registered, replacing default UnstructuredParser
Tellius CustomOCRParser processing: <file-url>
```

**If you see these, the custom parser is active.** If you see `Unsupported file type` errors, check that the `_get_file_extension` function in `parser.py` correctly handles signed URLs.

---

## Building for Other Connectors

The same custom base image works for **any** file-based source connector. To add a new connector, follow the same pattern from Step 2: change the `CONNECTOR_NAME` build arg, the build context path, and the image tag.

```bash
docker buildx build --platform linux/amd64 --no-cache --load \
  --tag 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:<your-tag> \
  -f /path/to/docker-images/Dockerfile.python-connector \
  --build-arg BASE_IMAGE=tellius/python-connector-base:4.0.2 \
  --build-arg CONNECTOR_NAME=<connector-name> \
  /path/to/airbyte-integrations/connectors/<connector-name>
```

**Connector name reference** (from each connector's `pyproject.toml`):

| Connector | `CONNECTOR_NAME` |
|-----------|-----------------|
| GCS | `source-gcs` |
| S3 | `source-s3` |
| Google Drive | `source-google-drive` |
| Azure Blob Storage | `source-azure-blob-storage` |

---

## Quick Reference: Full Workflow

```bash
DOCKER_IMAGES_DIR=/Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/docker-images
CONNECTORS_DIR=/Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/airbyte-integrations/connectors
ECR_REPO=956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte

# 1. Build custom base image (once, or after parser code changes)
docker buildx build --platform linux/amd64 \
  -t tellius/python-connector-base:4.0.2 \
  -f $DOCKER_IMAGES_DIR/Dockerfile.python-connector-base-tellius --load \
  $DOCKER_IMAGES_DIR

# 2. Build connector image (example: GCS)
docker buildx build --platform linux/amd64 --no-cache --load \
  --tag $ECR_REPO:source-gcs-tellius-mistral-ocr-poc-6 \
  -f $DOCKER_IMAGES_DIR/Dockerfile.python-connector \
  --build-arg BASE_IMAGE=tellius/python-connector-base:4.0.2 \
  --build-arg CONNECTOR_NAME=source-gcs \
  $CONNECTORS_DIR/source-gcs

# 3. Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 956938892320.dkr.ecr.us-east-1.amazonaws.com

# 4. Push
docker push $ECR_REPO:source-gcs-tellius-mistral-ocr-poc-6

# 5. Update Airbyte source definition to use the new tag, then sync
```

---

## Important Notes

- **Platform:** Always build with `--platform linux/amd64` — the K8s cluster runs amd64 nodes.
- **CONNECTOR_NAME is required:** Omitting `--build-arg CONNECTOR_NAME=...` results in an empty entrypoint and `exit code 127` errors at runtime.
- **K8s image caching:** Kubernetes nodes cache images by tag. If you push a new image with the same tag, the node may use the cached version. Always use a new tag (increment the suffix number) or set `imagePullPolicy: Always`.
- **Base image rebuild:** Rebuild the custom base image (Step 1) only when you change the parser code in `tellius-ocr-parser/`. Connector images (Step 2) reference the local base image at build time.

---

## Version History

### GCS (`source-gcs`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-gcs-tellius-OCR-1.8.0-1` | 2026-04-02 | Initial POC with simple parser — had URL parsing bug with signed GCS URLs |
| `source-gcs-tellius-OCR-1.8.0-2` | 2026-04-02 | Fixed `_get_file_extension` to use `urlparse` — PDFs parsed successfully |
| `source-gcs-tellius-docai-poc-2` | 2026-04-04 | Google Document AI POC |
| `source-gcs-tellius-mistral-ocr-poc-1` | 2026-04-05 | First Mistral OCR POC — SDK import error |
| `source-gcs-tellius-mistral-ocr-poc-2` | 2026-04-05 | Switched to REST API — 422 payload fix |
| `source-gcs-tellius-mistral-ocr-poc-4` | 2026-04-07 | Backward-compatible with middleware config check |
| `source-gcs-tellius-mistral-ocr-poc-5` | 2026-04-07 | Simplified middleware (decrypted key directly) |
| `source-gcs-tellius-mistral-ocr-poc-6` | 2026-04-07 | Fixed CONNECTOR_NAME build arg — **current working version** |

### S3 (`source-s3`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-s3-tellius-mistral-ocr-poc-1` | 2026-04-08 | Initial build — tested and working |

### Google Drive (`source-google-drive`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-google-drive-tellius-mistral-ocr-poc-1` | 2026-04-08 | Initial build — tested and working |

### Azure Blob Storage (`source-azure-blob-storage`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-azure-blob-storage-tellius-mistral-ocr-poc-1` | 2026-04-08 | Initial build — tested and working |
