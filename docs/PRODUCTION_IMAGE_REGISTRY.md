# Production Image Registry — Tellius Custom Airbyte Connectors

This document is the **single source of truth** for all custom Airbyte connector images deployed in production. It tracks the current live image tag for each connector, the base image they depend on, and a changelog of what changed in each version.

> **How to use this document:**
>
> 1. **Before deploying:** Check the "Current Production Images" table below to see what's currently running.
> 2. **After building & pushing a new image:** Update this document with a new row in the relevant changelog table AND update the "Current Production Images" table to reflect the new active tag.
> 3. **Commit this file** alongside any parser/Dockerfile changes so the branch always reflects what's deployed.
> 4. **Tag naming convention:** `<component>-tellius-release-<airbyte-base-version>-v<increment>` — always increment the `v` suffix; never reuse a tag.

---

## Current Production Images

**ECR Registry:** `956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte`

| Component | Current Tag | Date Deployed | Based On |
|-----------|-------------|---------------|----------|
| **Custom Base Image** | `python-connector-base-tellius-release-v2` | 2026-04-13 | `docker.io/airbyte/python-connector-base:4.0.2` |
| **Google Drive** | `source-gDrive-tellius-release-1.8.0-v3` | 2026-04-13 | `python-connector-base-tellius-release-v2` |
| **GCS** | `source-gcs-tellius-release-1.8.0-v3` | 2026-04-13 | `python-connector-base-tellius-release-v2` |
| **Azure Blob Storage** | `source-azure-blob-tellius-release-1.8.0-v2` | 2026-04-13 | `python-connector-base-tellius-release-v2` |
| **S3** | `source-s3-tellius-release-1.8.0-v2` | 2026-04-13 | `python-connector-base-tellius-release-v2` |
| **SharePoint** | `source-sharepoint-tellius-release-1.8.0-v1` | 2026-03-31 | `python-connector-base-tellius-release-v2` |

---

## Image Lineage

```
docker.io/airbyte/python-connector-base:4.0.2         (Airbyte official)
         │
         ▼
python-connector-base-tellius-release-v2               (Custom base: tellius-ocr-parser + .pth patch)
         │
         ├──▶ source-gDrive-tellius-release-1.8.0-v3
         ├──▶ source-gcs-tellius-release-1.8.0-v3
         ├──▶ source-azure-blob-tellius-release-1.8.0-v2
         ├──▶ source-s3-tellius-release-1.8.0-v2
         └──▶ source-sharepoint-tellius-release-1.8.0-v1
```

---

## What the Custom Base Image Contains

The custom base image extends Airbyte's official `python-connector-base` with:

- **`tellius-ocr-parser`** — a pip package containing `CustomOCRParser` that replaces Airbyte's default `UnstructuredParser`
- **`.pth` auto-patch** — a Python site-packages hook that swaps the parser at interpreter startup (no connector code changes needed)
- **Runtime config routing** — at sync time, the parser checks a Tellius middleware endpoint to decide whether to use Mistral OCR or fall back to the default `UnstructuredParser` (unstructured.io)

### Supported File Types (Mistral OCR mode)

| Extension | MIME Type | Method |
|-----------|-----------|--------|
| `.pdf` | `application/pdf` | Mistral OCR via data URI |
| `.docx` | `application/vnd.openxmlformats-officedocument.wordprocessingml.document` | Mistral OCR via data URI |
| `.pptx` | `application/vnd.openxmlformats-officedocument.presentationml.presentation` | Mistral OCR via data URI |
| `.md` | — | Direct UTF-8 read |
| `.txt` | — | Direct UTF-8 read |

---

## Changelogs

### Custom Base Image (`python-connector-base-tellius-release-*`)

| Tag | Date | Changes |
|-----|------|---------|
| `python-connector-base-tellius-release-v1` | 2026-04-08 | Initial production base image. Mistral OCR for PDFs; DOCX/PPTX handled by local `python-docx`/`python-pptx` libraries. |
| `python-connector-base-tellius-release-v2` | 2026-04-13 | **DOCX/PPTX native Mistral OCR support.** All document types (PDF, DOCX, PPTX) now routed to Mistral OCR API via data URI with proper MIME types. Removed `python-docx` and `python-pptx` dependencies. Removed debug logging. |

### Google Drive (`source-gDrive-tellius-release-*`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-gDrive-tellius-release-1.8.0-v1` | 2026-03-xx | Pre-Mistral custom image (before OCR parser integration). |
| `source-gDrive-tellius-release-1.8.0-v2` | 2026-04-08 | Rebuilt on `python-connector-base-tellius-release-v1` with Mistral OCR support for PDFs. |
| `source-gDrive-tellius-release-1.8.0-v3` | 2026-04-13 | Rebuilt on `python-connector-base-tellius-release-v2`. Adds native DOCX/PPTX Mistral OCR support. |

### GCS (`source-gcs-tellius-release-*`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-gcs-tellius-release-1.8.0-v1` | 2026-04-xx | Pre-production POC (not deployed). |
| `source-gcs-tellius-release-1.8.0-v2` | 2026-04-08 | Production release on `python-connector-base-tellius-release-v1`. Mistral OCR for PDFs. |
| `source-gcs-tellius-release-1.8.0-v3` | 2026-04-13 | Rebuilt on `python-connector-base-tellius-release-v2`. Adds native DOCX/PPTX Mistral OCR support. |

### Azure Blob Storage (`source-azure-blob-tellius-release-*`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-azure-blob-tellius-release-1.8.0-v1` | 2026-04-08 | Initial production release on `python-connector-base-tellius-release-v1`. |
| `source-azure-blob-tellius-release-1.8.0-v2` | 2026-04-13 | Rebuilt on `python-connector-base-tellius-release-v2`. Adds native DOCX/PPTX Mistral OCR support. |

### S3 (`source-s3-tellius-release-*`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-s3-tellius-release-1.8.0-v1` | 2026-04-08 | Initial production release on `python-connector-base-tellius-release-v1`. |
| `source-s3-tellius-release-1.8.0-v2` | 2026-04-13 | Rebuilt on `python-connector-base-tellius-release-v2`. Adds native DOCX/PPTX Mistral OCR support. |

### SharePoint (`source-sharepoint-tellius-release-*`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-sharepoint-tellius-release-1.8.0-v1` | 2026-03-31 | Initial production release on `python-connector-base-tellius-release-v2`. Full Mistral OCR support for PDF, DOCX, PPTX. Connector version: `source-microsoft-sharepoint 0.10.3`. |

---

## How to Release a New Version

### When to create a new version

- Parser code changed in `tellius-ocr-parser/`
- Dockerfile changed
- Dependencies updated
- Bug fix or feature addition

### Steps

1. **Make code changes** in `docker-images/tellius-ocr-parser/` or `docker-images/Dockerfile.python-connector-base-tellius`.

2. **Build the custom base image** (only if parser/base Dockerfile changed):
   ```bash
   cd <repo>/airbyte/docker-images

   docker buildx build --platform linux/amd64 --load \
     -t 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:<new-base-tag> \
     -f Dockerfile.python-connector-base-tellius .
   ```

3. **Build connector images** (all connectors that need the update):
   ```bash
   cd <repo>/airbyte/airbyte-integrations/connectors/<connector-dir>

   docker buildx build --platform linux/amd64 --no-cache --load \
     -t 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:<new-connector-tag> \
     -f <repo>/airbyte/docker-images/Dockerfile.python-connector \
     --build-arg BASE_IMAGE=956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:<new-base-tag> \
     --build-arg CONNECTOR_NAME=<connector-name> .
   ```

4. **Login to ECR and push**:
   ```bash
   aws ecr get-login-password --region us-east-1 | \
     docker login --username AWS --password-stdin 956938892320.dkr.ecr.us-east-1.amazonaws.com

   docker push 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:<new-base-tag>
   docker push 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:<new-connector-tag>
   ```

5. **Update Airbyte DB** to point to the new tag:
   ```sql
   UPDATE actor_definition_version
   SET docker_image_tag = '<new-connector-tag>'
   WHERE actor_definition_id = '<source-definition-id>';
   ```

6. **Update this document:**
   - Add a new row to the relevant changelog table(s) above
   - Update the "Current Production Images" table with the new tag and date
   - Commit and push

### Connector name reference

| Connector | `CONNECTOR_NAME` build arg | Connector directory |
|-----------|---------------------------|---------------------|
| Google Drive | `source-google-drive` | `source-google-drive` |
| GCS | `source-gcs` | `source-gcs` |
| Azure Blob Storage | `source-azure-blob-storage` | `source-azure-blob-storage` |
| S3 | `source-s3` | `source-s3` |
| SharePoint | `source-microsoft-sharepoint` | `source-microsoft-sharepoint` |

---

## Important Notes

- **Always build with `--platform linux/amd64`** — the K8s cluster runs amd64 nodes. Building on Apple Silicon without this flag will produce ARM images that fail to pull.
- **Never reuse a tag** — K8s nodes cache images by tag. Always increment the version suffix.
- **`CONNECTOR_NAME` build arg is required** — omitting it causes `exit code 127` at runtime.
- **Base image push is optional for local builds** — if you're building both base and connector locally, the connector build will use the local base image. Push the base to ECR only when you want it available independently or for CI/CD.
- **Fallback behavior** — if the middleware service is unreachable or `use_mistral_ocr` is `false`, the parser automatically falls back to Airbyte's default `UnstructuredParser` (unstructured.io). No manual intervention needed.
