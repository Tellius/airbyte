# Production Image Registry — Tellius Custom Airbyte Connectors

This document is the **single source of truth** for all custom Airbyte connector images deployed in production. It tracks the current live image tag for each connector, the base image they depend on, and a changelog of what changed in each version.

> **How to use this document:**
>
> 1. **Before deploying:** Check the "Current Production Images" table below to see what's currently running.
> 2. **After building & pushing a new image:** Update this document with a new row in the relevant changelog table AND update the "Current Production Images" table to reflect the new active tag.
> 3. **Commit this file** alongside any connector/CDK code changes so the branch always reflects what's deployed.
> 4. **Tag naming convention:** `<component>-tellius-release-<airbyte-base-version>-v<increment>` — always increment the `v` suffix; never reuse a tag.

---

## Current Production Images

There are two ECR repositories in use:

| Repository | Used for |
|------------|----------|
| `956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte` | Python source connectors + custom base image |
| `956938892320.dkr.ecr.us-east-1.amazonaws.com/release/airbyte` | Kotlin/Java destination connectors |

### Python Source Connectors

| Component | Current Tag | Date Deployed | Based On |
|-----------|-------------|---------------|----------|
| **Custom Base Image** | `python-connector-base-tellius-release-v2` | 2026-04-13 | `docker.io/airbyte/python-connector-base:4.0.2` |
| **Google Drive** | `source-gDrive-tellius-release-1.8.0-v4` | 2026-04-22 | `python-connector-base-tellius-release-v2` |
| **GCS** | `source-gcs-tellius-release-1.8.0-v4` | 2026-04-22 | `python-connector-base-tellius-release-v2` |
| **Azure Blob Storage** | `source-azure-blob-tellius-release-1.8.0-v3` | 2026-04-22 | `python-connector-base-tellius-release-v2` |
| **S3** | `source-s3-tellius-release-1.8.0-v3` | 2026-04-22 | `python-connector-base-tellius-release-v2` |
| **SharePoint** | `source-sharepoint-tellius-release-1.8.0-v2` | 2026-04-22 | `python-connector-base-tellius-release-v2` |

### Manifest-Only Source Connectors

Declarative (YAML) connectors built on Airbyte's `source-declarative-manifest` runner — no custom Python base / OCR parser (these ingest API records, not files).

| Component | Current Tag | Date Deployed | Based On |
|-----------|-------------|---------------|----------|
| **Granola** | `source-granola-tellius-release-0.2.1-v1` | 2026-06-01 | `docker.io/airbyte/source-declarative-manifest:7.17.2` |

### Kotlin/Java Destination Connectors

| Component | Current Tag | Date Deployed | Based On |
|-----------|-------------|---------------|----------|
| **Destination S3** | `destination-s3-tellius-release-1.8.0-v7` | 2026-06-01 | `docker.io/airbyte/java-connector-base:2.0.1` |

---

## Image Lineage

```
docker.io/airbyte/python-connector-base:4.0.2         (Airbyte official)
         │
         ▼
python-connector-base-tellius-release-v2               (Custom base: tellius-ocr-parser + .pth patch)
         │
         ├──▶ source-gDrive-tellius-release-1.8.0-v4
         ├──▶ source-gcs-tellius-release-1.8.0-v4
         ├──▶ source-azure-blob-tellius-release-1.8.0-v3
         ├──▶ source-s3-tellius-release-1.8.0-v3
         └──▶ source-sharepoint-tellius-release-1.8.0-v2

docker.io/airbyte/source-declarative-manifest:7.17.2  (Airbyte official manifest runner)
         │
         ▼
source-granola-tellius-release-0.2.1-v1               (Manifest-only: adds web_url to notes/detailed_notes schema)

docker.io/airbyte/java-connector-base:2.0.1            (Airbyte official Java base)
         │
         ▼
destination-s3-tellius-release-1.8.0-v7               (Custom destination: DocumentMetadataCollector + MiddlewareApiClient)
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
| `source-gDrive-tellius-release-1.8.0-v4` | 2026-04-22 | **Document Metadata feature.** Adds `_ab_source_file_content_type`, `_ab_source_file_prefix_path`, `_ab_source_file_created_at`, `_ab_source_file_owner`, `_ab_source_file_last_modified_by`, `_ab_source_file_shared` to every emitted record via custom `GoogleDriveStream`. |

### GCS (`source-gcs-tellius-release-*`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-gcs-tellius-release-1.8.0-v1` | 2026-04-xx | Pre-production POC (not deployed). |
| `source-gcs-tellius-release-1.8.0-v2` | 2026-04-08 | Production release on `python-connector-base-tellius-release-v1`. Mistral OCR for PDFs. |
| `source-gcs-tellius-release-1.8.0-v3` | 2026-04-13 | Rebuilt on `python-connector-base-tellius-release-v2`. Adds native DOCX/PPTX Mistral OCR support. |
| `source-gcs-tellius-release-1.8.0-v4` | 2026-04-22 | **Document Metadata feature.** Adds `_ab_source_file_content_type`, `_ab_source_file_prefix_path`, `_ab_source_file_size`, `_ab_source_file_storage_class`, `_ab_source_file_created_at`, `_ab_source_file_custom_metadata` to every emitted record via custom `GCSStream`. |

### Azure Blob Storage (`source-azure-blob-tellius-release-*`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-azure-blob-tellius-release-1.8.0-v1` | 2026-04-08 | Initial production release on `python-connector-base-tellius-release-v1`. |
| `source-azure-blob-tellius-release-1.8.0-v2` | 2026-04-13 | Rebuilt on `python-connector-base-tellius-release-v2`. Adds native DOCX/PPTX Mistral OCR support. |
| `source-azure-blob-tellius-release-1.8.0-v3` | 2026-04-22 | **Document Metadata feature.** Adds `_ab_source_file_content_type`, `_ab_source_file_prefix_path`, `_ab_source_file_size`, `_ab_source_file_created_at`, `_ab_source_file_access_tier`, `_ab_source_file_blob_metadata`, `_ab_source_file_container_metadata`. Uses `read_records_from_slice()` override (CDK 4.x compatibility — `transform_record()` not available in this CDK version). |

### S3 (`source-s3-tellius-release-*`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-s3-tellius-release-1.8.0-v1` | 2026-04-08 | Initial production release on `python-connector-base-tellius-release-v1`. |
| `source-s3-tellius-release-1.8.0-v2` | 2026-04-13 | Rebuilt on `python-connector-base-tellius-release-v2`. Adds native DOCX/PPTX Mistral OCR support. |
| `source-s3-tellius-release-1.8.0-v3` | 2026-04-22 | **Document Metadata feature.** Adds `_ab_source_file_content_type`, `_ab_source_file_prefix_path`, `_ab_source_file_size`, `_ab_source_file_storage_class`, `_ab_source_file_user_metadata`, `_ab_source_file_object_tags`. Eliminates redundant `head_object()` API call by reusing headers from `get_object()` response (N+1 fix). |

### SharePoint (`source-sharepoint-tellius-release-*`)

| Tag | Date | Changes |
|-----|------|---------|
| `source-sharepoint-tellius-release-1.8.0-v1` | 2026-03-31 | Initial production release on `python-connector-base-tellius-release-v2`. Full Mistral OCR support for PDF, DOCX, PPTX. Connector version: `source-microsoft-sharepoint 0.10.3`. |
| `source-sharepoint-tellius-release-1.8.0-v2` | 2026-04-22 | **Document Metadata feature.** Extends `MicrosoftSharePointRemoteFile` with 8 new fields. Adds `_ab_source_file_content_type`, `_ab_source_file_prefix_path`, `_ab_source_file_size`, `_ab_source_file_created_at`, `_ab_source_file_owner`, `_ab_source_file_last_modified_by`, `_ab_source_file_site_name`, `_ab_source_file_library_name` via new `SharePointStream` (CDK ^6 `transform_record()` override). Email → displayName fallback for user identity fields. |

### Granola (`source-granola-tellius-release-*`)

> **ECR path:** `956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte`
> **Airbyte DB `actor_definition_id`:** `9023923c-002f-4131-9554-3ebdf56540a4`
> **Type:** manifest-only (declarative YAML) — not built on the custom Python base.

| Tag | Date | Changes |
|-----|------|---------|
| `source-granola-tellius-release-0.2.1-v1` | 2026-06-01 | Initial release. New manifest-only connector for Granola (Enterprise/Admin Notes API, `api_key` auth). Two streams: `notes` (light index — id/title/owner/created_at) and `detailed_notes` (rich — summary, transcript, attendees, calendar_event, folders). **Adds `web_url` to both stream schemas** so the Airbyte CDK does not strip Granola's per-note permalink (`https://notes.granola.ai/d/{uuid}`) — the CDK drops undeclared fields despite `additionalProperties: true`. Downstream, only `detailed_notes` is selected for ingestion. |

### Destination S3 (`destination-s3-tellius-release-*`)

> **ECR path:** `956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte`
> **Airbyte DB `actor_definition_id`:** `15f9bec3-347b-42db-9b50-2ddf76c766fe`

| Tag | Date | Changes |
|-----|------|---------|
| `destination-s3-tellius-release-1.8.0-v1` | 2026-xx-xx | Initial custom destination release. Adds `DocumentMetadataCollector` and `MiddlewareApiClient` to notify middleware on each document upload. |
| `destination-s3-tellius-release-1.8.0-v2` | 2026-xx-xx | Improved metadata extraction. Adds `document_fingerprint` (deterministic UUID for dedup across syncs) and `airbyte_sync_timestamp` to `meta_data`. |
| `destination-s3-tellius-release-1.8.0-v3` | 2026-xx-xx | Adds `source_file_id` and `source_redirect_uri` to `meta_data` (Google Drive / SharePoint file linking). |
| `destination-s3-tellius-release-1.8.0-v4` | 2026-04-21 | **Phase 2 — Full document metadata wiring.** Extracts all new `_ab_source_file_*` fields from Airbyte records and stores them in the middleware `meta_data` JSONB column: `content_type`, `prefix_path`, `created_at`, `owner`, `last_modified_by`, `shared`, `storage_class`, `access_tier`, `user_metadata`, `object_tags`, `blob_metadata`, `container_metadata`, `custom_metadata`. Also fixes `file_size_bytes` to use actual source file size (`_ab_source_file_size`) instead of serialized record size. |
| `destination-s3-tellius-release-1.8.0-v5` | 2026-04-22 | **SharePoint metadata wiring.** Adds extraction of `_ab_source_file_site_name` → `site_name` and `_ab_source_file_library_name` → `library_name` into `meta_data`. Image moved from `release/airbyte` to `airbyte` ECR repository. |
| `destination-s3-tellius-release-1.8.0-v6` | 2026-05-29 | **Slack metadata wiring.** Adds Slack-specific branch in `DocumentMetadataCollector` (`tryExtractFromSlackSource`) that produces middleware-notifiable `DocumentMetadata` for three streams: `channels`, `users`, and `channel_messages` (thread parents only). Each emits a real Slack permalink as `sourcePath` (`https://app.slack.com/...`), so links from Tellius UI deep-link back into Slack. Destination-side filters mirror the ingestion-side filters: skip deleted/bot/Slackbot users; skip system-event message subtypes (`channel_join`, `channel_leave`, `channel_archive`/`unarchive`, `channel_topic`/`purpose`/`name`, `bot_add`/`remove`); skip thread replies (parents only — replies are aggregated into the parent's UnifiedDocument by the airflow SlackExtractor). |
| `destination-s3-tellius-release-1.8.0-v7` | 2026-06-01 | **Granola metadata wiring.** Adds `tryExtractFromGranolaSource` branch in `DocumentMetadataCollector` (routed before the generic API extractor). Emits middleware-notifiable `DocumentMetadata` only for `detailed_notes` records (rich content: `summary_markdown`/`summary_text`/`transcript`/`attendees`); skips the light `notes` index stream (used by the airflow GranolaExtractor as enrichment only). `type = granola_note`; `sourcePath` resolves to the real Granola permalink (`https://notes.granola.ai/d/{uuid}`) via the `web_url` field declared in the source manifest. |

---

## How to Release a New Version

---

### Python Source Connectors (GDrive, GCS, S3, Azure, SharePoint)

#### When to create a new version

- Parser code changed in `tellius-ocr-parser/`
- Source connector Python code changed (e.g., new metadata fields in `stream.py`, `stream_reader.py`)
- Dockerfile or dependencies updated

#### Steps

1. **Make code changes** in `docker-images/tellius-ocr-parser/` or the connector's Python source files.

2. **Build the custom base image** (only if parser/base Dockerfile changed):
   ```bash
   cd /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/docker-images

   docker buildx build --platform linux/amd64 --load \
     -t 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:<new-base-tag> \
     -f Dockerfile.python-connector-base-tellius .
   ```

3. **Build connector image:**
   ```bash
   cd /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/airbyte-integrations/connectors/<connector-dir>

   docker buildx build --platform linux/amd64 --no-cache --load \
     -t 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:<new-connector-tag> \
     -f /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/docker-images/Dockerfile.python-connector \
     --build-arg BASE_IMAGE=956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:<new-base-tag> \
     --build-arg CONNECTOR_NAME=<connector-name> .
   ```

4. **Login to ECR and push:**
   ```bash
   aws ecr get-login-password --region us-east-1 | \
     docker login --username AWS --password-stdin 956938892320.dkr.ecr.us-east-1.amazonaws.com

   docker push 956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte:<new-connector-tag>
   ```

5. **Update Airbyte DB:**
   ```sql
   UPDATE actor_definition_version
   SET docker_image_tag = '<new-connector-tag>'
   WHERE actor_definition_id = '<source-definition-id>';
   ```

6. **Update this document** and commit.

#### Python source connector name reference

| Connector | `CONNECTOR_NAME` build arg | Connector directory |
|-----------|---------------------------|---------------------|
| Google Drive | `source-google-drive` | `source-google-drive` |
| GCS | `source-gcs` | `source-gcs` |
| Azure Blob Storage | `source-azure-blob-storage` | `source-azure-blob-storage` |
| S3 | `source-s3` | `source-s3` |
| SharePoint | `source-microsoft-sharepoint` | `source-microsoft-sharepoint` |

---

### Kotlin/Java Destination Connector (destination-s3)

The destination connector is a **Kotlin/Gradle project** built with the Airbyte bulk CDK. The build process is two-stage: Gradle compiles and packages the JAR, then Docker wraps it.

#### When to create a new version

- CDK code changed (`DocumentMetadataCollector.kt`, `MiddlewareApiClient.kt`, `DocumentUploadNotifier.kt`, `ObjectLoaderPartFormatter.kt`)
- Destination connector Kotlin source changed under `airbyte-integrations/connectors/destination-s3/src/`
- Bug fix or new metadata field wiring

#### Key files

| File | Purpose |
|------|---------|
| `airbyte-cdk/bulk/toolkits/load-object-storage/src/main/kotlin/io/airbyte/cdk/load/file/object_storage/DocumentMetadataCollector.kt` | Extracts metadata from each Airbyte record |
| `airbyte-cdk/bulk/toolkits/load-object-storage/src/main/kotlin/io/airbyte/cdk/load/file/object_storage/MiddlewareApiClient.kt` | Builds the API payload and POSTs to middleware |
| `airbyte-cdk/bulk/toolkits/load-object-storage/src/main/kotlin/io/airbyte/cdk/load/file/object_storage/DocumentUploadNotifier.kt` | Async wrapper that calls `MiddlewareApiClient` per record |
| `airbyte-cdk/bulk/toolkits/load-object-storage/src/main/kotlin/io/airbyte/cdk/load/pipline/object_storage/ObjectLoaderPartFormatter.kt` | Invokes `DocumentMetadataExtractor` for each incoming record |
| `airbyte-integrations/connectors/destination-s3/build/airbyte/docker/Dockerfile` | Dockerfile for the destination (auto-generated by Gradle) |
| `docker-images/Dockerfile.java-connector-non-airbyte-ci` | Dockerfile used for the manual ECR build |

#### Steps

**Step 1 — Gradle build** (compiles CDK + destination, produces `build/airbyte/docker/airbyte-app.tar`):

```bash
cd /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte

export JAVA_HOME=/opt/homebrew/Cellar/openjdk@21/21.0.9/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH

./gradlew clean \
  :airbyte-cdk:bulk:build \
  :airbyte-integrations:connectors:destination-s3:build \
  -x integrationTestJava \
  -x test
```

> This compiles the entire bulk CDK (including your changes to `DocumentMetadataCollector.kt` and `MiddlewareApiClient.kt`) and packages the destination as a TAR at `airbyte-integrations/connectors/destination-s3/build/airbyte/docker/airbyte-app.tar`.

**Step 2 — Docker build** (wraps the TAR in the Java base image):

```bash
cd /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/airbyte-integrations/connectors/destination-s3

docker buildx build \
  --platform linux/amd64 \
  --no-cache \
  --load \
  --label io.airbyte.app=destination-s3 \
  --label io.airbyte.version=dev \
  --build-arg VERSION=dev \
  --tag airbyte/destination-s3:dev-amd64 \
  --tag 956938892320.dkr.ecr.us-east-1.amazonaws.com/release/airbyte:<new-tag> \
  --file /Users/harshkhetrapal/Desktop/Tellius_Code/airbyte_codes/airbyte_forked_tellius/airbyte/docker-images/Dockerfile.java-connector-non-airbyte-ci \
  --build-arg BASE_IMAGE=docker.io/airbyte/java-connector-base:2.0.1@sha256:ec89bd1a89e825514dd2fc8730ba299a3ae1544580a078df0e35c5202c2085b3 \
  --build-arg CONNECTOR_NAME=destination-s3 \
  build/airbyte/docker
```

> Replace `<new-tag>` with the next version, e.g., `destination-s3-tellius-release-1.8.0-v5`.
> The build context is `build/airbyte/docker` (relative to the connector directory) — this is where the Gradle TAR is placed.

**Step 3 — ECR login and push:**

```bash
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin 956938892320.dkr.ecr.us-east-1.amazonaws.com

docker push 956938892320.dkr.ecr.us-east-1.amazonaws.com/release/airbyte:<new-tag>
```

**Step 4 — Update Airbyte DB** to point the custom destination definition at the new image:

```bash
kubectl exec -n unstructuredkaiya airbyte-db-0 -- psql -U airbyte -d db-airbyte -c \
  "UPDATE actor_definition_version
   SET docker_image_tag = '<new-tag>'
   WHERE actor_definition_id = '15f9bec3-347b-42db-9b50-2ddf76c766fe'::uuid"
```

Also ensure the required flags are set (only needed once, but safe to re-run):

```bash
kubectl exec -n unstructuredkaiya airbyte-db-0 -- psql -U airbyte -d db-airbyte -c \
  "UPDATE actor_definition_version
   SET supports_refreshes = true, supports_file_transfer = true
   WHERE actor_definition_id = '15f9bec3-347b-42db-9b50-2ddf76c766fe'::uuid"
```

Verify:

```bash
kubectl exec -n unstructuredkaiya airbyte-db-0 -- psql -U airbyte -d db-airbyte -t -c \
  "SELECT docker_image_tag, supports_refreshes, supports_file_transfer
   FROM actor_definition_version
   WHERE actor_definition_id = '15f9bec3-347b-42db-9b50-2ddf76c766fe'::uuid"
```

**Step 5 — Restart Airbyte server and worker** to pick up the new image tag:

```bash
kubectl rollout restart deployment airbyte-server -n unstructuredkaiya
kubectl rollout restart deployment airbyte-worker -n unstructuredkaiya

kubectl rollout status deployment airbyte-server -n unstructuredkaiya --timeout=120s
kubectl rollout status deployment airbyte-worker -n unstructuredkaiya --timeout=120s
```

**Step 6 — Update this document** and commit.

---

## Important Notes

- **Always build with `--platform linux/amd64`** — the K8s cluster runs amd64 nodes. Building on Apple Silicon without this flag will produce ARM images that fail to pull.
- **Never reuse a tag** — K8s nodes cache images by tag. Always increment the `v` suffix.
- **`CONNECTOR_NAME` build arg is required** for both Python and Java connectors — omitting it causes `exit code 127` at runtime.
- **Python source connectors** use ECR path `956938892320.dkr.ecr.us-east-1.amazonaws.com/airbyte`.
- **Kotlin destination connector** uses ECR path `956938892320.dkr.ecr.us-east-1.amazonaws.com/release/airbyte`.
- **Gradle must run before Docker** for the destination — the Docker build consumes `build/airbyte/docker/airbyte-app.tar` produced by Gradle. Running Docker alone without a fresh Gradle build will package stale code.
- **Fallback behavior** — if the middleware service is unreachable, `MiddlewareApiClient` logs the error and returns `false` — the sync itself is never blocked.
