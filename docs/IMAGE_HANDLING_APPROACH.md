# Mistral OCR — Image Handling & Storage Approach

This document covers how Mistral OCR handles images in documents, the available options for image extraction, and the recommended approach for storing extracted images in production.

---

## How Mistral OCR Handles Images Today

### Current Behavior (No Image Extraction)

Our `CustomOCRParser` calls the Mistral OCR API with basic parameters — no image extraction flag is set.

When Mistral OCR encounters an image in a PDF page, it:

1. **Detects the image** and its position on the page
2. **Inserts a markdown placeholder** in the output: `![img-0001.jpeg](img-0001.jpeg)`
3. **Does NOT return the actual image data** — just the placeholder reference

Example markdown output for a page with a chart:

```markdown
## Revenue Overview

The following chart shows quarterly revenue trends:

![img-0005.jpeg](img-0005.jpeg)

As shown above, revenue increased by 15% year-over-year.
```

All text around the image is fully extracted, but the image itself is a dead reference.

### With `include_image_base64: true`

Mistral OCR supports a flag called `include_image_base64`. When enabled:

1. **Mistral extracts every image** from the document pages
2. **The API response** includes an `images` array for each page containing:
   - `id` — the image reference ID (e.g., `img-0001.jpeg`)
   - `image_base64` — the full base64-encoded image data (e.g., `data:image/jpeg;base64,/9j/4AAQ...`)
3. **The markdown** still has the `![img-0001.jpeg](img-0001.jpeg)` placeholder
4. **Post-processing** is needed to replace those placeholders with actual image URLs

API request with image extraction:

```json
{
    "model": "mistral-ocr-latest",
    "document": {
        "type": "document_url",
        "document_url": "data:application/pdf;base64,..."
    },
    "include_image_base64": true
}
```

API response with images:

```json
{
    "pages": [
        {
            "index": 0,
            "markdown": "Some text\n\n![img-0001.jpeg](img-0001.jpeg)\n\nMore text",
            "images": [
                {
                    "id": "img-0001.jpeg",
                    "image_base64": "data:image/jpeg;base64,/9j/4AAQSkZJRg..."
                }
            ]
        }
    ]
}
```

---

## Image Storage Options

Once images are extracted via `include_image_base64: true`, we need to decide what to do with them.

### Option A: Inline Base64 in Content

Replace markdown references with data URIs directly in the text content:

```markdown
Some text

![img-0001.jpeg](data:image/jpeg;base64,/9j/4AAQSkZJRg...)

More text
```

| Pros | Cons |
|------|------|
| Self-contained — no external dependencies | Massively inflates record size (single image = 100KB–1MB+ in base64) |
| Simple implementation | Could hit Airbyte record size limits |
| No storage infrastructure needed | Increases S3 destination storage costs |
| | Downstream consumers must parse base64 |

### Option B: Store Images to S3 (Recommended)

Upload extracted images to S3 and replace placeholders with S3 URLs:

```markdown
Some text

![img-0001.jpeg](https://tellius-airbyte.s3.us-east-1.amazonaws.com/ocr-images/doc123/img-0001.jpeg)

More text
```

| Pros | Cons |
|------|------|
| Clean content, small record size | Requires S3 write access from source container |
| Images accessible via standard URLs | Need image lifecycle management (cleanup) |
| Consumers can fetch images on demand | Additional S3 API calls during parsing |
| Leverages existing S3 infrastructure | |

### Option C: Drop Image Placeholders

Strip all `![...](...)` placeholders from the markdown, keeping only text:

| Pros | Cons |
|------|------|
| Cleanest text output | Image content is permanently lost |
| Simplest implementation | Charts, diagrams, visual data inaccessible |
| No storage overhead | |

**Recommendation: Option B (Store to S3)** — provides the best balance of clean content and image accessibility.

---

## Why We Can't Use the Destination S3 Directly

Airbyte's replication pod has three separate containers:

```
┌─────────────────────────────────────────────────────┐
│                 Replication Pod                       │
│                                                      │
│  ┌──────────┐    ┌──────────────┐    ┌────────────┐ │
│  │  Source   │ →  │ Orchestrator │ →  │Destination │ │
│  │Container │    │              │    │ Container  │ │
│  │          │    │              │    │            │ │
│  │ CustomOCR│    │  (pipes      │    │ S3 Writer  │ │
│  │ Parser   │    │   records)   │    │            │ │
│  │ runs     │    │              │    │ Has S3     │ │
│  │ HERE     │    │              │    │ creds      │ │
│  └──────────┘    └──────────────┘    └────────────┘ │
└─────────────────────────────────────────────────────┘
```

- Our parser runs in the **source container**
- S3 destination credentials live in the **destination container**
- These are **completely separate processes** — the source has zero visibility into the destination's configuration
- Airbyte's data flow is strictly `Source → Records → Destination`. Having the source write directly to the destination's storage bypasses this separation

**Solution:** Provide the parser with independent S3 access — configured through the Kaiya unstructured service.

---

## Recommended Architecture: S3 Config from Kaiya Service

### Why Kaiya Service (Not Middleware)

The **Kaiya unstructured service** (`kaiya-unstructured-service:8080`) is the service that creates Airbyte sources, destinations, and connections. It already knows the S3 destination details because it configured them.

| Aspect | From Middleware | From Kaiya Service |
|--------|----------------|-------------------|
| Source of truth | Config duplication — must manually sync with destination | Single source of truth — same config used to create the destination |
| Airbyte awareness | General purpose, no Airbyte knowledge | Knows the exact S3 destination details |
| Credentials | S3 creds must be duplicated in middleware config | Can derive from the same credentials used for the Airbyte S3 destination |
| Maintenance | Two places to update if bucket/creds change | One place — change in Kaiya, image storage follows |

### Proposed Config Flow

```
Parser startup (per sync):

  1. GET kaiya-service/internal-auth-token
     → auth token                                    [EXISTING]

  2. GET middleware/unstructureMistralOcrConfig
     → use_mistral_ocr, mistral_api_key              [EXISTING]

  3. GET kaiya-service/<new-endpoint>
     → S3 bucket, prefix, region, credentials         [NEW]
```

### Proposed Kaiya Service Endpoint

```
GET /api/unstructured/image-storage-config
Authorization: <auth-token>
```

Response:

```json
{
    "enabled": true,
    "s3_bucket": "tellius-airbyte",
    "s3_prefix": "ocr-images/",
    "s3_region": "us-east-1",
    "aws_access_key_id": "AKIA...",
    "aws_secret_access_key": "..."
}
```

- `enabled` — toggle to enable/disable image extraction and storage (default `false`)
- `s3_bucket` — the S3 bucket to store images in (can be the same as the destination bucket)
- `s3_prefix` — path prefix to keep images separated from destination JSONL output
- `s3_region` — AWS region for the bucket
- `aws_access_key_id` / `aws_secret_access_key` — S3 credentials for writing images

### S3 Image Path Convention

```
s3://<bucket>/<prefix>/<document_hash>/<image_id>

Example:
s3://tellius-airbyte/ocr-images/4009b301-525e-3411-8cbf-0be097bcb49b/img-0001.jpeg
s3://tellius-airbyte/ocr-images/4009b301-525e-3411-8cbf-0be097bcb49b/img-0002.jpeg
```

- `document_hash` — derived from the source file path (same fingerprint logic we already use for middleware document notifications)
- `image_id` — the ID returned by Mistral OCR (e.g., `img-0001.jpeg`)

---

## End-to-End Flow (with Image Storage)

```
1. Sync starts → parser fetches config:
   - Middleware: use_mistral_ocr=true, api_key=***
   - Kaiya: image_storage enabled, bucket=tellius-airbyte, prefix=ocr-images/

2. Parser processes a PDF file:
   a. Read file bytes from source (GCS/S3/Azure/GDrive)
   b. Call Mistral OCR API with include_image_base64=true
   c. Receive markdown + images array

3. For each image in the response:
   a. Decode the base64 image data
   b. Upload to S3: s3://tellius-airbyte/ocr-images/<doc_hash>/<img_id>
   c. Generate the S3 URL

4. Post-process the markdown:
   - Replace ![img-0001.jpeg](img-0001.jpeg)
   - With    ![img-0001.jpeg](https://tellius-airbyte.s3.us-east-1.amazonaws.com/ocr-images/<doc_hash>/img-0001.jpeg)

5. Yield the record with image-enriched content
   → Orchestrator pipes it → S3 destination writes the JSONL
```

---

## Impact on Existing Components

| Component | Change Required |
|-----------|----------------|
| **Kaiya unstructured service** | New endpoint: `/api/unstructured/image-storage-config` returning S3 bucket, prefix, region, credentials |
| **CustomOCRParser (`parser.py`)** | 1. Fetch image storage config from Kaiya at startup (cached per sync) |
| | 2. Pass `include_image_base64: true` to Mistral OCR API when image storage is enabled |
| | 3. Upload extracted images to S3 using `boto3` |
| | 4. Replace markdown placeholders with S3 URLs |
| **Custom base image** | Add `boto3` as a dependency in `pyproject.toml` |
| **Middleware service** | No changes — continues to provide `use_mistral_ocr` and `mistral_api_key` |
| **S3 destination bucket** | No changes — images are stored under a separate prefix (`ocr-images/`) |
| **Downstream consumers (Kaiya RAG)** | Need read access to the image S3 path to render/process images |

---

## Cost Considerations

### Additional Mistral OCR Cost

Enabling `include_image_base64` does **not** change the Mistral OCR API pricing — the cost is per page processed, regardless of whether images are extracted. No additional cost from Mistral.

### S3 Storage Cost

| Factor | Estimate |
|--------|----------|
| Average image size | 50KB–500KB per image |
| Average images per document | 5–20 for reports with charts/tables |
| S3 storage cost | $0.023/GB/month (S3 Standard) |
| S3 PUT request cost | $0.005 per 1,000 requests |
| Example: 1,000 documents × 10 images × 200KB avg | ~2GB storage = ~$0.05/month |

S3 storage cost for images is negligible compared to Mistral OCR API costs.

### API Latency Impact

Enabling `include_image_base64` increases the Mistral OCR response payload size (base64 images included in response). Expected impact:

- Response size increases proportional to number/size of images in the document
- Additional S3 upload time per image (~50–200ms per image depending on size)
- Overall per-document parsing time may increase by 1–5 seconds for image-heavy documents

---

## Open Questions for Team Discussion

1. **Image storage toggle** — Should this be a global toggle (all sources) or per-source? Currently leaning global (same as `use_mistral_ocr`).

2. **Same bucket vs. separate bucket** — Using the same S3 bucket (`tellius-airbyte`) with a different prefix (`ocr-images/`) is simpler. A separate bucket provides better isolation but adds infrastructure.

3. **Image lifecycle** — When a document is re-synced or deleted, should the corresponding images be cleaned up? If yes, who handles cleanup — the parser on re-sync, or a separate cleanup job?

4. **Image access from Kaiya** — Does Kaiya's RAG pipeline need to access/render these images? If yes, does it already have read access to the S3 bucket?

5. **Which file types need image extraction?** — PDFs are the primary use case. DOCX and PPTX images are currently extracted as text only (via python-docx/python-pptx). Should we extract embedded images from Office formats too?

6. **Do we need this now?** — Current text extraction quality is high (validated across all four connectors). Image storage adds complexity. Is there a concrete use case driving this requirement (e.g., chart understanding, visual Q&A)?
