# wa-agents

A Python API for my WhatsApp chatbots apps

## Environment Variables

The following subsections list the env vars that need to be in an app's `.env` file for it to work with `wa-agents`.

### Digital Ocean Spaces S3 Bucket

|                     |     |
| ------------------- | --- |
| `BUCKET_NAME`       | Bucket Name                  |
| `BUCKET_REGION`     | Bucket Region (e.g., `atl1`) |
| `BUCKET_KEY`        | Bucket Access Key ID         |
| `BUCKET_KEY_SECRET` | Bucket Secret Access Key     |

### LLM API

At least one of the following:
* `OPENROUTER_API_KEY`
* `OPENAI_API_KEY`
* `MISTRAL_API_KEY`

### Queue Database

* `QUEUE_DB_NAME` (Optional. Default value is `queue.sqlite3`.)

### WhatsApp

* `WA_TOKEN`
* `WA_VERIFY_TOKEN`
