# Telefetch

A Telegram bot that downloads and re-uploads media files via Saved Messages.

## Heroku Deployment

### 1. Create the app

```bash
heroku create your-app-name
```

### 2. Set config vars (never commit `.env` to git)

```bash
heroku config:set API_ID=your_api_id
heroku config:set API_HASH=your_api_hash
heroku config:set SESSION_STRING=your_session_string
heroku config:set DOWNLOAD_CONCURRENCY=2
heroku config:set UPLOAD_CONCURRENCY=1
heroku config:set STATS_TTL_SECONDS=30
heroku config:set LOG_LEVEL=INFO
```

> **Note:** `TEMP_DIR` and `DATABASE_PATH` intentionally default to `/tmp/telefetch/…`
> on Heroku, because Heroku dynos have an ephemeral (read-only slug) filesystem.
> Jobs in progress will be lost on dyno restart — this is expected.

### 3. Deploy

```bash
git push heroku main
```

### 4. Scale the worker dyno

Telefetch runs as a **worker** (no HTTP server), so you need to scale the
worker dyno and turn off the (non-existent) web dyno:

```bash
heroku ps:scale web=0 worker=1
```

### 5. View logs

```bash
heroku logs --tail
```

---

## Local Development

Copy `.env.example` to `.env` and fill in your credentials, then:

```bash
pip install -r requirements.txt
python -m telefetch
```

## Generating a Session String

Use [Pyrogram's session string generator](https://t.me/PyrogramBot) or:

```python
from pyrogram import Client
async with Client("gen", api_id=API_ID, api_hash=API_HASH) as app:
    print(await app.export_session_string())
```
