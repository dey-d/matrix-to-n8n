# matrix-to-n8n

Forward messages from a Matrix room to an n8n webhook.

## What this is

`matrix-to-n8n` is a small Dockerized bridge that:

- logs in to a Matrix homeserver
- watches a single Matrix room for new message events
- forwards each new message to an n8n webhook as JSON
- stores sync state so it can resume cleanly after restarts
- uses long-polling `/sync` with backoff so it does not hammer the homeserver

It is designed for simple automation flows where Matrix is the source of events and n8n handles the downstream processing.

## Why this approach

This project is intentionally conservative:

- **Long polling instead of frequent polling** to avoid needless traffic
- **Event deduplication** so restarts do not re-forward the same message
- **Backoff and retry handling** for Matrix and webhook failures
- **State persisted in a volume** so the bridge can recover safely
- **Optional access token support** so you can avoid keeping a password around after first login

## Features

- Watches one configurable Matrix room
- Forwards only `m.room.message` events
- Skips the initial backlog on first run, then forwards new messages only
- Excludes the bridge user’s own messages by default to prevent loops
- Sends structured JSON to n8n
- Retries transient failures with exponential backoff
- Uses Docker Compose for easy deployment

## Repository layout

```text
.
├── app.py              # bridge implementation
├── docker-compose.yml   # compose stack
├── Dockerfile          # container image
├── requirements.txt    # Python dependencies
├── .env.example        # example configuration
└── README.md           # this file
```

## Prerequisites

- Docker
- Docker Compose
- A Matrix account with access to the target room
- An n8n webhook URL

## Quick start

1. Copy the example env file:

   ```bash
   cp .env.example .env
   ```

2. Edit `.env` and fill in the secret values.

3. Start the stack:

   ```bash
   docker compose up -d --build
   ```

4. Watch logs:

   ```bash
   docker compose logs -f
   ```

## Configuration

All runtime settings live in environment variables.

### Required variables

- `MATRIX_HOMESERVER_URL` — Matrix homeserver base URL
- `MATRIX_ROOM_ID` — room to watch
- `MATRIX_USER_ID` — Matrix login user ID
- `N8N_WEBHOOK_URL` — destination webhook URL

### Authentication variables

Use **one** of these:

- `MATRIX_PASSWORD` — password login for first run
- `MATRIX_ACCESS_TOKEN` — reusable token, preferred after you have one

### Other useful variables

- `MATRIX_DEVICE_NAME` — device label shown in Matrix
- `MATRIX_SYNC_TIMEOUT_MS` — long-poll timeout for `/sync`
- `MATRIX_FORWARD_OWN_MESSAGES` — set to `true` if you want to forward your own messages too
- `MATRIX_MAX_SEEN_EVENTS` — dedupe cache size
- `WEBHOOK_TIMEOUT_SECONDS` — timeout for webhook requests
- `WEBHOOK_MAX_RETRIES` — retry attempts for webhook failures
- `WEBHOOK_BACKOFF_INITIAL_SECONDS` — starting backoff delay
- `WEBHOOK_BACKOFF_MAX_SECONDS` — max backoff delay
- `LOG_LEVEL` — logging level

See `.env.example` for the full list and default values.

## Webhook payload

The bridge sends a JSON payload like this:

```json
{
  "source": "matrix",
  "matrix": {
    "homeserver": "https://matrix.example.com",
    "room_id": "!roomid:server",
    "sender": "@user:server",
    "event_id": "$event",
    "timestamp": "2026-06-21T12:34:56+00:00",
    "msgtype": "m.text",
    "body": "hello",
    "content": {},
    "raw_event": {}
  }
}
```

The important fields for n8n are usually:

- `matrix.body`
- `matrix.sender`
- `matrix.room_id`
- `matrix.event_id`
- `matrix.timestamp`

## Deployment notes

- The container stores its sync token and dedupe state in `/data`.
- The Compose file mounts `/data` as a named volume so restarts are safe.
- On the first run with no stored token, the bridge logs in with `MATRIX_PASSWORD`, saves the returned access token, and reuses it later.
- If you already have a valid `MATRIX_ACCESS_TOKEN`, you can set it and leave `MATRIX_PASSWORD` blank.

## Rate limiting and safety

This bridge is built to be gentle on the Matrix server:

- Uses `/sync` long polling rather than rapid polling loops
- Sleeps with exponential backoff on 429 and 5xx responses
- Adds jitter to retries so failures do not cause synchronized retry storms
- Stores the last `next_batch` token so it resumes instead of starting over
- Deduplicates event IDs so repeated deliveries are avoided after restarts
- Skips the initial backlog on first run to avoid flooding your webhook with old room history

## First run checklist

1. Set your values in `.env`
2. Start the stack with `docker compose up -d --build`
3. Check logs for a successful Matrix login
4. Send a test message in the watched room
5. Confirm n8n receives the webhook

## Updating configuration later

You can change any setting by editing `.env` and restarting the stack:

```bash
docker compose up -d
```

If you change the Matrix password, the bridge will only need it if the saved access token becomes invalid.

## Troubleshooting

### The bridge logs in but does not forward anything

- Confirm the room ID matches exactly
- Verify the account is actually joined to the room
- Send a brand-new message after the bridge starts
- Check whether `MATRIX_FORWARD_OWN_MESSAGES` is `false`

### The webhook is not receiving data

- Confirm `N8N_WEBHOOK_URL` is correct
- Look at `docker compose logs -f` for webhook errors
- Make sure your n8n workflow is listening on the right test/webhook path

### The Matrix server returns 429 or 5xx

- The bridge will retry automatically
- If the errors continue, wait a bit and inspect your homeserver health
- You can also raise the sync timeout slightly if needed

### I want to use an access token instead of a password

1. Log in once using `MATRIX_PASSWORD`
2. Let the bridge save the token to `/data/state.json`
3. Copy the token into `MATRIX_ACCESS_TOKEN`
4. Clear `MATRIX_PASSWORD` if you no longer want password-based startup

## Security notes

- Do not commit your real `.env` file
- Keep access tokens and passwords out of source control
- Rotate the token if you suspect it was exposed
- Use a dedicated Matrix account for automation if possible

## License

No explicit license is included yet. Add one before publishing publicly if needed.
