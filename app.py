import asyncio
import json
import logging
import os
import random
import signal
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

LOG = logging.getLogger("matrix-to-n8n")


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


class Config:
    def __init__(self) -> None:
        self.homeserver_url = env("MATRIX_HOMESERVER_URL").rstrip("/")
        self.room_id = env("MATRIX_ROOM_ID")
        self.user_id = env("MATRIX_USER_ID")
        self.password = os.getenv("MATRIX_PASSWORD", "")
        self.access_token = os.getenv("MATRIX_ACCESS_TOKEN", "").strip() or None
        self.device_name = os.getenv("MATRIX_DEVICE_NAME", "matrix-to-n8n")
        self.webhook_url = env("N8N_WEBHOOK_URL")
        self.state_dir = Path(os.getenv("MATRIX_STATE_DIR", "/data"))
        self.state_file = self.state_dir / "state.json"
        self.sync_timeout_ms = int(os.getenv("MATRIX_SYNC_TIMEOUT_MS", "30000"))
        self.forward_own_messages = os.getenv("MATRIX_FORWARD_OWN_MESSAGES", "false").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self.webhook_timeout_seconds = float(os.getenv("WEBHOOK_TIMEOUT_SECONDS", "30"))
        self.webhook_max_retries = int(os.getenv("WEBHOOK_MAX_RETRIES", "5"))
        self.webhook_backoff_initial = float(os.getenv("WEBHOOK_BACKOFF_INITIAL_SECONDS", "1"))
        self.webhook_backoff_max = float(os.getenv("WEBHOOK_BACKOFF_MAX_SECONDS", "30"))
        self.max_seen_events = int(os.getenv("MATRIX_MAX_SEEN_EVENTS", "2000"))


class State:
    def __init__(self, config: Config) -> None:
        self.path = config.state_file
        self.sync_token: str | None = None
        self.access_token: str | None = config.access_token
        self.device_id: str | None = None
        self.seen_events: deque[str] = deque(maxlen=config.max_seen_events)
        self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(self.path.read_text())
        except FileNotFoundError:
            return
        except Exception as exc:  # pragma: no cover - defensive logging
            LOG.warning("Could not load state file %s: %s", self.path, exc)
            return

        self.sync_token = raw.get("sync_token") or None
        self.access_token = raw.get("access_token") or self.access_token
        self.device_id = raw.get("device_id") or None
        for event_id in raw.get("seen_events", []):
            if isinstance(event_id, str) and event_id:
                self.seen_events.append(event_id)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "sync_token": self.sync_token,
            "access_token": self.access_token,
            "device_id": self.device_id,
            "seen_events": list(self.seen_events),
        }
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
        tmp.replace(self.path)

    def mark_seen(self, event_id: str) -> None:
        if event_id in self.seen_events:
            return
        self.seen_events.append(event_id)

    def has_seen(self, event_id: str) -> bool:
        return event_id in self.seen_events


class MatrixBridge:
    def __init__(self, config: Config, state: State) -> None:
        self.config = config
        self.state = state
        self.session: aiohttp.ClientSession | None = None
        self.stop_event = asyncio.Event()

    async def run(self) -> None:
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.webhook_timeout_seconds + 20),
            raise_for_status=False,
            headers={"User-Agent": "matrix-to-n8n/1.0"},
        )
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except NotImplementedError:
                pass

        try:
            await self.ensure_authenticated()
            await self.sync_forever()
        finally:
            if self.session:
                await self.session.close()

    async def ensure_authenticated(self) -> None:
        if self.state.access_token:
            try:
                await self.whoami()
                LOG.info("Reusing stored Matrix access token")
                return
            except Exception as exc:
                LOG.warning("Stored access token unusable; will re-login: %s", exc)

        if not self.config.password:
            raise SystemExit(
                "MATRIX_PASSWORD is required on first run when MATRIX_ACCESS_TOKEN is not set"
            )

        payload = {
            "type": "m.login.password",
            "identifier": {"type": "m.id.user", "user": self.config.user_id},
            "password": self.config.password,
            "initial_device_display_name": self.config.device_name,
        }
        data = await self.matrix_request(
            "POST",
            "/_matrix/client/v3/login",
            json_data=payload,
            auth=False,
            retries=5,
        )
        self.state.access_token = data["access_token"]
        self.state.device_id = data.get("device_id")
        self.state.save()
        LOG.info("Logged into Matrix as %s", self.config.user_id)

    async def whoami(self) -> dict[str, Any]:
        return await self.matrix_request("GET", "/_matrix/client/v3/account/whoami", auth=True)

    def auth_headers(self) -> dict[str, str]:
        if not self.state.access_token:
            return {}
        return {"Authorization": f"Bearer {self.state.access_token}"}

    async def matrix_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_data: dict[str, Any] | None = None,
        auth: bool = True,
        retries: int = 6,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        assert self.session is not None
        url = f"{self.config.homeserver_url}{path}"
        backoff = self.config.webhook_backoff_initial
        last_retryable: RetryableMatrixError | None = None
        last_network_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                async with self.session.request(
                    method,
                    url,
                    params=params,
                    json=json_data,
                    headers=self.auth_headers() if auth else None,
                    timeout=aiohttp.ClientTimeout(total=timeout) if timeout else None,
                ) as resp:
                    text = await resp.text()
                    if 200 <= resp.status < 300:
                        return json.loads(text) if text else {}
                    if resp.status == 429:
                        retry_after = self.retry_after_seconds(text)
                        raise RetryableMatrixError(f"Matrix rate limit hit; retrying in {retry_after:.1f}s", retry_after)
                    if resp.status in {500, 502, 503, 504}:
                        raise RetryableMatrixError(f"Matrix server error {resp.status}")
                    if resp.status == 401 and auth:
                        raise UnauthorizedError(text)
                    raise RuntimeError(f"Matrix request failed: {resp.status} {text}")
            except UnauthorizedError:
                raise
            except RetryableMatrixError as exc:
                last_retryable = exc
                sleep_for = min(exc.retry_after or backoff, self.config.webhook_backoff_max)
                sleep_for += random.uniform(0, 0.3)
                LOG.warning("%s (attempt %s/%s)", exc, attempt, retries)
                if attempt < retries:
                    await asyncio.sleep(sleep_for)
                    backoff = min(backoff * 2, self.config.webhook_backoff_max)
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
                last_network_error = exc
                if attempt >= retries:
                    break
                sleep_for = min(backoff, self.config.webhook_backoff_max)
                sleep_for += random.uniform(0, 0.3)
                LOG.warning("Matrix request error: %s (attempt %s/%s)", exc, attempt, retries)
                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, self.config.webhook_backoff_max)
        if last_retryable is not None:
            raise last_retryable
        if last_network_error is not None:
            raise last_network_error
        raise RuntimeError(f"Matrix request failed after {retries} retries: {method} {path}")

    @staticmethod
    def retry_after_seconds(response_text: str) -> float:
        try:
            data = json.loads(response_text)
            retry_after_ms = float(data.get("retry_after_ms", 0))
            if retry_after_ms > 0:
                return retry_after_ms / 1000.0
        except Exception:
            pass
        return 5.0

    async def sync_forever(self) -> None:
        assert self.session is not None
        backoff = self.config.webhook_backoff_initial
        while not self.stop_event.is_set():
            bootstrap = self.state.sync_token is None
            params = {"timeout": str(self.config.sync_timeout_ms)}
            if self.state.sync_token:
                params["since"] = self.state.sync_token
            try:
                data = await self.matrix_request(
                    "GET",
                    "/_matrix/client/v3/sync",
                    params=params,
                    auth=True,
                    retries=1,
                    timeout=(self.config.sync_timeout_ms / 1000.0) + 20,
                )
                if bootstrap:
                    self.state.sync_token = data.get("next_batch", self.state.sync_token)
                    self.state.save()
                    LOG.info("Initial Matrix sync completed; backlog skipped and future messages will be forwarded")
                else:
                    await self.process_sync(data)
                    self.state.sync_token = data.get("next_batch", self.state.sync_token)
                    self.state.save()
                backoff = self.config.webhook_backoff_initial
            except UnauthorizedError:
                LOG.warning("Matrix token expired; re-authenticating")
                self.state.access_token = None
                self.state.save()
                await self.ensure_authenticated()
            except RetryableMatrixError as exc:
                sleep_for = min(exc.retry_after or backoff, self.config.webhook_backoff_max)
                sleep_for += random.uniform(0, 0.5)
                LOG.warning("Sync rate limited or temporarily unavailable: %s", exc)
                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, self.config.webhook_backoff_max)
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
                LOG.warning("Sync loop error: %s", exc)
                sleep_for = min(backoff, self.config.webhook_backoff_max)
                sleep_for += random.uniform(0, 0.5)
                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, self.config.webhook_backoff_max)

    async def process_sync(self, data: dict[str, Any]) -> None:
        rooms = data.get("rooms", {}).get("join", {})
        room = rooms.get(self.config.room_id)
        if not room:
            return

        timeline = room.get("timeline", {}).get("events", [])
        for event in timeline:
            if event.get("type") != "m.room.message":
                continue
            event_id = event.get("event_id")
            if not event_id or self.state.has_seen(event_id):
                continue
            if not self.config.forward_own_messages and event.get("sender") == self.config.user_id:
                self.state.mark_seen(event_id)
                self.state.save()
                continue

            payload = self.build_webhook_payload(event)
            await self.post_webhook(payload)
            self.state.mark_seen(event_id)
            self.state.save()
            LOG.info(
                "Forwarded message from %s in %s (%s)",
                event.get("sender"),
                self.config.room_id,
                event_id,
            )

    def build_webhook_payload(self, event: dict[str, Any]) -> dict[str, Any]:
        content = event.get("content", {})
        body = content.get("body") or content.get("formatted_body") or ""
        timestamp = event.get("origin_server_ts")
        timestamp_iso = None
        if isinstance(timestamp, (int, float)):
            timestamp_iso = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).isoformat()

        return {
            "source": "matrix",
            "matrix": {
                "homeserver": self.config.homeserver_url,
                "room_id": self.config.room_id,
                "sender": event.get("sender"),
                "event_id": event.get("event_id"),
                "timestamp": timestamp_iso,
                "msgtype": content.get("msgtype"),
                "body": body,
                "content": content,
                "raw_event": event,
            },
        }

    async def post_webhook(self, payload: dict[str, Any]) -> None:
        assert self.session is not None
        backoff = self.config.webhook_backoff_initial
        for attempt in range(1, self.config.webhook_max_retries + 1):
            try:
                async with self.session.post(
                    self.config.webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=self.config.webhook_timeout_seconds),
                ) as resp:
                    text = await resp.text()
                    if 200 <= resp.status < 300:
                        return
                    if resp.status == 429:
                        retry_after = self.retry_after_seconds(text)
                        raise RetryableMatrixError(
                            f"Webhook rate limited; retrying in {retry_after:.1f}s",
                            retry_after,
                        )
                    if resp.status in {500, 502, 503, 504}:
                        raise RetryableMatrixError(f"Webhook server error {resp.status}")
                    raise RuntimeError(f"Webhook rejected payload: {resp.status} {text}")
            except RetryableMatrixError as exc:
                sleep_for = min(exc.retry_after or backoff, self.config.webhook_backoff_max)
                sleep_for += random.uniform(0, 0.3)
                LOG.warning("%s (attempt %s/%s)", exc, attempt, self.config.webhook_max_retries)
                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, self.config.webhook_backoff_max)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self.config.webhook_max_retries:
                    raise
                sleep_for = min(backoff, self.config.webhook_backoff_max)
                sleep_for += random.uniform(0, 0.3)
                LOG.warning("Webhook request error: %s (attempt %s/%s)", exc, attempt, self.config.webhook_max_retries)
                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, self.config.webhook_backoff_max)


class UnauthorizedError(Exception):
    pass


class RetryableMatrixError(Exception):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


async def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = Config()
    state = State(config)
    bridge = MatrixBridge(config, state)
    await bridge.run()


if __name__ == "__main__":
    asyncio.run(main())
