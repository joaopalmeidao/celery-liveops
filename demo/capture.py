"""Drive the demo stack and photograph what it shows.

Runs the same way on a laptop and in CI:

    python demo/capture.py --out-dir docs/screenshots

It starts real work, waits for the failures to actually happen, and captures the
panel at three moments that are hard to fake:

  01-live        a task streaming its terminal from another container
  02-no-signal   the same row after its worker was stopped
  03-orphan-lock the lock a dead process left behind, named and releasable

The worker is stopped and restarted through ``docker compose``, so the "no
signal" shot is a genuinely dead process rather than a styled badge.

Requires: the stack already up (``docker compose up -d``), playwright, httpx.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

import httpx
from playwright.sync_api import sync_playwright

VIEWPORT = {"width": 1440, "height": 960}

# NOT "networkidle": the panel polls every two seconds, so the network is never
# idle and the wait always times out. What actually means "ready" here is that
# the first poll painted something -- so we wait on a selector instead.
READY = "domcontentloaded"


def compose(args: list[str], compose_file: str) -> None:
    subprocess.run(
        ["docker", "compose", "-f", compose_file, *args],
        check=True,
        capture_output=True,
    )


def wait_for_api(base: str, timeout: float = 180) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"{base}/demo/overview", timeout=5).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit(f"the demo API never answered at {base}")


def wait_for_worker(base: str, timeout: float = 120) -> None:
    """A worker must be consuming before we queue anything, or nothing runs."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"{base}/demo/overview", timeout=5).json()["workers"]:
                return
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit("no worker ever sent a heartbeat")


def start(base: str, kind: str, **params) -> str:
    body = httpx.post(f"{base}/demo/start/{kind}", params=params, timeout=15).json()
    return body["task_id"]


def wait_until(fn, timeout: float, what: str) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit(f"timed out waiting for {what}")


def has_lines(base: str, task_id: str, minimum: int) -> bool:
    logs = httpx.get(f"{base}/liveops/runs/{task_id}/logs", timeout=10).json()["logs"]
    return len(logs.splitlines()) >= minimum


def is_alive(base: str, task_id: str) -> bool:
    return httpx.get(f"{base}/liveops/runs/{task_id}/logs", timeout=10).json()["alive"]


def has_lock(base: str) -> bool:
    return bool(httpx.get(f"{base}/liveops/locks", timeout=10).json()["locks"])


def open_run(page, base: str, task_id: str) -> None:
    """Open the panel with this run selected.

    Deep link rather than a click: the card list is redrawn by a poll every two
    seconds, so clicking races the redraw and lands on a detached element.
    """
    page.goto(f"{base}/?run={task_id}", wait_until=READY)
    page.wait_for_selector(".run.sel", timeout=30_000)
    page.wait_for_timeout(2500)   # let one poll cycle paint the terminal


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://localhost:8000")
    parser.add_argument("--out-dir", default="docs/screenshots")
    parser.add_argument("--compose-file", default="demo/docker-compose.yml")
    args = parser.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    wait_for_api(args.base)
    wait_for_worker(args.base)

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport=VIEWPORT, device_scale_factor=2)

        # ── 1. a healthy long task, streaming ────────────────────────────────
        slow = start(args.base, "slow", steps=180)
        wait_until(lambda: has_lines(args.base, slow, 6), 90, "the terminal to fill")

        open_run(page, args.base, slow)
        page.screenshot(path=out / "01-live.png")
        print(f"captured 01-live.png (run {slow[:8]})")

        # ── 2. the same row once its process is gone ─────────────────────────
        # Not a styled badge: the worker container is actually stopped.
        compose(["stop", "worker"], args.compose_file)
        wait_until(lambda: not is_alive(args.base, slow), 180, "presence to expire")
        open_run(page, args.base, slow)
        page.screenshot(path=out / "02-no-signal.png")
        print("captured 02-no-signal.png")

        compose(["start", "worker"], args.compose_file)
        wait_for_worker(args.base)

        # ── 3. the lock a dead process left behind ───────────────────────────
        start(args.base, "locked")
        wait_until(lambda: has_lock(args.base), 120, "the orphan lock to appear")
        page.reload(wait_until=READY)
        page.wait_for_selector(".lock", timeout=30_000)
        page.screenshot(path=out / "03-orphan-lock.png")
        print("captured 03-orphan-lock.png")

        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
