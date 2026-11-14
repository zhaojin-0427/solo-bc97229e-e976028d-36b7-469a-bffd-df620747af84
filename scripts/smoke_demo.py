#!/usr/bin/env python3
"""End-to-end smoke demo against a real uvicorn server.

Spins up the API on a local port with a fresh data directory, then drives:
clean groups -> pass & download; leaking groups -> conflict with real path;
re-upload -> old release revoked. Prints every step for live demonstration.

Usage:  python scripts/smoke_demo.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
DEMO = ROOT / "demo"


def wait_ready(base: str, timeout: float = 20) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{base}/health", timeout=2)
            if r.status_code == 200:
                return
        except httpx.TransportError:
            pass
        time.sleep(0.3)
    raise RuntimeError("server did not become ready")


def upload(base: str, kind: str, path: Path) -> dict:
    with path.open("rb") as fh:
        r = httpx.post(
            f"{base}/api/upload/{kind}",
            files={"file": (path.name, fh, "text/csv")},
            timeout=30,
        )
    r.raise_for_status()
    return r.json()


def wait_run(base: str, run_id: int, timeout: float = 30) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = httpx.get(f"{base}/api/runs/{run_id}", timeout=10)
        r.raise_for_status()
        body = r.json()
        if body["status"] in ("passed", "failed", "superseded"):
            return body
        time.sleep(0.3)
    raise RuntimeError(f"run {run_id} did not finish")


def banner(title: str) -> None:
    print("\n" + "=" * 68)
    print(f"  {title}")
    print("=" * 68)


def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="leakcheck-demo-")
    port = int(os.environ.get("DEMO_PORT", "8099"))
    base = f"http://127.0.0.1:{port}"
    env = {
        **os.environ,
        "LEAKCHECK_DATA_DIR": data_dir,
        "LEAKCHECK_POLL_INTERVAL": "0.2",
        "LEAKCHECK_DISABLE_WORKER": "0",
        "PYTHONPATH": str(ROOT),
    }
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_ready(base)
        print(f"[ready] {base}  (data dir {data_dir})")
        print(f"[health] {httpx.get(base + '/health').json()}")

        banner("1. 上传衍生关系（只增、重复合一）")
        out = upload(base, "relations", DEMO / "relations_clean.csv")
        print(f"版本 v{out['data_version']}，新增 {out['inserted_rows']} 条边，"
              f"合计 {out['total_rows_after']} 条")

        banner("2. 上传无泄漏分组 → 后台核查 → 放行下载")
        out = upload(base, "groups", DEMO / "groups_clean.csv")
        run = wait_run(base, out["verification"]["run_id"])
        print(f"run#{run['run_id']} 绑定 v{run['data_version']} -> {run['status']}，"
              f"连通分量 {run['components_total']}，冲突 {run['conflicts_total']}")
        assert run["status"] == "passed"
        dl = httpx.get(f"{base}/api/release/groups.csv", timeout=10)
        dl.raise_for_status()
        print(f"下载放行快照: {dl.headers['x-release-version']}，"
              f"{dl.headers['x-release-rows']} 行，前两行:")
        for line in dl.text.splitlines()[:3]:
            print("   ", line)

        banner("3. 上传跨组泄漏分组 → 核查失败，返回实际连接路径")
        out = upload(base, "groups", DEMO / "groups_leak.csv")
        run = wait_run(base, out["verification"]["run_id"])
        print(f"run#{run['run_id']} -> {run['status']}，冲突 {run['conflicts_total']} 处")
        for c in run["conflicts"]:
            print(f"  ✖ {c['sample_a']}({c['group_a']}) ↔ "
                  f"{c['sample_b']}({c['group_b']}) 分量大小 {c['component_size']}")
            print("    路径: " + " → ".join(c["path"]))
        assert run["status"] == "failed"
        blocked = httpx.get(f"{base}/api/release/groups.csv")
        print(f"下载被拒绝: HTTP {blocked.status_code}（未完成/未通过不报安全）")
        assert blocked.status_code == 409

        banner("4. 重新上传无泄漏分组 → 重新核查通过 → 恢复下载")
        out = upload(base, "groups", DEMO / "groups_clean.csv")
        run = wait_run(base, out["verification"]["run_id"])
        print(f"run#{run['run_id']} -> {run['status']}")
        assert run["status"] == "passed"
        st = httpx.get(f"{base}/api/status").json()
        print(f"当前 v{st['current_data_version']}，放行版本 "
              f"v{st['accepted_version']}，可下载={st['download_available']}")

        banner("全部演示通过 ✔")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
