#!/usr/bin/env python3
"""Drive real binaries, sockets, curl, PostgreSQL, and a process restart.
Requires TEST_DATABASE_URL selecting an isolated *_test database with migrations
and grants applied. Never uses DATABASE_URL or any real provider configuration.
Run: python3 -I scripts/smoke.py
"""
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from urllib.parse import urlparse
import uuid

ROOT = Path(__file__).resolve().parent.parent
BASE = "http://127.0.0.1:8000"

def main():
    dsn = os.environ.get("TEST_DATABASE_URL", "")
    if not urlparse(dsn).path.endswith("_test"):
        raise SystemExit("TEST_DATABASE_URL must select a disposable *_test database")
    for port in (8000, 18081):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))  # Refuse to touch an existing listener.
    subprocess.run(["make", "build"], cwd=ROOT, check=True)
    key = "smoke-" + uuid.uuid4().hex
    # A fresh invocation must not replay runs retained by an earlier smoke.
    # Keep this namespace stable across this invocation's gateway restart.
    invocation = uuid.uuid4().hex
    env = {**os.environ, "DATABASE_URL": dsn, "GATEWAY_API_KEY": key,
           "MOCK_API_KEY": "local-mock-only", "APP_ENV": "development"}
    # The runtime must not receive DDL credentials.
    env.pop("MIGRATION_DATABASE_URL", None)
    processes = []
    with tempfile.TemporaryDirectory(prefix="route-mind-owned-smoke-") as tmp:
        # Dedicated bulk-verification admission settings, not production defaults.
        config_path = Path(tmp) / "config.yaml"
        sample = (ROOT / "configs/config.yaml").read_text()
        config_path.write_text(sample.replace("rate_per_minute: 60", "rate_per_minute: 100000").replace("burst: 10", "burst: 100"))
        logs = []
        def launch(command):
            log = open(Path(tmp) / (str(len(logs)) + ".log"), "wb")
            logs.append(log)
            proc = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=log)
            processes.append(proc)
            return proc
        def stop(proc):
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        def ready(url, proc):
            end = time.monotonic() + 15
            while time.monotonic() < end:
                if proc.poll() is not None:
                    raise RuntimeError("owned service exited before readiness")
                p = subprocess.run(["curl", "-fsS", "--max-time", "1", url],
                                   capture_output=True)
                if p.returncode == 0:
                    return
                time.sleep(0.1)
            raise RuntimeError("readiness deadline exceeded")
        def call(path, body=None, idem=None, expected=200, anthropic=False):
            command = ["curl", "-sSN", "--max-time", "20", "-w", "\n%{http_code}", BASE + path,
                       "-H", ("x-api-key: " if anthropic else "Authorization: Bearer ") + key]
            if body is not None:
                command += ["-H", "Content-Type: application/json", "--data-binary", json.dumps(body, separators=(",", ":"))]
            if idem:
                command += ["-H", "Idempotency-Key: " + invocation + "-" + idem]
            p = subprocess.run(command, capture_output=True, check=True)
            payload, status = p.stdout.decode().rsplit("\n", 1)
            if int(status) != expected:
                raise AssertionError(f"{path}: status={status}, expected={expected}, body={payload[:1000]}")
            if "PRIVATE" in payload:
                raise AssertionError("internal work appeared in a public response")
            return payload
        try:
            mock = launch([str(ROOT / "bin/mock-provider")])
            ready("http://127.0.0.1:18081/healthz", mock)
            gateway = launch([str(ROOT / "bin/route-mind"), "-conf", str(config_path)])
            ready(BASE + "/readyz", gateway)
            matrix = 0
            for anthropic in (False, True):
                path = "/v1/messages" if anthropic else "/v1/chat/completions"
                for model in ("small", "strong", "auto"):
                    for stream in (False, True):
                        body = {"model": model, "max_tokens": 256, "stream": stream,
                                "messages": [{"role": "user", "content": "hello"}]}
                        payload = call(path, body, anthropic=anthropic)
                        if stream:
                            assert ("event: message_stop" if anthropic else "[DONE]") in payload
                        else:
                            assert json.loads(payload)["model"] == model
                        matrix += 1
            simple = json.loads(call("/v1/agent/runs", {"input": [{"role": "user", "content": [{"type": "text", "text": "extract analyze summarize"}]}]}, "simple", 201))
            assert simple["status"] == "completed" and simple["counters"]["calls"] == 6
            assert [s["model"] for s in simple["steps"] if s["kind"] != "decision"] == ["small", "strong", "small"]
            body = {"input": [{"role": "user", "content": [{"type": "text", "text": "mixed tools"}]}],
                    "tools": [{"executor": "server", "name": "sum"}, {"executor": "client", "name": "client_lookup", "parameters": {"type": "object"}}],
                    "tool_choice": {"mode": "required"}}
            waiting = json.loads(call("/v1/agent/runs", body, "mixed", 201))
            assert waiting["status"] == "awaiting_tool_results" and len(waiting["required_actions"]) == 1
            replay = json.loads(call("/v1/agent/runs", body, "mixed"))
            assert replay["counters"] == waiting["counters"]
            stop(gateway)
            gateway = launch([str(ROOT / "bin/route-mind"), "-conf", str(config_path)])
            ready(BASE + "/readyz", gateway)
            path = "/v1/agent/runs/" + waiting["id"]
            restored = json.loads(call(path))
            assert restored["status"] == "awaiting_tool_results"
            assert restored["counters"] == waiting["counters"]
            results = {"revision": restored["revision"], "results": [{"call_id": a["id"], "content": "local-client-result"} for a in restored["required_actions"]]}
            final = json.loads(call(path + "/tool-results", results, "resume"))
            assert final["status"] == "completed" and final["counters"]["calls"] == restored["counters"]["calls"] + 2
            repeated = json.loads(call(path + "/tool-results", results, "resume"))
            assert repeated["counters"] == final["counters"]
            print(json.dumps({"protocol_matrix_requests": matrix, "native_steps": 3,
                              "native_upstream_calls": 6, "real_process_restart": "passed",
                              "mixed_tools_and_idempotency": "passed", "paid_provider_calls": 0}))
        except Exception:
            for log in logs:
                log.flush()
                print(Path(log.name).read_text()[-4000:])
            raise
        finally:
            for proc in reversed(processes):
                stop(proc)
            for log in logs:
                log.close()

if __name__ == "__main__":
    main()
