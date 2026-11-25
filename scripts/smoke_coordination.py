#!/usr/bin/env python3
"""Verify two real Kratos processes using isolated PostgreSQL, Redis and mocks.

Requires migrated TEST_DATABASE_URL (*_test) and a disposable literal-loopback
TEST_REDIS_URL (redis://). Never uses production DATABASE_URL/REDIS_URL defaults.
Run: python3 -I scripts/smoke_coordination.py
"""
import concurrent.futures
import ipaddress
import json
import os
from pathlib import Path
import select
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import unquote, urlparse, urlunparse
import uuid

ROOT = Path(__file__).resolve().parent.parent


def loopback_url(raw, scheme):
    parsed = urlparse(raw)
    try:
        valid = parsed.scheme == scheme and ipaddress.ip_address(parsed.hostname).is_loopback
        port = parsed.port
    except (ValueError, TypeError):
        valid = False
        port = None
    if not valid or not port or parsed.query or parsed.fragment:
        raise RuntimeError("test service requires an explicit literal-loopback URL")
    return parsed


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def redis_reply(reader, depth=0):
    if depth > 4:
        raise RuntimeError("Redis test response nesting exceeds bound")
    line = reader.readline(65537)
    if len(line) > 65536 or not line.endswith(b"\r\n"):
        raise RuntimeError("invalid Redis test response")
    kind, value = line[:1], line[1:-2]
    if kind == b"+":
        return value
    if kind == b"-":
        raise RuntimeError("Redis test command rejected")
    if kind == b":":
        return int(value)
    size = int(value)
    if size == -1:
        return None
    if size < 0 or size > 1048576:
        raise RuntimeError("Redis test response exceeds bound")
    if kind == b"$":
        data = reader.read(size + 2)
        if len(data) != size + 2 or data[-2:] != b"\r\n":
            raise RuntimeError("truncated Redis test response")
        return data[:-2]
    if kind == b"*" and size <= 4096:
        return [redis_reply(reader, depth + 1) for _ in range(size)]
    raise RuntimeError("unsupported Redis test response")


class RedisWire:
    def __init__(self, raw):
        parsed = loopback_url(raw, "redis")
        self.sock = socket.create_connection((parsed.hostname, parsed.port), timeout=3)
        self.reader = self.sock.makefile("rb")
        if parsed.password is not None:
            auth = [unquote(parsed.password)]
            if parsed.username:
                auth.insert(0, unquote(parsed.username))
            self.command("AUTH", *auth)
        database = parsed.path.strip("/") or "0"
        if not database.isdecimal() or not 0 <= int(database) <= 15:
            self.close()
            raise RuntimeError("invalid Redis test database")
        self.command("SELECT", database)

    def command(self, *parts):
        encoded = [p if isinstance(p, bytes) else str(p).encode() for p in parts]
        payload = b"*%d\r\n" % len(encoded)
        payload += b"".join(b"$%d\r\n" % len(p) + p + b"\r\n" for p in encoded)
        self.sock.sendall(payload)
        return redis_reply(self.reader)

    def close(self):
        self.reader.close()
        self.sock.close()


def run_proxy(port, host, remote_port):
    if not ipaddress.ip_address(host).is_loopback:
        raise RuntimeError("proxy target must be literal loopback")

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            try:
                with socket.create_connection((host, remote_port), timeout=2) as upstream:
                    peers = [self.request, upstream]
                    for peer in peers:
                        peer.settimeout(2)
                    while True:
                        readable, _, _ = select.select(peers, [], [], 0.5)
                        for peer in readable:
                            data = peer.recv(65536)
                            if not data:
                                return
                            (upstream if peer is self.request else self.request).sendall(data)
            except OSError:
                return

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True
        slots = threading.BoundedSemaphore(64)

        def process_request(self, request, address):
            if not self.slots.acquire(blocking=False):
                self.shutdown_request(request)
                return
            try:
                super().process_request(request, address)
            except BaseException:
                self.slots.release()
                raise

        def process_request_thread(self, request, address):
            try:
                super().process_request_thread(request, address)
            finally:
                self.slots.release()

    with Server(("127.0.0.1", port), Handler) as server:
        server.serve_forever()


class OwnedProcesses:
    def __init__(self, directory, env):
        self.directory, self.env = directory, env
        self.processes, self.logs = [], []

    def launch(self, command):
        log = open(self.directory / (str(len(self.logs)) + ".log"), "wb")
        self.logs.append(log)
        proc = subprocess.Popen(command, cwd=ROOT, env=self.env, stdout=log, stderr=log)
        self.processes.append(proc)
        return proc

    @staticmethod
    def stop(proc, crash=False):
        if proc.poll() is None:
            if crash:
                proc.kill()
            else:
                proc.terminate()
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)

    def close(self):
        for proc in reversed(self.processes):
            self.stop(proc)
        for log in self.logs:
            log.close()

    def show_logs(self):
        for log in self.logs:
            log.flush()
            print(Path(log.name).read_text()[-3000:], file=sys.stderr)


def wait_until(check, description, timeout=12):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return
        time.sleep(0.05)
    raise AssertionError(description + " deadline exceeded")


def http_call(base, path, key="", body=None, allow_disconnect=False, idem=None):
    command = ["curl", "-sSN", "--max-time", "20", "-w", "\n%{http_code}", base + path]
    if key:
        command += ["-H", "Authorization: Bearer " + key]
    if body is not None:
        command += ["-H", "Content-Type: application/json", "--data-binary", json.dumps(body, separators=(",", ":"))]
    if idem:
        command += ["-H", "Idempotency-Key: " + idem]
    result = subprocess.run(command, capture_output=True)
    if result.returncode:
        if allow_disconnect:
            return 0, ""
        raise AssertionError("owned local HTTP request failed with curl code " + str(result.returncode))
    payload, status = result.stdout.decode().rsplit("\n", 1)
    return int(status), payload


def http_ready(base, proc, path="/readyz"):
    if proc.poll() is not None:
        raise RuntimeError("owned test service exited before readiness")
    try:
        return http_call(base, path)[0] == 200
    except AssertionError:
        return False


def config_text(namespace, address, mock_address, rate=100000, burst=100, upstream=1):
    # A complete task-owned test configuration: do not mutate production YAML or
    # rely on textual replacements of defaults that may change independently.
    return f"""server:
  address: {address}
  max_concurrent: 8
  per_principal_concurrent: 4
  request_seconds: 20
  write_seconds: 2
  rate_per_minute: {rate}
  burst: {burst}
principals:
  - id: smoke
    key_env: GATEWAY_API_KEY
    allow_agent: true
    models: [small, strong]
    providers: [mock]
providers:
  - id: mock
    format: openai
    base_url: http://{mock_address}/v1
    key_env: MOCK_API_KEY
    allow_http: true
models:
  - {{id: decision, provider: mock, upstream: mock-decision, max_input_bytes: 262144, max_output_tokens: 4096}}
  - {{id: small, provider: mock, upstream: mock-small, max_input_bytes: 262144, max_output_tokens: 4096, stream: true, tools: true, forced_tools: true, cost_rank: 1, quality_tier: 1}}
  - {{id: strong, provider: mock, upstream: mock-strong, max_input_bytes: 262144, max_output_tokens: 4096, stream: true, tools: true, forced_tools: true, cost_rank: 10, quality_tier: 3}}
agent:
  decision_model: decision
  models: [small, strong]
  providers: [mock]
database:
  url_env: DATABASE_URL
  schema: public
  allow_insecure: true
coordination:
  mode: redis
  namespace: {namespace}
  url_env: REDIS_URL
  allow_insecure_local: true
  lease_ttl_ms: 3000
  renew_interval_ms: 750
  operation_timeout_ms: 250
  dial_timeout_ms: 500
  max_connections: 8
gateway:
  max_concurrent: {upstream}
  provider_concurrent: 1
  retries: 0
  circuit_failures: 1
  circuit_cooldown_seconds: 1
"""


def main():
    dsn, raw_redis = os.environ.get("TEST_DATABASE_URL", ""), os.environ.get("TEST_REDIS_URL", "")
    if not urlparse(dsn).path.endswith("_test"):
        raise RuntimeError("TEST_DATABASE_URL must select a disposable *_test database")
    parsed = loopback_url(raw_redis, "redis")
    subprocess.run(["make", "build"], cwd=ROOT, check=True)
    results = {"gateway_instances": 2, "paid_provider_calls": 0}
    key = "coordination-smoke-" + uuid.uuid4().hex
    wire = RedisWire(raw_redis)
    snapshots = {}
    def command_count():
        info = dict(line.split(b":", 1) for line in wire.command("INFO", "stats").splitlines() if b":" in line)
        return int(info[b"total_commands_processed"])
    def snapshot(label, base):
        status, payload = http_call(base, "/metrics", key)
        assert status == 200
        snapshots[label] = dict(line.split() for line in payload.splitlines() if line.startswith("route_mind_coordination_") and "{" not in line)
    commands_before, smoke_started = command_count(), time.monotonic()
    try:
        for phase, delay, failures in (("rate", "0s", 0), ("capacity", "500ms", 0), ("breaker", "500ms", 1), ("crash", "5s", 0), ("outage", "5s", 0)):
            namespace = "smoke-" + uuid.uuid4().hex
            prefix = "route-mind:{" + namespace + "}:"
            marker, owner = prefix + "test-owner", uuid.uuid4().hex.encode()
            if wire.command("SET", marker, owner, "NX", "EX", "3600") != b"OK":
                raise RuntimeError("cannot establish test namespace ownership")
            with tempfile.TemporaryDirectory(prefix="route-mind-owned-coordination-") as tmp:
                directory = Path(tmp)
                mock_port, a_port, b_port, proxy_port = [free_port() for _ in range(4)]
                proxy_user = parsed.netloc.rsplit("@", 1)[0] + "@" if "@" in parsed.netloc else ""
                proxy_url = urlunparse(("redis", proxy_user + "127.0.0.1:" + str(proxy_port), parsed.path, "", "", ""))
                env = {**os.environ, "DATABASE_URL": dsn, "REDIS_URL": proxy_url, "GATEWAY_API_KEY": key, "MOCK_API_KEY": "local-mock-only", "APP_ENV": "development"}
                for secret in ("MIGRATION_DATABASE_URL", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
                    env.pop(secret, None)
                owned = OwnedProcesses(directory, env)
                a_base, b_base, mock_base = ["http://127.0.0.1:" + str(p) for p in (a_port, b_port, mock_port)]
                pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
                try:
                    def start_proxy():
                        proc = owned.launch([sys.executable, "-I", str(Path(__file__).resolve()), "--redis-proxy", str(proxy_port), parsed.hostname, str(parsed.port)])
                        def listening():
                            if proc.poll() is not None:
                                raise RuntimeError("owned Redis proxy exited")
                            try:
                                with socket.create_connection(("127.0.0.1", proxy_port), timeout=0.2):
                                    return True
                            except OSError:
                                return False
                        wait_until(listening, "proxy startup")
                        return proc
                    proxy = start_proxy()
                    mock = owned.launch([str(ROOT / "bin/mock-provider"), "-address", "127.0.0.1:" + str(mock_port), "-delay", delay, "-fail-first", str(failures)])
                    wait_until(lambda: http_ready(mock_base, mock, "/healthz"), "mock readiness")
                    gateways = []
                    for index, port in enumerate((a_port, b_port)):
                        path = directory / ("gateway-" + str(index) + ".yaml")
                        path.write_text(config_text(namespace, "127.0.0.1:" + str(port), "127.0.0.1:" + str(mock_port), rate=1 if phase == "rate" else 100000, burst=2 if phase == "rate" else 100, upstream=2 if phase == "rate" else 1))
                        proc = owned.launch([str(ROOT / "bin/route-mind"), "-conf", str(path)])
                        gateways.append(proc)
                        base = (a_base, b_base)[index]
                        wait_until(lambda: http_ready(base, proc), "gateway readiness")
                    body = {"model": "small", "max_tokens": 32, "messages": [{"role": "user", "content": "hello"}]}
                    def call(base, request=body, disconnect=False):
                        return http_call(base, "/v1/chat/completions", key, request, allow_disconnect=disconnect)
                    def stats():
                        status, payload = http_call(mock_base, "/_stats", "local-mock-only")
                        assert status == 200
                        return json.loads(payload)
                    def no_live_upstream():
                        seconds, micros = wire.command("TIME")
                        now = int(seconds) * 1000 + int(micros) // 1000
                        return wire.command("ZCOUNT", prefix + "upstream:global", now + 1, "+inf") == 0
                    if phase == "rate":
                        assert call(a_base)[0] == 200 and call(b_base)[0] == 200
                        assert call(a_base)[0] == 429 and stats()["calls"] == 2
                        results["shared_rate_accepted"] = 2
                    elif phase == "capacity":
                        pending = pool.submit(call, a_base)
                        wait_until(lambda: stats()["active"] == 1, "held upstream permit")
                        assert call(b_base)[0] == 503
                        assert call(b_base, {**body, "model": "auto"})[0] == 503
                        assert pending.result(timeout=8)[0] == 200
                        native = {"input": [{"role": "user", "content": [{"type": "text", "text": "extract analyze summarize"}]}]}
                        status, payload = http_call(b_base, "/v1/agent/runs", key, native, idem=uuid.uuid4().hex)
                        run = json.loads(payload)
                        assert status == 201 and run["status"] == "completed" and run["counters"]["calls"] == 6
                        assert stats()["calls"] == 7 and stats()["peak"] == 1
                        results["shared_upstream_peak"] = stats()["peak"]
                        results["redis_native_calls"] = run["counters"]["calls"]
                    elif phase == "breaker":
                        assert call(a_base)[0] == 502
                        status, payload = call(b_base)
                        assert status == 503 and "circuit_open" in payload and stats()["calls"] == 1
                        time.sleep(1.1)
                        barrier = threading.Barrier(2)
                        def probe(base):
                            barrier.wait(timeout=2)
                            return call(base)[0]
                        probes = [pool.submit(probe, base) for base in (a_base, b_base)]
                        statuses = sorted(p.result(timeout=8) for p in probes)
                        assert statuses == [200, 503] and stats()["calls"] == 2
                        results["shared_half_open_probes"] = 1
                    elif phase == "crash":
                        pending = pool.submit(call, a_base, body, True)
                        wait_until(lambda: stats()["active"] == 1, "crash victim in flight")
                        snapshot(phase + "-a", a_base)
                        owned.stop(gateways[0], crash=True)
                        assert pending.result(timeout=8)[0] == 0
                        assert call(b_base)[0] == 503 and stats()["calls"] == 1
                        wait_until(no_live_upstream, "crashed lease expiry", timeout=6)
                        assert call(b_base)[0] == 200 and stats()["calls"] == 2 and stats()["peak"] == 1
                        results["forced_exit_ttl_recovery"] = "passed"
                    else:
                        pending = pool.submit(call, a_base)
                        wait_until(lambda: stats()["active"] == 1, "request before Redis outage")
                        owned.stop(proxy, crash=True)
                        assert pending.result(timeout=8)[0] in (0, 503)
                        assert http_call(b_base, "/readyz")[0] == 503
                        assert http_call(b_base, "/healthz")[0] == 200
                        assert http_call(b_base, "/metrics", key)[0] == 200
                        assert call(b_base)[0] == 503 and stats()["calls"] == 1
                        proxy = start_proxy()
                        wait_until(lambda: http_ready(b_base, gateways[1]), "Redis reconnect")
                        # Readiness does not erase uncertain permits: wait for
                        # their actual Redis-time expiry before new dispatch.
                        wait_until(no_live_upstream, "uncertain lease expiry", timeout=6)
                        assert call(b_base)[0] == 200 and stats()["calls"] == 2
                        results["redis_outage_fail_closed"] = "passed"
                    results[phase + "_observed_calls"] = stats()["calls"]
                    for index, base in enumerate((a_base, b_base)):
                        if gateways[index].poll() is None:
                            snapshot(phase + ("-a" if index == 0 else "-b"), base)
                except BaseException:
                    owned.show_logs()
                    raise
                finally:
                    owned.close()
                    pool.shutdown(wait=True, cancel_futures=True)
                    if wire.command("GET", marker) != owner:
                        raise RuntimeError("refusing Redis cleanup without namespace ownership")
                    keys, cursor = set(), b"0"
                    while True:
                        cursor, batch = wire.command("SCAN", cursor, "MATCH", prefix + "*", "COUNT", "100")
                        keys.update(batch)
                        if len(keys) > 10000:
                            raise RuntimeError("owned namespace cleanup exceeds bound")
                        if cursor == b"0":
                            break
                    if keys:
                        wire.command("DEL", *sorted(keys))
        observed = command_count() - commands_before
        operations = sum(int(m["route_mind_coordination_operations_total"]) for m in snapshots.values())
        duration = sum(float(m["route_mind_coordination_duration_seconds_sum"]) for m in snapshots.values())
        results.update({"redis_server_commands_observed": observed, "redis_server_observed_commands_per_second": round(observed / (time.monotonic() - smoke_started), 2), "coordination_operations_observed": operations, "coordination_mean_observed_ms": round(1000 * duration / operations, 3)})
        print(json.dumps(results, sort_keys=True))
    finally:
        wire.close()


if __name__ == "__main__":
    if len(sys.argv) == 5 and sys.argv[1] == "--redis-proxy":
        run_proxy(int(sys.argv[2]), sys.argv[3], int(sys.argv[4]))
    else:
        main()
