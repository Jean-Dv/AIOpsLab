#!/usr/bin/env python3
"""Run the reproducible warm-up/normal/fault/recovery observation protocol."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from urllib.error import HTTPError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
ETL_DIR = ROOT / "deploy" / "etl"
REPORT_DIR = ETL_DIR / "reports"
RUNS_DIR = REPORT_DIR / "runs"
TOPICS = ("otlp.metrics.v1", "otlp.logs.v1", "otlp.traces.v1")
FLAGD_ROLLOUT_TIMEOUT_SECONDS = 600
MAX_HOST_DISK_USAGE_PERCENT = 90.0
CONTROL_PLANE_COMPONENTS = {
    "etcd",
    "kube-apiserver",
    "kube-controller-manager",
    "kube-scheduler",
}


def load_env() -> dict[str, str]:
    values: dict[str, str] = {}
    for candidate in (ETL_DIR / ".env", ETL_DIR / ".env.example"):
        if not candidate.exists():
            continue
        for line in candidate.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key] = value
        break
    return values


ENV = load_env()


def run(*command: str, input_text: str | None = None) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            input=input_text,
            text=True,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as error:
        details = []
        if error.stdout and error.stdout.strip():
            details.append(f"stdout:\n{error.stdout.strip()}")
        if error.stderr and error.stderr.strip():
            details.append(f"stderr:\n{error.stderr.strip()}")
        suffix = "\n" + "\n".join(details) if details else ""
        raise RuntimeError(
            f"Command failed with exit code {error.returncode}: "
            f"{' '.join(command)}{suffix}"
        ) from error
    return completed.stdout.strip()


def run_snapshot(*command: str) -> str:
    """Capture a diagnostic command without hiding a failed run package."""
    completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
    output = completed.stdout
    if completed.stderr:
        output += ("\n" if output else "") + completed.stderr
    if completed.returncode:
        output += f"\n[exit_code={completed.returncode}]\n"
    return output.strip()


def now_ms() -> int:
    return int(time.time() * 1000)


def iso(timestamp_ms: int | None = None) -> str:
    value = datetime.fromtimestamp((timestamp_ms or now_ms()) / 1000, timezone.utc)
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def clickhouse_insert(table: str, row: dict[str, object]) -> None:
    query = quote(f"INSERT INTO aiopslab.{table} FORMAT JSONEachRow")
    request = Request(
        f"http://127.0.0.1:8123/?date_time_input_format=best_effort&query={query}",
        data=(json.dumps(row, separators=(",", ":")) + "\n").encode(),
        method="POST",
    )
    user = ENV.get("CLICKHOUSE_USER", "aiopslab")
    password = ENV.get("CLICKHOUSE_PASSWORD", "aiopslab-local")
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    try:
        with urlopen(request, timeout=15) as response:
            if response.status != 200:
                raise RuntimeError(response.read().decode())
    except HTTPError as error:
        detail = error.read().decode(errors="replace").strip()
        raise RuntimeError(
            f"ClickHouse rejected insert into {table} "
            f"with HTTP {error.code}: {detail}"
        ) from error


def clickhouse_query(query: str) -> list[dict[str, object]]:
    encoded = quote(query + " FORMAT JSON")
    request = Request(f"http://127.0.0.1:8123/?query={encoded}")
    user = ENV.get("CLICKHOUSE_USER", "aiopslab")
    password = ENV.get("CLICKHOUSE_PASSWORD", "aiopslab-local")
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    with urlopen(request, timeout=30) as response:
        return json.load(response).get("data", [])


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")


def flink_snapshot() -> dict[str, object]:
    result: dict[str, object] = {}
    for endpoint in ("jobs/overview", "jobmanager/config"):
        try:
            with urlopen(f"http://127.0.0.1:8081/{endpoint}", timeout=10) as response:
                result[endpoint] = json.load(response)
        except Exception as error:  # Diagnostic evidence must survive partial outages.
            result[endpoint] = {"error": str(error)}
    jobs = result.get("jobs/overview", {}).get("jobs", [])
    for job in jobs:
        job_id = job.get("jid")
        if not job_id:
            continue
        try:
            with urlopen(
                    f"http://127.0.0.1:8081/jobs/{job_id}/checkpoints", timeout=10) as response:
                result[f"jobs/{job_id}/checkpoints"] = json.load(response)
        except Exception as error:
            result[f"jobs/{job_id}/checkpoints"] = {"error": str(error)}
    return result


def consumer_lag() -> dict[str, object]:
    command = (
        "docker", "exec", "aiopslab-etl-kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "kafka:9092", "--describe",
        "--group", "aiopslab-flink-telemetry-etl-v2",
    )
    raw = run_snapshot(*command)
    total = 0
    parsed = False
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) >= 6 and fields[1] in TOPICS:
            try:
                total += int(fields[5])
                parsed = True
            except ValueError:
                pass
    return {"total_lag": total if parsed else None, "raw": raw}


def host_disk_snapshot() -> dict[str, object]:
    usage = shutil.disk_usage(ROOT)
    # Match df(1): reserved filesystem blocks are not available to the runner.
    effective_capacity = usage.used + usage.free
    percent = usage.used * 100 / effective_capacity
    return {
        "path": str(ROOT),
        "total_bytes": usage.total,
        "effective_capacity_bytes": effective_capacity,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "used_percent": round(percent, 2),
        "maximum_accepted_percent": MAX_HOST_DISK_USAGE_PERCENT,
    }


def require_disk_capacity() -> dict[str, object]:
    snapshot = host_disk_snapshot()
    if snapshot["used_percent"] > MAX_HOST_DISK_USAGE_PERCENT:
        free_gib = snapshot["free_bytes"] / (1024 ** 3)
        raise RuntimeError(
            f"Host filesystem is {snapshot['used_percent']:.2f}% full "
            f"({free_gib:.2f} GiB free), above the "
            f"{MAX_HOST_DISK_USAGE_PERCENT:.0f}% experimental limit; "
            "free disk space before starting a run"
        )
    return snapshot


def kubernetes_control_plane_snapshot() -> dict[str, object]:
    payload = json.loads(run(
        "kubectl", "get", "pods", "--namespace", "kube-system",
        "--selector", "tier=control-plane", "-o", "json",
        "--request-timeout=15s",
    ))
    components: dict[str, object] = {}
    for item in payload.get("items", []):
        component = item.get("metadata", {}).get("labels", {}).get("component")
        if component not in CONTROL_PLANE_COMPONENTS:
            continue
        statuses = item.get("status", {}).get("containerStatuses", [])
        components[component] = {
            "pod": item.get("metadata", {}).get("name"),
            "uid": item.get("metadata", {}).get("uid"),
            "phase": item.get("status", {}).get("phase"),
            "ready": bool(statuses) and all(status.get("ready") for status in statuses),
            "restart_count": sum(int(status.get("restartCount", 0)) for status in statuses),
        }
    missing = sorted(CONTROL_PLANE_COMPONENTS - components.keys())
    unhealthy = sorted(
        name for name, state in components.items()
        if state["phase"] != "Running" or not state["ready"]
    )
    return {
        "captured_at": iso(),
        "healthy": not missing and not unhealthy,
        "missing": missing,
        "unhealthy": unhealthy,
        "components": components,
    }


def wait_for_control_plane(stable_polls: int = 7, poll_seconds: float = 5.0,
                           timeout_seconds: float = 120.0) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    consecutive = 0
    previous_signature: tuple[tuple[object, ...], ...] | None = None
    last: dict[str, object] = {"healthy": False, "error": "not checked"}
    while True:
        try:
            last = kubernetes_control_plane_snapshot()
            signature = tuple(
                (
                    component,
                    state["uid"],
                    state["restart_count"],
                )
                for component, state in sorted(last.get("components", {}).items())
            )
            if last["healthy"] and signature == previous_signature:
                consecutive += 1
            elif last["healthy"]:
                consecutive = 1
            else:
                consecutive = 0
            previous_signature = signature if last["healthy"] else None
            if consecutive >= stable_polls:
                last["required_stable_polls"] = stable_polls
                last["poll_seconds"] = poll_seconds
                return last
        except Exception as error:
            consecutive = 0
            previous_signature = None
            last = {"healthy": False, "captured_at": iso(), "error": str(error)}
        if time.monotonic() >= deadline:
            raise RuntimeError(
                "Kubernetes control plane did not remain healthy for "
                f"{stable_polls} checks within {timeout_seconds} seconds: {last}"
            )
        time.sleep(poll_seconds)


def require_unchanged_control_plane(before: dict[str, object]) -> dict[str, object]:
    after = kubernetes_control_plane_snapshot()
    changes = []
    if not after["healthy"]:
        changes.append(
            f"unhealthy={after['unhealthy']}, missing={after['missing']}"
        )
    before_components = before.get("components", {})
    after_components = after.get("components", {})
    for component in sorted(CONTROL_PLANE_COMPONENTS):
        old = before_components.get(component)
        new = after_components.get(component)
        if old is None or new is None:
            continue
        if old["uid"] != new["uid"]:
            changes.append(f"{component} pod UID changed")
        if old["restart_count"] != new["restart_count"]:
            changes.append(
                f"{component} restarts changed from {old['restart_count']} "
                f"to {new['restart_count']}"
            )
    if changes:
        raise RuntimeError(
            "Kubernetes control plane changed during the repetition; "
            "the run is not experimentally valid: " + "; ".join(changes)
        )
    return after


def preflight(allow_backlog: bool, max_lag: int) -> dict[str, object]:
    disk = require_disk_capacity()
    control_plane = wait_for_control_plane()
    run("./scripts/etl.sh", "validate")
    active = clickhouse_query(
        "SELECT count() AS active FROM aiopslab.experiment_runs FINAL "
        "WHERE status = 'running'"
    )
    active_count = int(active[0]["active"]) if active else 0
    if active_count:
        raise RuntimeError(f"There are {active_count} experiment runs marked running")
    lag = consumer_lag()
    if lag["total_lag"] is not None and lag["total_lag"] > max_lag and not allow_backlog:
        raise RuntimeError(
            f"Flink has a Kafka backlog of {lag['total_lag']} records, above {max_lag}; "
            "wait until it is bounded "
            "or use --allow-backlog only for a diagnostic run"
        )
    jobs = flink_snapshot()
    running = [job for job in jobs.get("jobs/overview", {}).get("jobs", [])
               if job.get("state") == "RUNNING"]
    if len(running) != 1:
        raise RuntimeError(f"Expected one running Flink job, found {len(running)}")
    return {
        "checked_at": iso(),
        "consumer": lag,
        "maximum_accepted_lag": max_lag,
        "flink": jobs,
        "active_experiment_runs": active_count,
        "host_disk": disk,
        "kubernetes_control_plane": control_plane,
    }


def publish_control(event: dict[str, object]) -> None:
    payload = json.dumps(event, separators=(",", ":"))
    run(
        "docker", "exec", "-i", "aiopslab-etl-kafka",
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server", "kafka:9092",
        "--topic", "experiment.control.v1",
        input_text=payload + "\n",
    )


def offsets() -> dict[str, int]:
    snapshot: dict[str, int] = {}
    for topic in TOPICS:
        output = run(
            "docker", "exec", "aiopslab-etl-kafka",
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server", "kafka:9092", "--topic", topic, "--time", "-1",
        )
        snapshot[topic] = sum(int(line.rsplit(":", 1)[1]) for line in output.splitlines() if line)
    return snapshot


def initialize_package(run_dir: Path, manifest: dict[str, object],
                       preflight_report: dict[str, object]) -> None:
    (run_dir / "configuration").mkdir(parents=True, exist_ok=False)
    (run_dir / "logs").mkdir()
    write_json(run_dir / "manifest.json", manifest)
    write_json(run_dir / "configuration" / "preflight.json", preflight_report)
    (run_dir / "configuration" / "compose.yaml").write_text(
        run_snapshot("docker", "compose", "--env-file", "deploy/etl/.env.example",
                     "-f", "deploy/etl/docker-compose.yaml", "config") + "\n",
        encoding="utf-8",
    )
    (run_dir / "configuration" / "git-status.txt").write_text(
        run_snapshot("git", "status", "--short") + "\n", encoding="utf-8")
    (run_dir / "configuration" / "kubernetes.txt").write_text(
        run_snapshot("kubectl", "get", "all", "--namespace", "astronomy-shop", "-o", "wide")
        + "\n", encoding="utf-8")


def quality_report(run_id: str) -> dict[str, object]:
    quoted_run = run_id.replace("'", "''")
    return {
        "generated_at": iso(),
        "quality": clickhouse_query(
            "SELECT * FROM aiopslab.gold_quality_by_run_v2 "
            f"WHERE run_id = '{quoted_run}'"
        ),
        "grid": clickhouse_query(
            "SELECT count() AS grid_rows, sum(row_present) AS observed_rows, "
            "countIf(row_present = 0) AS generated_missing_rows "
            "FROM aiopslab.feature_tabular_v1 "
            f"WHERE run_id = '{quoted_run}'"
        ),
        "sequence_contract": clickhouse_query(
            "SELECT count() AS steps, uniqExact(sequence_id) AS sequences, "
            "countIf(sequence_step = 0) AS sequence_starts "
            "FROM aiopslab.feature_sequence_v1 "
            f"WHERE run_id = '{quoted_run}'"
        ),
        "label_leakage_columns": clickhouse_query(
            "SELECT name FROM system.columns WHERE database = 'aiopslab' "
            "AND table IN ('feature_tabular_v1', 'feature_sequence_v1') "
            "AND name IN ('condition', 'fault_type', 'affected_component')"
        ),
        "quality_by_service": clickhouse_query(
            "SELECT service_name, k8s_namespace, count() AS windows, "
            "countIf(metric_point_count > 0) AS metric_windows, "
            "countIf(cpu_usage_cores IS NOT NULL) AS cpu_windows, "
            "countIf(memory_working_set_bytes IS NOT NULL) AS memory_windows "
            "FROM aiopslab.gold_service_window_v2_latest "
            f"WHERE run_id = '{quoted_run}' "
            "GROUP BY service_name, k8s_namespace ORDER BY service_name, k8s_namespace"
        ),
    }


def wait_for_gold_stability(run_id: str, stable_polls: int = 3,
                            poll_seconds: float = 10.0,
                            timeout_seconds: float = 180.0) -> dict[str, object]:
    """Wait until a non-empty run keeps the same Gold count across polls."""
    quoted_run = run_id.replace("'", "''")
    deadline = time.monotonic() + timeout_seconds
    previous: int | None = None
    consecutive = 0
    history: list[dict[str, object]] = []
    while True:
        rows = clickhouse_query(
            "SELECT count() AS windows "
            "FROM aiopslab.gold_service_window_v2_latest "
            f"WHERE run_id = '{quoted_run}'"
        )
        current = int(rows[0]["windows"]) if rows else 0
        history.append({"captured_at": iso(), "windows": current})
        consecutive = consecutive + 1 if current == previous and current > 0 else 1
        previous = current
        if current > 0 and consecutive >= stable_polls:
            return {
                "required_equal_polls": stable_polls,
                "poll_seconds": poll_seconds,
                "final_windows": current,
                "history": history,
            }
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"Gold did not reach a stable non-empty count for run {run_id} "
                f"within {timeout_seconds} seconds"
            )
        time.sleep(poll_seconds)


def collect_package(run_dir: Path, run_id: str, before: dict[str, object],
                    after: dict[str, object], phases: list[dict[str, object]],
                    drain_seconds: float, manifest: dict[str, object]) -> None:
    if drain_seconds > 0:
        wait_phase(drain_seconds, "drain")
    after["gold_stability"] = wait_for_gold_stability(run_id)
    after["flink"] = flink_snapshot()
    after["consumer"] = consumer_lag()
    write_json(run_dir / "offsets-and-checkpoints.json", {"before": before, "after": after})
    write_json(run_dir / "phase-timeline.json", phases)

    profile_output = run("./scripts/etl.sh", "profile", run_id)
    profile_name = Path(profile_output.splitlines()[-1]).name
    profile_source = REPORT_DIR / profile_name
    if profile_source.exists():
        shutil.copyfile(profile_source, run_dir / "telemetry-profile.json")
    write_json(run_dir / "gold-quality-report.json", quality_report(run_id))

    containers = {
        "jobmanager.log": "aiopslab-etl-jobmanager",
        "taskmanager.log": "aiopslab-etl-taskmanager",
        "kafka.log": "aiopslab-etl-kafka",
        "clickhouse.log": "aiopslab-etl-clickhouse",
    }
    for filename, container in containers.items():
        (run_dir / "logs" / filename).write_text(
            run_snapshot("docker", "logs", "--since", "2h", container) + "\n",
            encoding="utf-8",
        )
    collector = run_snapshot(
        "kubectl", "get", "pods", "--namespace", "astronomy-shop",
        "-l", "app.kubernetes.io/name=opentelemetry-collector",
        "-o", "jsonpath={.items[0].metadata.name}",
    )
    if collector and "exit_code=" not in collector:
        (run_dir / "logs" / "otel-collector.log").write_text(
            run_snapshot("kubectl", "logs", "--namespace", "astronomy-shop",
                         collector, "--since=2h") + "\n",
            encoding="utf-8",
        )

    manifest["package_completed_at"] = iso()
    write_json(run_dir / "manifest.json", manifest)
    checksums = []
    for path in sorted(run_dir.rglob("*")):
        if path.is_file() and path.name != "checksums.txt":
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            checksums.append(f"{digest}  {path.relative_to(run_dir)}")
    (run_dir / "checksums.txt").write_text("\n".join(checksums) + "\n", encoding="utf-8")


def flag_document() -> dict[str, object]:
    configmap = json.loads(run(
        "kubectl", "get", "configmap", "flagd-config",
        "--namespace", "astronomy-shop", "-o", "json",
    ))
    return json.loads(configmap["data"]["demo.flagd.json"])


def select_fault(document: dict[str, object], requested: str) -> str:
    flags = document.get("flags", {})
    if requested in flags:
        return requested
    lowered = {name.lower(): name for name in flags}
    if requested.lower() in lowered:
        return lowered[requested.lower()]
    candidates = [name for name in flags if "ad" in name.lower() and "fail" in name.lower()]
    if len(candidates) == 1:
        return candidates[0]
    raise KeyError(f"Could not find {requested!r}; candidates: {', '.join(candidates) or 'none'}")


def set_fault(document: dict[str, object], flag: str, active: bool, original: str) -> None:
    entry = document["flags"][flag]
    if active:
        variants = entry.get("variants", {})
        for candidate in ("on", "100%", "failure", "enabled", "true"):
            if candidate in variants:
                entry["defaultVariant"] = candidate
                break
        else:
            raise KeyError(f"Flag {flag} has no recognized failure variant")
    else:
        entry["defaultVariant"] = original
    patch = {"data": {"demo.flagd.json": json.dumps(document, separators=(",", ":"))}}
    run(
        "kubectl", "patch", "configmap", "flagd-config",
        "--namespace", "astronomy-shop", "--type", "merge",
        "-p", json.dumps(patch),
    )
    restart_patch = {
        "spec": {"template": {"metadata": {"annotations": {
            "aiopslab.dev/flag-config-revision": str(uuid.uuid4()),
        }}}}
    }
    run(
        "kubectl", "patch", "deployment/flagd",
        "--namespace", "astronomy-shop", "--type", "merge",
        "-p", json.dumps(restart_patch),
    )
    run(
        "kubectl", "rollout", "status", "deployment/flagd",
        "--namespace", "astronomy-shop",
        f"--timeout={FLAGD_ROLLOUT_TIMEOUT_SECONDS}s",
    )
    expected = document["flags"][flag]["defaultVariant"]
    observed = flag_document()["flags"][flag]["defaultVariant"]
    if observed != expected:
        raise RuntimeError(
            f"Flag {flag} expected variant {expected!r}, observed {observed!r} after rollout"
        )


def wait_phase(seconds: float, label: str) -> None:
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        print(f"  {label}: {int(remaining)} s remaining", flush=True)
        time.sleep(min(30, remaining))


def phase(run_id: str, scenario_id: str, condition: str, duration: float,
          fault_type: str = "", affected_component: str = "") -> dict[str, object]:
    start = now_ms()
    event: dict[str, object] = {
        "run_id": run_id,
        "scenario_id": scenario_id,
        "phase_id": str(uuid.uuid4()),
        "condition": condition,
        "fault_type": fault_type,
        "affected_component": affected_component,
        "phase_start_ms": start,
        "phase_end_ms": None,
    }
    publish_control(event)
    clickhouse_insert("experiment_conditions", {
        "run_id": run_id,
        "phase_id": event["phase_id"],
        "condition": condition,
        "fault_type": fault_type,
        "affected_component": affected_component,
        "phase_start": iso(start),
        "phase_end": None,
    })
    wait_phase(duration, condition)
    event["phase_end_ms"] = now_ms()
    publish_control(event)
    clickhouse_insert("experiment_conditions", {
        "run_id": run_id,
        "phase_id": event["phase_id"],
        "condition": condition,
        "fault_type": fault_type,
        "affected_component": affected_component,
        "phase_start": iso(start),
        "phase_end": iso(int(event["phase_end_ms"])),
    })
    return event


def execute(args: argparse.Namespace) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    preflight_report = preflight(args.allow_backlog, args.max_lag)
    document = flag_document()
    fault = select_fault(document, args.fault)
    original_variant = document["flags"][fault]["defaultVariant"]
    report: dict[str, object] = {
        "scenario_id": args.scenario,
        "fault_type": fault,
        "configuration": args.configuration,
        "load_profile": args.load_profile,
        "runs": [],
    }
    stationary = args.stationary_seconds is not None
    if stationary and args.stationary_seconds <= 0:
        raise ValueError("--stationary-seconds must be greater than zero")
    durations = (
        {"normal": args.stationary_seconds}
        if stationary
        else {
            "warmup": args.warmup * args.duration_scale,
            "normal": args.normal * args.duration_scale,
            "fault": args.fault_duration * args.duration_scale,
            "recovery": args.recovery * args.duration_scale,
        }
    )

    for repetition in range(1, args.repetitions + 1):
        disk_before = require_disk_capacity()
        control_plane_before = wait_for_control_plane()
        run_id = str(uuid.uuid4())
        run_dir = RUNS_DIR / run_id
        started = now_ms()
        before = offsets()
        before_snapshot: dict[str, object] = {
            "captured_at": iso(started),
            "topic_end_offsets": before,
            "consumer": consumer_lag(),
            "flink": flink_snapshot(),
        }
        status = "running"
        nominal = (
            args.duration_scale == 1.0
            and args.repetitions == 3
            and not args.allow_backlog
            and not stationary
        )
        manifest: dict[str, object] = {
            "run_id": run_id,
            "scenario_id": args.scenario,
            "repetition": repetition,
            "status": status,
            "classification": "nominal" if nominal else "diagnostic",
            "application_version": args.application_version,
            "etl_contract": "gold_service_window_v2.2",
            "protocol": "stationary" if stationary else "fault-cycle",
            "configuration": args.configuration,
            "load_profile": args.load_profile,
            "fault_requested": args.fault,
            "fault_resolved": fault,
            "collection_start": iso(started),
            "durations_seconds": durations,
            "timezone": "UTC",
            "host_timezone": os.environ.get("TZ", "system-default"),
            "git_revision": run_snapshot("git", "rev-parse", "HEAD"),
            "host_disk_before": disk_before,
            "control_plane_before": control_plane_before,
        }
        initialize_package(run_dir, manifest, preflight_report)
        clickhouse_insert("experiment_runs", {
            "run_id": run_id,
            "scenario_id": args.scenario,
            "application_version": args.application_version,
            "configuration": args.configuration,
            "load_profile": args.load_profile,
            "collection_start": iso(started),
            "collection_end": None,
            "repetition": repetition,
            "status": status,
        })
        print(f"Run {repetition}/{args.repetitions}: {run_id}", flush=True)
        phases: list[dict[str, object]] = []
        fault_change_attempted = False
        try:
            if stationary:
                phases.append(phase(run_id, args.scenario, "normal", durations["normal"]))
            else:
                phases.append(phase(run_id, args.scenario, "warmup", durations["warmup"]))
                phases.append(phase(run_id, args.scenario, "normal", durations["normal"]))
                fault_change_attempted = True
                set_fault(document, fault, True, original_variant)
                phases.append(phase(
                    run_id, args.scenario, "fault", durations["fault"], fault, "ad",
                ))
                set_fault(document, fault, False, original_variant)
                fault_change_attempted = False
                manifest["fault_restored"] = True
                phases.append(phase(run_id, args.scenario, "recovery", durations["recovery"]))
            manifest["control_plane_after"] = require_unchanged_control_plane(
                control_plane_before
            )
            status = "complete" if nominal else "diagnostic_complete"
        except BaseException as run_error:
            status = "failed"
            manifest["run_error"] = {
                "type": type(run_error).__name__,
                "message": str(run_error),
            }
            if fault_change_attempted:
                try:
                    set_fault(document, fault, False, original_variant)
                    manifest["fault_restored"] = True
                    fault_change_attempted = False
                except Exception as cleanup_error:
                    manifest["fault_restored"] = False
                    manifest["cleanup_error"] = {
                        "type": type(cleanup_error).__name__,
                        "message": str(cleanup_error),
                    }
                    print(f"Fault restoration warning: {cleanup_error}", flush=True)
            raise
        finally:
            ended = now_ms()
            after = offsets()
            clickhouse_insert("experiment_runs", {
                "run_id": run_id,
                "scenario_id": args.scenario,
                "application_version": args.application_version,
                "configuration": args.configuration,
                "load_profile": args.load_profile,
                "collection_start": iso(started),
                "collection_end": iso(ended),
                "repetition": repetition,
                "status": status,
            })
            seconds = max(0.001, (ended - started) / 1000)
            report["runs"].append({
                "run_id": run_id,
                "repetition": repetition,
                "status": status,
                "collection_start": iso(started),
                "collection_end": iso(ended),
                "phases": phases,
                "events": {topic: after[topic] - before[topic] for topic in TOPICS},
                "events_per_second": {
                    topic: (after[topic] - before[topic]) / seconds for topic in TOPICS
                },
            })
            target = REPORT_DIR / f"experiment-{args.scenario}-{started}.json"
            target.write_text(json.dumps(report, indent=2), encoding="utf-8")
            manifest["status"] = status
            manifest["collection_end"] = iso(ended)
            manifest["package_collection_started_at"] = iso()
            write_json(run_dir / "manifest.json", manifest)
            after_snapshot: dict[str, object] = {
                "captured_at": iso(ended),
                "topic_end_offsets": after,
            }
            try:
                collect_package(
                    run_dir, run_id, before_snapshot, after_snapshot, phases,
                    args.drain_seconds, manifest,
                )
            except Exception as package_error:
                manifest["package_error"] = str(package_error)
                write_json(run_dir / "manifest.json", manifest)
                print(f"Package collection warning: {package_error}", flush=True)
            print(f"Partial report: {target}", flush=True)
            print(f"Run package: {run_dir}", flush=True)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--scenario", default="astronomy-ad-failure-v1")
    result.add_argument("--fault", default="adFailure")
    result.add_argument("--application-version", default="opentelemetry-demo-0.42.6")
    result.add_argument("--configuration", default="local-kind-etl-v1")
    result.add_argument("--load-profile", default="astronomy-shop-default-load-generator")
    result.add_argument("--repetitions", type=int, default=3)
    result.add_argument("--warmup", type=float, default=300)
    result.add_argument("--normal", type=float, default=600)
    result.add_argument("--fault-duration", type=float, default=600)
    result.add_argument("--recovery", type=float, default=600)
    result.add_argument(
        "--stationary-seconds", type=float,
        help="Collect only a normal stationary phase; always classifies the run as diagnostic.",
    )
    result.add_argument(
        "--duration-scale", type=float, default=1.0,
        help="Scale factor for smoke tests; 1.0 preserves the full protocol.",
    )
    result.add_argument(
        "--drain-seconds", type=float, default=45.0,
        help="Wait after each repetition for lateness, Gold emission, and checkpoints.",
    )
    result.add_argument(
        "--allow-backlog", action="store_true",
        help="Allow Kafka backlog and mark the execution diagnostic, never nominal.",
    )
    result.add_argument(
        "--max-lag", type=int, default=1000,
        help="Maximum bounded Kafka lag accepted at preflight (default: 1000 records).",
    )
    return result


if __name__ == "__main__":
    execute(parser().parse_args())
