#!/usr/bin/env python3
"""Profile Silver Parquet and Gold ClickHouse without changing either dataset."""

from __future__ import annotations

import base64
import json
import math
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

import pyarrow.dataset as dataset
from pyarrow.fs import FileSelector, FileType, S3FileSystem


TABLES = ("metrics", "logs", "traces", "schema_errors")
CPU_METRIC = "container.cpu.usage"
MEMORY_METRIC = "container.memory.working_set"
RUN_ID = os.environ.get("PROFILE_RUN_ID", "").strip()
if RUN_ID and not re.fullmatch(r"[A-Za-z0-9._:-]+", RUN_ID):
    raise ValueError("PROFILE_RUN_ID contains unsupported characters")


def percentile(sorted_values: list[int], value: float) -> int | None:
    """Return an exact nearest-rank percentile for an already sorted sample."""
    if not sorted_values:
        return None
    rank = max(1, math.ceil((value / 100) * len(sorted_values)))
    return sorted_values[rank - 1]


def identity_source(resource: dict[str, object]) -> str:
    """Return the declared source of a service identity, without guessing."""
    service = str(resource.get("service.name") or "").strip()
    if service and service.lower() != "unknown" and not service.lower().startswith(
            "unknown_service"):
        return "service.name"
    for key in (
        "k8s.deployment.name", "k8s.statefulset.name", "k8s.daemonset.name",
        "k8s.cronjob.name", "k8s.job.name",
    ):
        if str(resource.get(key) or "").strip():
            return key
    return "unresolved"


def object_files(fs: S3FileSystem, prefix: str) -> list[str]:
    try:
        return [
            info.path for info in fs.get_file_info(FileSelector(prefix, recursive=True))
            if info.type == FileType.File and info.path.endswith(".parquet")
        ]
    except FileNotFoundError:
        # Flink creates a table prefix only after its first committed record.
        # An absent optional prefix (for example schema_errors) means zero rows.
        return []


def profile_table(fs: S3FileSystem, table: str) -> dict[str, object]:
    files = object_files(fs, f"aiopslab-silver-v2/{table}")
    if not files:
        return {"files": 0, "events": 0}
    source = dataset.dataset(files, filesystem=fs, format="parquet")
    columns = [
        "eventId", "eventTime", "receivedAt", "runId", "serviceName", "k8sNamespace",
        "serviceNamespace", "podName", "deploymentName", "traceId", "spanId",
        "name", "unit", "instrumentType", "spanKind", "severity", "attributesJson",
        "resourceAttributesJson", "late", "duplicate", "schemaError",
        "sourcePartition", "sourceOffset",
    ]
    count = 0
    first = None
    last = None
    late = duplicates = schema_errors = missing_service = 0
    trace_links = 0
    services: Counter[str] = Counter()
    pods: set[str] = set()
    names: Counter[str] = Counter()
    severities: Counter[str] = Counter()
    schemas: Counter[str] = Counter()
    ordered: dict[int, list[tuple[int, int]]] = defaultdict(list)
    run_ids: set[str] = set()
    delays_ms: list[int] = []
    filter_expression = dataset.field("runId") == RUN_ID if RUN_ID else None
    identity_signatures: Counter[str] = Counter()
    units: Counter[str] = Counter()
    instrument_types: Counter[str] = Counter()
    span_kinds: Counter[str] = Counter()
    identity_sources: Counter[str] = Counter()
    metric_semantics: Counter[tuple[str, str, str, str, str]] = Counter()
    for batch in source.scanner(
            columns=columns, filter=filter_expression, batch_size=8192).to_batches():
        for row in batch.to_pylist():
            count += 1
            event_time = int(row["eventTime"] or 0)
            first = event_time if first is None else min(first, event_time)
            last = event_time if last is None else max(last, event_time)
            delays_ms.append(int(row["receivedAt"] or 0) - event_time)
            late += bool(row["late"])
            duplicates += bool(row["duplicate"])
            schema_errors += bool(row["schemaError"])
            service = row["serviceName"] or ""
            missing_service += not service or service == "unknown"
            services[service or "<missing>"] += 1
            identity_signatures["|".join((
                service or "<missing>", row["serviceNamespace"] or "",
                row["k8sNamespace"] or "", row["podName"] or "",
                row["deploymentName"] or "",
            ))] += 1
            if row["podName"]:
                pods.add(row["podName"])
            if row["name"]:
                names[row["name"]] += 1
            if row["severity"]:
                severities[row["severity"]] += 1
            if row["unit"]:
                units[row["unit"]] += 1
            if row["instrumentType"]:
                instrument_types[row["instrumentType"]] += 1
            if row["spanKind"]:
                span_kinds[row["spanKind"]] += 1
            if row["traceId"] and row["spanId"]:
                trace_links += 1
            if row["runId"]:
                run_ids.add(row["runId"])
            try:
                resource = json.loads(row["resourceAttributesJson"] or "{}")
                resource_keys = sorted(resource.keys())
                schemas[",".join(resource_keys)] += 1
            except (json.JSONDecodeError, AttributeError):
                resource = {}
                schemas["<invalid-json>"] += 1
            source = identity_source(resource)
            identity_sources[source] += 1
            if table == "metrics":
                metric_semantics[(
                    service or "<missing>", row["name"] or "<missing>",
                    row["unit"] or "", row["instrumentType"] or "", source,
                )] += 1
            ordered[int(row["sourcePartition"] or 0)].append(
                (int(row["sourceOffset"] or 0), event_time)
            )
    out_of_order = 0
    for records in ordered.values():
        previous = 0
        for _, timestamp in sorted(records):
            if previous and timestamp < previous:
                out_of_order += 1
            previous = max(previous, timestamp)
    duration = max(0.001, ((last or 0) - (first or 0)) / 1000)
    delays_ms.sort()
    return {
        "files": len(files),
        "events": count,
        "event_time_start": iso(first),
        "event_time_end": iso(last),
        "events_per_second": count / duration,
        "event_to_etl_reception_delay_ms": {
            "p50": percentile(delays_ms, 50),
            "p95": percentile(delays_ms, 95),
            "p99": percentile(delays_ms, 99),
            "maximum": delays_ms[-1] if delays_ms else None,
        },
        "late_events": late,
        "out_of_order_events": out_of_order,
        "duplicate_events": duplicates,
        "schema_errors": schema_errors,
        "missing_service_name": missing_service,
        "trace_and_span_id_coverage": trace_links / count if count else 0,
        "services": dict(services.most_common()),
        "pods": sorted(pods),
        "names": dict(names.most_common()),
        "severities": dict(severities.most_common()),
        "units": dict(units.most_common()),
        "instrument_types": dict(instrument_types.most_common()),
        "span_kinds": dict(span_kinds.most_common()),
        "identity_signatures": dict(identity_signatures.most_common()),
        "identity_resolution_sources": dict(identity_sources.most_common()),
        "metric_semantics": [
            {
                "service_name": key[0],
                "metric_name": key[1],
                "unit": key[2],
                "instrument_type": key[3],
                "identity_source": key[4],
                "samples": samples,
                "selected_for_gold": key[1] in (CPU_METRIC, MEMORY_METRIC),
            }
            for key, samples in metric_semantics.most_common()
        ],
        "resource_schema_signatures": dict(schemas.most_common()),
        "run_ids": sorted(run_ids),
    }


def bronze_profile(fs: S3FileSystem) -> dict[str, object]:
    files = object_files(fs, "aiopslab-bronze-v2/otlp")
    if not files:
        return {"files": 0, "records": 0, "payload_bytes": 0}
    source = dataset.dataset(files, filesystem=fs, format="parquet")
    counts: Counter[str] = Counter()
    sizes: Counter[str] = Counter()
    first = None
    last = None
    filter_expression = dataset.field("runId") == RUN_ID if RUN_ID else None
    for batch in source.scanner(
            columns=["signal", "payload", "receivedAt"],
            filter=filter_expression).to_batches():
        for row in batch.to_pylist():
            signal = row["signal"] or "unknown"
            counts[signal] += 1
            sizes[signal] += len(row["payload"] or b"")
            timestamp = int(row["receivedAt"] or 0)
            first = timestamp if first is None else min(first, timestamp)
            last = timestamp if last is None else max(last, timestamp)
    duration = max(0.001, ((last or 0) - (first or 0)) / 1000)
    return {
        "files": len(files),
        "records": sum(counts.values()),
        "payload_bytes": sum(sizes.values()),
        "records_by_signal": dict(counts),
        "bytes_by_signal": dict(sizes),
        "bytes_per_second_by_signal": {key: value / duration for key, value in sizes.items()},
    }


def clickhouse(query: str) -> object:
    url = "http://clickhouse:8123/?query=" + quote(query + " FORMAT JSON")
    request = Request(url)
    user = os.environ.get("CLICKHOUSE_USER", "aiopslab")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "aiopslab-local")
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    with urlopen(request, timeout=30) as response:
        return json.load(response).get("data", [])


def iso(timestamp_ms: int | None) -> str | None:
    if not timestamp_ms:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).isoformat()


def main() -> None:
    fs = S3FileSystem(
        access_key=os.environ.get("MINIO_ROOT_USER", "aiopslab"),
        secret_key=os.environ.get("MINIO_ROOT_PASSWORD", "aiopslab-local-secret"),
        endpoint_override="minio:9000",
        scheme="http",
    )
    silver = {table: profile_table(fs, table) for table in TABLES}
    signal_services = {
        table: set(profile.get("services", {})) - {"<missing>", "unknown"}
        for table, profile in silver.items() if table != "schema_errors"
    }
    common_services = set.intersection(*signal_services.values()) if signal_services else set()
    where = f" WHERE run_id = '{RUN_ID}'" if RUN_ID else ""
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id_filter": RUN_ID or None,
        "bronze": bronze_profile(fs),
        "silver": silver,
        "cross_signal": {
            "services_present_in_all_three_signals": sorted(common_services),
            "services_by_signal": {key: sorted(value) for key, value in signal_services.items()},
        },
        "gold": clickhouse(
            "SELECT count() windows, uniqExact(run_id) runs, uniqExact(service_name) services, "
            "avg(missing_metrics) missing_metrics_ratio, avg(missing_logs) missing_logs_ratio, "
            "avg(missing_traces) missing_traces_ratio, sum(late_event_count) late_events, "
            "sum(duplicate_event_count) duplicate_events, sum(schema_error_count) schema_errors "
            ", countIf(service_name = 'unknown') unresolved_service_windows "
            ", countIf(metric_point_count > 0) metric_windows "
            ", countIf(metric_point_count > 0 AND cpu_usage_cores IS NOT NULL) "
            "/ nullIf(countIf(metric_point_count > 0), 0) cpu_coverage_when_metrics_present "
            ", countIf(metric_point_count > 0 AND memory_working_set_bytes IS NOT NULL) "
            "/ nullIf(countIf(metric_point_count > 0), 0) memory_coverage_when_metrics_present "
            "FROM aiopslab.gold_service_window_v2_latest" + where
        ),
        "gold_quality": clickhouse(
            "SELECT * FROM aiopslab.gold_quality_by_run_v2" + where
        ),
        "gold_quality_by_service": clickhouse(
            "SELECT service_name, k8s_namespace, count() windows, "
            "countIf(metric_point_count > 0) metric_windows, "
            "countIf(cpu_usage_cores IS NOT NULL) cpu_windows, "
            "countIf(memory_working_set_bytes IS NOT NULL) memory_windows, "
            "countIf(metric_point_count > 0 AND cpu_usage_cores IS NOT NULL) "
            "/ nullIf(countIf(metric_point_count > 0), 0) cpu_coverage_when_metrics_present, "
            "countIf(metric_point_count > 0 AND memory_working_set_bytes IS NOT NULL) "
            "/ nullIf(countIf(metric_point_count > 0), 0) memory_coverage_when_metrics_present "
            "FROM aiopslab.gold_service_window_v2_latest" + where
            + " GROUP BY service_name, k8s_namespace ORDER BY service_name, k8s_namespace"
        ),
    }
    suffix = f"-{RUN_ID}" if RUN_ID else ""
    target = Path("/reports") / (
        f"telemetry-profile{suffix}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    )
    target.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(target)


if __name__ == "__main__":
    main()
