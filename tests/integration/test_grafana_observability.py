"""Live verification of the Docker Grafana telemetry path.

Run after deploying Astronomy Shop and starting deploy/grafana/docker-compose.yaml:

    AIOPSLAB_GRAFANA_INTEGRATION=true poetry run pytest \
      tests/integration/test_grafana_observability.py -v
"""

import os

import pytest
import requests

pytestmark = pytest.mark.integration


def _enabled():
    return os.getenv("AIOPSLAB_GRAFANA_INTEGRATION", "false").lower() == "true"


@pytest.fixture(scope="module")
def grafana():
    if not _enabled():
        pytest.skip("set AIOPSLAB_GRAFANA_INTEGRATION=true for the live test")

    base_url = os.getenv("GRAFANA_URL", "http://localhost:3000").rstrip("/")
    auth = (
        os.getenv("GRAFANA_ADMIN_USER", "admin"),
        os.environ["GRAFANA_ADMIN_PASSWORD"],
    )
    return base_url, auth


def _get(grafana, path, **params):
    base_url, auth = grafana
    response = requests.get(f"{base_url}{path}", auth=auth, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def test_grafana_is_healthy_and_provisioned(grafana):
    health = _get(grafana, "/api/health")
    assert health["database"] == "ok"

    datasources = _get(grafana, "/api/datasources")
    assert {source["uid"] for source in datasources} >= {
        "webstore-metrics",
        "aiopslab-infra-metrics",
        "webstore-logs",
        "webstore-traces",
    }


def test_application_metrics_are_queryable(grafana):
    result = _get(
        grafana,
        "/api/datasources/proxy/uid/webstore-metrics/api/v1/query",
        query='count({__name__=~".+"})',
    )
    assert result["status"] == "success"
    assert result["data"]["result"]


def test_logs_are_queryable(grafana):
    indices = _get(
        grafana,
        "/api/datasources/proxy/uid/webstore-logs/_cat/indices/otel-logs-*",
        format="json",
    )
    assert indices


def test_traces_are_queryable(grafana):
    result = _get(
        grafana,
        "/api/datasources/proxy/uid/webstore-traces/api/services",
    )
    assert result.get("data")
