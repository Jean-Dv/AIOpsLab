import subprocess
from unittest.mock import MagicMock, patch

import pytest

from aiopslab.service.helm import Helm


@patch("aiopslab.service.helm.subprocess.Popen")
def test_install_supports_version_values_and_split_extra_args(mock_popen):
    process = MagicMock()
    process.returncode = 0
    process.communicate.return_value = (b"deployed", b"")
    mock_popen.return_value = process

    Helm.install(
        release_name="astronomy-shop",
        chart_path="open-telemetry/opentelemetry-demo",
        namespace="astronomy-shop",
        remote_chart=True,
        version="0.42.6",
        values_files=["first.yaml", "second.yaml"],
        extra_args=["--wait", "--timeout 10m"],
    )

    command = mock_popen.call_args.args[0]
    assert command == [
        "helm",
        "install",
        "astronomy-shop",
        "open-telemetry/opentelemetry-demo",
        "-n",
        "astronomy-shop",
        "--create-namespace",
        "--version",
        "0.42.6",
        "-f",
        "first.yaml",
        "-f",
        "second.yaml",
        "--wait",
        "--timeout",
        "10m",
    ]
    assert mock_popen.call_args.kwargs == {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
    }


@patch("aiopslab.service.helm.subprocess.Popen")
def test_install_raises_when_helm_fails(mock_popen):
    process = MagicMock()
    process.returncode = 1
    process.communicate.return_value = (b"", b"invalid values")
    mock_popen.return_value = process

    with pytest.raises(RuntimeError, match="invalid values"):
        Helm.install(
            release_name="broken",
            chart_path="remote/chart",
            namespace="test",
            remote_chart=True,
        )


@patch("aiopslab.service.helm.subprocess.Popen")
def test_install_accepts_one_values_file_as_a_string(mock_popen):
    process = MagicMock()
    process.returncode = 0
    process.communicate.return_value = (b"deployed", b"")
    mock_popen.return_value = process

    Helm.install(
        release_name="demo",
        chart_path="repo/chart",
        namespace="demo",
        remote_chart=True,
        values_files="profile.yaml",
    )

    assert mock_popen.call_args.args[0][-2:] == ["-f", "profile.yaml"]
