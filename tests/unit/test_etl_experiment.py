import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch


MODULE_PATH = Path(__file__).parents[2] / "deploy" / "etl" / "experiment.py"
SPEC = importlib.util.spec_from_file_location("etl_experiment", MODULE_PATH)
experiment = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(experiment)


class ExperimentTest(unittest.TestCase):
    def test_select_fault_accepts_exact_name(self):
        document = {"flags": {"adFailure": {"defaultVariant": "off"}}}
        self.assertEqual("adFailure", experiment.select_fault(document, "adFailure"))

    def test_select_fault_falls_back_to_single_ad_failure(self):
        document = {"flags": {"adServiceFailure": {"defaultVariant": "off"}}}
        self.assertEqual(
            "adServiceFailure", experiment.select_fault(document, "adFailure")
        )

    def test_select_fault_does_not_guess_when_ambiguous(self):
        document = {
            "flags": {
                "adFailureA": {"defaultVariant": "off"},
                "adFailureB": {"defaultVariant": "off"},
            }
        }
        with self.assertRaises(KeyError):
            experiment.select_fault(document, "missing")

    def test_wait_for_gold_stability_resets_after_count_change(self):
        samples = [
            [{"windows": "10"}],
            [{"windows": "11"}],
            [{"windows": "11"}],
            [{"windows": "11"}],
        ]
        with patch.object(experiment, "clickhouse_query", side_effect=samples), \
                patch.object(experiment.time, "sleep"):
            result = experiment.wait_for_gold_stability(
                "run-1", stable_polls=3, poll_seconds=0, timeout_seconds=1,
            )

        self.assertEqual(11, result["final_windows"])
        self.assertEqual(4, len(result["history"]))

    def test_set_fault_uses_unique_revision_and_extended_rollout_timeout(self):
        document = {
            "flags": {
                "adFailure": {
                    "defaultVariant": "off",
                    "variants": {"off": False, "on": True},
                }
            }
        }
        with patch.object(experiment, "run") as command, \
                patch.object(experiment, "flag_document", return_value={
                    "flags": {"adFailure": {"defaultVariant": "on"}}
                }):
            experiment.set_fault(document, "adFailure", True, "off")

        commands = [call.args for call in command.call_args_list]
        self.assertTrue(any("aiopslab.dev/flag-config-revision" in " ".join(args)
                            for args in commands))
        self.assertTrue(any("--timeout=600s" in args for args in commands))

    def test_require_disk_capacity_rejects_pressure(self):
        snapshot = {
            "used_percent": 96.0,
            "free_bytes": 10 * 1024 ** 3,
        }
        with patch.object(experiment, "host_disk_snapshot", return_value=snapshot):
            with self.assertRaisesRegex(RuntimeError, "above the 90% experimental limit"):
                experiment.require_disk_capacity()

    def test_host_disk_snapshot_counts_reserved_blocks_as_unavailable(self):
        usage = type("Usage", (), {
            "total": 100,
            "used": 80,
            "free": 10,
        })()
        with patch.object(experiment.shutil, "disk_usage", return_value=usage):
            snapshot = experiment.host_disk_snapshot()
        self.assertEqual(90, snapshot["effective_capacity_bytes"])
        self.assertEqual(88.89, snapshot["used_percent"])

    def test_wait_for_control_plane_requires_consecutive_healthy_checks(self):
        unhealthy = {"healthy": False}
        healthy = {"healthy": True, "components": {}}
        with patch.object(
            experiment,
            "kubernetes_control_plane_snapshot",
            side_effect=[healthy, unhealthy, healthy, healthy, healthy],
        ), patch.object(experiment.time, "sleep"):
            result = experiment.wait_for_control_plane(
                stable_polls=3, poll_seconds=0, timeout_seconds=1,
            )
        self.assertTrue(result["healthy"])
        self.assertEqual(3, result["required_stable_polls"])

    def test_wait_for_control_plane_resets_when_restart_count_changes(self):
        def healthy(restarts):
            return {
                "healthy": True,
                "components": {
                    "etcd": {"uid": "same", "restart_count": restarts},
                },
            }

        with patch.object(
            experiment,
            "kubernetes_control_plane_snapshot",
            side_effect=[healthy(0), healthy(0), healthy(1),
                         healthy(1), healthy(1)],
        ), patch.object(experiment.time, "sleep"):
            result = experiment.wait_for_control_plane(
                stable_polls=3, poll_seconds=0, timeout_seconds=1,
            )
        self.assertEqual(1, result["components"]["etcd"]["restart_count"])

    def test_require_unchanged_control_plane_rejects_restart(self):
        before = {
            "components": {
                "etcd": {"uid": "same", "restart_count": 0},
            }
        }
        after = {
            "healthy": True,
            "missing": [],
            "unhealthy": [],
            "components": {
                "etcd": {"uid": "same", "restart_count": 1},
            },
        }
        with patch.object(
            experiment, "kubernetes_control_plane_snapshot", return_value=after,
        ):
            with self.assertRaisesRegex(RuntimeError, "etcd restarts changed"):
                experiment.require_unchanged_control_plane(before)


if __name__ == "__main__":
    unittest.main()
