import importlib.util
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
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

    def test_fault_baseline_rejects_active_variant(self):
        document = {"flags": {"adFailure": {"defaultVariant": "on"}}}
        with self.assertRaisesRegex(RuntimeError, "is active"):
            experiment.require_fault_baseline(document, "adFailure")

    def test_fault_baseline_rejects_change_between_repetitions(self):
        document = {"flags": {"adFailure": {"defaultVariant": "disabled"}}}
        with self.assertRaisesRegex(RuntimeError, "changed from baseline"):
            experiment.require_fault_baseline(
                document, "adFailure", expected_variant="off",
            )

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

    def test_consumer_lag_must_be_measurable_for_nominal_run(self):
        with self.assertRaisesRegex(RuntimeError, "Could not determine"):
            experiment.require_bounded_consumer_lag(
                {"total_lag": None}, max_lag=1000, allow_backlog=False,
            )

    def test_consumer_lag_rejects_backlog_above_limit(self):
        with self.assertRaisesRegex(RuntimeError, "above 1000"):
            experiment.require_bounded_consumer_lag(
                {"total_lag": 1001}, max_lag=1000, allow_backlog=False,
            )

    def test_wait_for_pipeline_stability_requires_gold_and_bounded_lag(self):
        windows = [
            [{"windows": "10"}],
            [{"windows": "10"}],
            [{"windows": "10"}],
            [{"windows": "10"}],
            [{"windows": "10"}],
        ]
        lags = [
            {"total_lag": 2000},
            {"total_lag": 900},
            {"total_lag": 800},
            {"total_lag": 700},
            {"total_lag": 600},
        ]
        with patch.object(experiment, "clickhouse_query", side_effect=windows), \
                patch.object(experiment, "consumer_lag", side_effect=lags), \
                patch.object(experiment.time, "sleep"):
            result = experiment.wait_for_pipeline_stability(
                "run-1", max_lag=1000, allow_backlog=False,
                stable_polls=3, poll_seconds=0, timeout_seconds=1,
            )

        self.assertEqual(700, result["final_consumer_lag"])
        self.assertEqual(4, len(result["history"]))

    def test_phase_duration_guard_rejects_suspension(self):
        event = {
            "condition": "recovery",
            "expected_duration_seconds": 600.0,
            "observed_duration_seconds": 48_300.0,
            "duration_tolerance_seconds": 12.0,
            "duration_valid": False,
        }
        with self.assertRaisesRegex(RuntimeError, "may have been suspended"):
            experiment.require_valid_phase_duration(event)

    def test_phase_duration_guard_accepts_small_scheduler_delay(self):
        event = {
            "condition": "normal",
            "expected_duration_seconds": 600.0,
            "observed_duration_seconds": 606.0,
            "duration_tolerance_seconds": 12.0,
            "duration_valid": True,
        }
        experiment.require_valid_phase_duration(event)

    def test_execute_repeats_preflight_before_every_repetition(self):
        args = SimpleNamespace(
            scenario="scenario",
            fault="adFailure",
            application_version="app-v1",
            configuration="config-v1",
            load_profile="load-v1",
            repetitions=3,
            warmup=1.0,
            normal=1.0,
            fault_duration=1.0,
            recovery=1.0,
            stationary_seconds=None,
            duration_scale=1.0,
            drain_seconds=0.0,
            allow_backlog=False,
            max_lag=1000,
        )
        preflight_result = {
            "host_disk": {"used_percent": 10.0},
            "kubernetes_control_plane": {"healthy": True, "components": {}},
        }

        def add_phase(phases, _run_id, _scenario, condition, *_args):
            event = {"condition": condition, "duration_valid": True}
            phases.append(event)
            return event

        with TemporaryDirectory() as temporary:
            report_dir = Path(temporary) / "reports"
            runs_dir = report_dir / "runs"
            with patch.object(experiment, "REPORT_DIR", report_dir), \
                    patch.object(experiment, "RUNS_DIR", runs_dir), \
                    patch.object(experiment, "preflight", return_value=preflight_result) \
                    as preflight_check, \
                    patch.object(experiment, "flag_document", return_value={
                        "flags": {"adFailure": {
                            "defaultVariant": "off",
                            "variants": {"off": False, "on": True},
                        }}
                    }), \
                    patch.object(experiment, "offsets", return_value={
                        topic: 0 for topic in experiment.TOPICS
                    }), \
                    patch.object(experiment, "consumer_lag", return_value={
                        "total_lag": 0, "raw": ""
                    }), \
                    patch.object(experiment, "flink_snapshot", return_value={}), \
                    patch.object(experiment, "initialize_package"), \
                    patch.object(experiment, "clickhouse_insert"), \
                    patch.object(experiment, "run_snapshot", return_value="revision"), \
                    patch.object(experiment, "append_phase", side_effect=add_phase), \
                    patch.object(experiment, "set_fault"), \
                    patch.object(
                        experiment, "require_unchanged_control_plane",
                        return_value={"healthy": True, "components": {}},
                    ), \
                    patch.object(experiment, "collect_package"):
                experiment.execute(args)

        self.assertEqual(3, preflight_check.call_count)

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
