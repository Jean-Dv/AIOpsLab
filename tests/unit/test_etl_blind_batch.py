import importlib.util
import json
import random
import stat
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest


ETL_DIR = Path(__file__).parents[2] / "deploy" / "etl"
sys.path.insert(0, str(ETL_DIR))
MODULE_PATH = ETL_DIR / "blind_batch.py"
SPEC = importlib.util.spec_from_file_location("etl_blind_batch", MODULE_PATH)
blind_batch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(blind_batch)


class BlindBatchTest(unittest.TestCase):
    def setUp(self):
        self.protocol_path = ETL_DIR / "blind_protocol_v1.json"
        self.protocol = json.loads(self.protocol_path.read_text(encoding="utf-8"))

    def test_protocol_expands_to_balanced_equal_duration_cohort(self):
        allocations = blind_batch.validate_protocol(self.protocol)
        self.assertEqual(12, len(allocations))
        counts = {}
        for allocation in allocations:
            counts[allocation["cell_id"]] = counts.get(allocation["cell_id"], 0) + 1
            horizon = (
                allocation["normal_before_fault_seconds"]
                + allocation["fault_seconds"]
                + allocation["recovery_seconds"]
            )
            self.assertEqual(3000, horizon)
        self.assertEqual({
            "control_no_fault": 3,
            "fault_early": 3,
            "fault_middle": 3,
            "fault_late": 3,
        }, counts)

    def test_prepare_hides_allocation_and_commits_sealed_mapping(self):
        with TemporaryDirectory() as temporary:
            batch_dir = blind_batch.prepare_batch(
                self.protocol_path, Path(temporary), "batch-test",
                rng=random.Random(7),
            )
            public = blind_batch.read_json(batch_dir / blind_batch.PUBLIC_NAME)
            public_text = json.dumps(public)
            self.assertNotIn("cell_id", public_text)
            self.assertNotIn("fault_early", public_text)
            sealed_mode = stat.S_IMODE(
                (batch_dir / blind_batch.SEALED_NAME).stat().st_mode
            )
            self.assertEqual(0o600, sealed_mode)
            protocol, sealed, commitment = blind_batch.verify_batch(batch_dir)
            self.assertEqual(12, len(sealed["allocations"]))
            self.assertEqual(
                blind_batch.sha256_value(protocol), commitment["protocol_sha256"]
            )
            self.assertEqual(
                blind_batch.sha256_file(batch_dir / blind_batch.CONFIRMATORY_NAME),
                commitment["confirmatory_manifest_sha256"],
            )
            self.assertEqual(
                commitment["confirmatory_manifest_sha256"],
                public["confirmatory_manifest_sha256"],
            )

    def test_execution_args_keep_control_fault_inactive(self):
        allocations = blind_batch.validate_protocol(self.protocol)
        control = next(item for item in allocations if item["control"])
        control["run_code"] = "BR-CONTROL"
        commitment = {
            "batch_id": "batch-test",
            "allocation_commitment_sha256": "abc123",
        }
        args = blind_batch.execution_args(self.protocol, control, commitment)
        self.assertTrue(args.control_cycle)
        self.assertEqual(3000, args.normal)
        self.assertEqual(0, args.fault_duration)
        self.assertFalse(args.allow_backlog)
        self.assertEqual(1.0, args.duration_scale)

    def test_reveal_requires_complete_execution_and_frozen_predictions(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch_dir = blind_batch.prepare_batch(
                self.protocol_path, root, "batch-test", rng=random.Random(9),
            )
            predictions = root / "predictions.json"
            predictions.write_text('{"predictions": []}\n', encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "before all runs complete"):
                blind_batch.reveal_batch(batch_dir, predictions)

            public_path = batch_dir / blind_batch.PUBLIC_NAME
            public = blind_batch.read_json(public_path)
            public["status"] = "execution_complete"
            for index, run in enumerate(public["runs"], start=1):
                run["status"] = "complete"
                run["run_id"] = f"run-{index}"
            blind_batch.atomic_json(public_path, public)
            revealed_path = blind_batch.reveal_batch(batch_dir, predictions)
            revealed = blind_batch.read_json(revealed_path)
            self.assertEqual(
                blind_batch.sha256_file(predictions),
                revealed["predictions_commitment"]["predictions_sha256"],
            )
            self.assertEqual(12, len(revealed["allocations"]))

    def test_reveal_rejects_empty_predictions(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch_dir = blind_batch.prepare_batch(
                self.protocol_path, root, "batch-test", rng=random.Random(11),
            )
            public_path = batch_dir / blind_batch.PUBLIC_NAME
            public = blind_batch.read_json(public_path)
            public["status"] = "execution_complete"
            for index, run in enumerate(public["runs"], start=1):
                run["status"] = "complete"
                run["run_id"] = f"run-{index}"
            blind_batch.atomic_json(public_path, public)
            predictions = root / "empty.json"
            predictions.touch()
            with self.assertRaisesRegex(ValueError, "must not be empty"):
                blind_batch.reveal_batch(batch_dir, predictions)


if __name__ == "__main__":
    unittest.main()
