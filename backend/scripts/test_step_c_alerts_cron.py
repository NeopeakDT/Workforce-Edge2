"""
backend/scripts/test_step_c_alerts_cron.py
STEP C (Task 8) -- unit tests for aggregation/alerts_cron.py.

Pure orchestration-logic tests: get_cursor() and all four evaluator
functions are mocked, so no real database connection is required. The one
exception is the lock test, which exercises the REAL fcntl-based
main_with_lock() against a throwaway lock file (not a production path),
never touching the DB.

Run: python scripts/test_step_c_alerts_cron.py
"""

from pathlib import Path
import sys
import os
import fcntl
import tempfile
import unittest
from unittest.mock import patch, MagicMock, call

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from aggregation import alerts_cron


def _fake_cursor(rows):
    """Build a MagicMock usable as the `with get_cursor() as cur:` context,
    whose fetchall() returns `rows`."""
    cur = MagicMock()
    cur.fetchall.return_value = rows
    ctx = MagicMock()
    ctx.__enter__.return_value = cur
    ctx.__exit__.return_value = False
    return ctx


class RunOrchestrationTests(unittest.TestCase):
    def _patch_all(self, instance_ids=None, zones=None, device_ids=None):
        instance_ids = instance_ids or []
        zones = zones or []
        device_ids = device_ids or []

        cursor_sequence = [
            _fake_cursor([{"id": i} for i in instance_ids]),
            _fake_cursor([{"id": z, "farm_id": f} for z, f in zones]),
            _fake_cursor([{"id": d} for d in device_ids]),
        ]

        get_cursor_patch = patch.object(
            alerts_cron, "get_cursor", side_effect=cursor_sequence
        )
        late_sweep_patch = patch.object(alerts_cron, "evaluate_late_start_sweep")
        in_progress_patch = patch.object(alerts_cron, "evaluate_in_progress_instance")
        zone_patch = patch.object(alerts_cron, "evaluate_zone_staleness")
        device_offline_patch = patch.object(alerts_cron, "evaluate_device_offline")
        detector_offline_patch = patch.object(alerts_cron, "evaluate_detector_offline")

        mocks = {
            "get_cursor": get_cursor_patch.start(),
            "late_sweep": late_sweep_patch.start(),
            "in_progress": in_progress_patch.start(),
            "zone": zone_patch.start(),
            "device_offline": device_offline_patch.start(),
            "detector_offline": detector_offline_patch.start(),
        }
        self.addCleanup(get_cursor_patch.stop)
        self.addCleanup(late_sweep_patch.stop)
        self.addCleanup(in_progress_patch.stop)
        self.addCleanup(zone_patch.stop)
        self.addCleanup(device_offline_patch.stop)
        self.addCleanup(detector_offline_patch.stop)
        return mocks

    def test_late_start_sweep_called_once_no_args(self):
        mocks = self._patch_all()
        alerts_cron.run()
        mocks["late_sweep"].assert_called_once_with()

    def test_in_progress_instances_evaluated_once_each(self):
        ids = ["inst-1", "inst-2", "inst-3"]
        mocks = self._patch_all(instance_ids=ids)
        alerts_cron.run()
        self.assertEqual(mocks["in_progress"].call_count, len(ids))
        mocks["in_progress"].assert_has_calls([call(i) for i in ids], any_order=False)

    def test_zones_evaluated_with_correct_pairs(self):
        zones = [("zone-1", "farm-a"), ("zone-2", "farm-b")]
        mocks = self._patch_all(zones=zones)
        alerts_cron.run()
        self.assertEqual(mocks["zone"].call_count, len(zones))
        mocks["zone"].assert_has_calls(
            [call(farm_id, zone_id) for zone_id, farm_id in zones], any_order=False
        )

    def test_devices_evaluated_both_matchers_each(self):
        devices = ["dev-1", "dev-2"]
        mocks = self._patch_all(device_ids=devices)
        alerts_cron.run()
        self.assertEqual(mocks["device_offline"].call_count, len(devices))
        self.assertEqual(mocks["detector_offline"].call_count, len(devices))
        mocks["device_offline"].assert_has_calls([call(d) for d in devices], any_order=False)
        mocks["detector_offline"].assert_has_calls([call(d) for d in devices], any_order=False)

    def test_late_start_sweep_failure_does_not_block_rest(self):
        mocks = self._patch_all(
            instance_ids=["inst-1"], zones=[("z1", "f1")], device_ids=["d1"]
        )
        mocks["late_sweep"].side_effect = RuntimeError("boom")
        # Should not raise.
        alerts_cron.run()
        mocks["in_progress"].assert_called_once_with("inst-1")
        mocks["zone"].assert_called_once_with("f1", "z1")
        mocks["device_offline"].assert_called_once_with("d1")
        mocks["detector_offline"].assert_called_once_with("d1")

    def test_one_bad_instance_does_not_skip_others(self):
        ids = ["inst-1", "inst-2", "inst-3"]
        mocks = self._patch_all(instance_ids=ids)

        def side_effect(instance_id):
            if instance_id == "inst-2":
                raise RuntimeError("bad instance")

        mocks["in_progress"].side_effect = side_effect
        alerts_cron.run()
        self.assertEqual(mocks["in_progress"].call_count, 3)
        mocks["in_progress"].assert_has_calls([call(i) for i in ids], any_order=False)

    def test_one_bad_device_offline_call_does_not_skip_detector_call(self):
        mocks = self._patch_all(device_ids=["d1"])
        mocks["device_offline"].side_effect = RuntimeError("boom")
        alerts_cron.run()
        mocks["device_offline"].assert_called_once_with("d1")
        mocks["detector_offline"].assert_called_once_with("d1")

    def test_one_bad_device_does_not_skip_next_device(self):
        mocks = self._patch_all(device_ids=["d1", "d2"])

        def offline_side_effect(device_id):
            if device_id == "d1":
                raise RuntimeError("boom")

        mocks["device_offline"].side_effect = offline_side_effect
        alerts_cron.run()
        self.assertEqual(mocks["device_offline"].call_count, 2)
        self.assertEqual(mocks["detector_offline"].call_count, 2)


class LockTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(delete=False)
        self._tmp.close()
        self._lock_path = self._tmp.name
        self._env_patch = patch.dict(
            os.environ, {"ALERTS_CRON_LOCK_FILE": self._lock_path}
        )
        self._env_patch.start()
        self.addCleanup(self._env_patch.stop)
        self.addCleanup(lambda: os.path.exists(self._lock_path) and os.unlink(self._lock_path))
        # alerts_cron reads LOCK_FILE at import time into a module-level
        # constant, so patch that directly to point at our throwaway file.
        self._lock_file_patch = patch.object(alerts_cron, "LOCK_FILE", self._lock_path)
        self._lock_file_patch.start()
        self.addCleanup(self._lock_file_patch.stop)

    def test_second_invocation_skips_when_first_holds_lock(self):
        # Genuinely hold the flock ourselves, simulating a concurrent tick.
        held_fd = open(self._lock_path, "w")
        fcntl.flock(held_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            with patch.object(alerts_cron, "run") as run_mock:
                alerts_cron.main_with_lock()
                run_mock.assert_not_called()
        finally:
            fcntl.flock(held_fd, fcntl.LOCK_UN)
            held_fd.close()

    def test_successful_acquisition_calls_run(self):
        with patch.object(alerts_cron, "run") as run_mock:
            alerts_cron.main_with_lock()
            run_mock.assert_called_once_with()

    def test_lock_released_after_run_completes(self):
        with patch.object(alerts_cron, "run"):
            alerts_cron.main_with_lock()

        # Lock should be free again -- a fresh acquisition attempt succeeds.
        probe_fd = open(self._lock_path, "w")
        try:
            fcntl.flock(probe_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(probe_fd, fcntl.LOCK_UN)
        except BlockingIOError:
            self.fail("lock was not released after main_with_lock() completed")
        finally:
            probe_fd.close()

    def test_lock_released_even_if_run_raises(self):
        with patch.object(alerts_cron, "run", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                alerts_cron.main_with_lock()

        probe_fd = open(self._lock_path, "w")
        try:
            fcntl.flock(probe_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(probe_fd, fcntl.LOCK_UN)
        except BlockingIOError:
            self.fail("lock was not released after run() raised")
        finally:
            probe_fd.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
