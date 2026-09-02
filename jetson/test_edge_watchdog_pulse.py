#!/usr/bin/env python3
"""
Unit tests for the detector-health pulse behavior added to edge_watchdog.py.

Pure unit tests only: requests.post is always mocked, no real HTTP, no DB,
no real sleeps (time.time is monkeypatched where needed). Run directly:

    python3 jetson/test_edge_watchdog_pulse.py
"""

import os
import sys
import types
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import edge_watchdog as wd


def make_payload(total_frames=100, camera_last_seen=None):
    return {
        "total_frames": total_frames,
        "camera_last_seen": camera_last_seen if camera_last_seen is not None else {"cam1": 0.0},
        "process_started_at": 0,
    }


class SendDetectorHealthPulseTests(unittest.TestCase):
    def setUp(self):
        self._orig_api_base = wd.API_BASE
        self._orig_device_key = wd.DEVICE_KEY

    def tearDown(self):
        wd.API_BASE = self._orig_api_base
        wd.DEVICE_KEY = self._orig_device_key

    def test_success_200_returns_true(self):
        wd.API_BASE = "https://example.test/api/v1"
        wd.DEVICE_KEY = "fake-device-key"
        mock_resp = MagicMock(status_code=200)
        with patch.object(wd.requests, "post", return_value=mock_resp) as mock_post:
            result = wd.send_detector_health_pulse(make_payload(total_frames=42))
        self.assertTrue(result)
        mock_post.assert_called_once()
        _, kwargs = mock_post.call_args
        self.assertEqual(kwargs["json"]["detector_healthy"], True)
        self.assertEqual(kwargs["json"]["total_frames"], 42)
        self.assertEqual(kwargs["json"]["camera_count"], 1)
        self.assertEqual(kwargs["headers"]["X-DEVICE-KEY"], "fake-device-key")

    def test_non_200_returns_false(self):
        wd.API_BASE = "https://example.test/api/v1"
        wd.DEVICE_KEY = "fake-device-key"
        mock_resp = MagicMock(status_code=500, text="internal error")
        with patch.object(wd.requests, "post", return_value=mock_resp):
            result = wd.send_detector_health_pulse(make_payload())
        self.assertFalse(result)

    def test_401_returns_false(self):
        wd.API_BASE = "https://example.test/api/v1"
        wd.DEVICE_KEY = "fake-device-key"
        mock_resp = MagicMock(status_code=401, text="unauthorized")
        with patch.object(wd.requests, "post", return_value=mock_resp):
            result = wd.send_detector_health_pulse(make_payload())
        self.assertFalse(result)

    def test_exception_returns_false_no_raise(self):
        wd.API_BASE = "https://example.test/api/v1"
        wd.DEVICE_KEY = "fake-device-key"
        with patch.object(wd.requests, "post", side_effect=Exception("connection timed out")):
            result = wd.send_detector_health_pulse(make_payload())
        self.assertFalse(result)

    def test_missing_config_returns_false_no_attempt(self):
        wd.API_BASE = None
        wd.DEVICE_KEY = None
        with patch.object(wd.requests, "post") as mock_post:
            result = wd.send_detector_health_pulse(make_payload())
        self.assertFalse(result)
        mock_post.assert_not_called()

    def test_missing_device_key_only_returns_false_no_attempt(self):
        wd.API_BASE = "https://example.test/api/v1"
        wd.DEVICE_KEY = None
        with patch.object(wd.requests, "post") as mock_post:
            result = wd.send_detector_health_pulse(make_payload())
        self.assertFalse(result)
        mock_post.assert_not_called()


class MainLoopPulseGuardTests(unittest.TestCase):
    """
    Exercise the single iteration of main()'s loop body that decides whether
    to call send_detector_health_pulse, without running the real infinite
    loop. main()'s loop body is extracted into
    edge_watchdog.run_one_watchdog_cycle(payload, age_seconds); these tests
    call that real function directly (with restart_detector/requests.post/
    time mocked), so both main() and this suite exercise the same code path
    instead of a hand-copied reproduction of it.
    """

    def setUp(self):
        self._orig_api_base = wd.API_BASE
        self._orig_device_key = wd.DEVICE_KEY
        self._orig_last_pulse = wd._last_pulse_sent_at
        wd.API_BASE = "https://example.test/api/v1"
        wd.DEVICE_KEY = "fake-device-key"
        wd._last_pulse_sent_at = 0.0
        # Reset is_system_stuck's function-attribute state between tests.
        for attr in ("startup_logged", "prev_frames", "no_progress_count"):
            if hasattr(wd.is_system_stuck, attr):
                delattr(wd.is_system_stuck, attr)

    def tearDown(self):
        wd.API_BASE = self._orig_api_base
        wd.DEVICE_KEY = self._orig_device_key
        wd._last_pulse_sent_at = self._orig_last_pulse
        for attr in ("startup_logged", "prev_frames", "no_progress_count"):
            if hasattr(wd.is_system_stuck, attr):
                delattr(wd.is_system_stuck, attr)

    def test_A_healthy_branch3_interval_elapsed_pulse_sent(self):
        payload = make_payload(total_frames=10)
        mock_resp = MagicMock(status_code=200)
        post_mock = MagicMock(return_value=mock_resp)
        restart_mock = MagicMock()
        wd._last_pulse_sent_at = 0.0
        with patch.object(wd, "restart_detector", restart_mock), \
             patch.object(wd.time, "time", return_value=100.0), \
             patch.object(wd.time, "sleep", lambda *_a, **_k: None), \
             patch.object(wd.requests, "post", post_mock):
            wd.run_one_watchdog_cycle(payload, 5.0)
        post_mock.assert_called_once()
        restart_mock.assert_not_called()

    def test_B_healthy_branch3_interval_not_elapsed_no_pulse(self):
        payload = make_payload(total_frames=10)
        post_mock = MagicMock()
        restart_mock = MagicMock()
        wd._last_pulse_sent_at = 100.0
        with patch.object(wd, "restart_detector", restart_mock), \
             patch.object(wd.time, "time", return_value=110.0), \
             patch.object(wd.time, "sleep", lambda *_a, **_k: None), \
             patch.object(wd.requests, "post", post_mock):
            wd.run_one_watchdog_cycle(payload, 5.0)
        post_mock.assert_not_called()

    def test_C_successful_pulse_advances_last_sent(self):
        payload = make_payload(total_frames=10)
        mock_resp = MagicMock(status_code=200)
        post_mock = MagicMock(return_value=mock_resp)
        restart_mock = MagicMock()
        wd._last_pulse_sent_at = 0.0
        with patch.object(wd, "restart_detector", restart_mock), \
             patch.object(wd.time, "time", return_value=200.0), \
             patch.object(wd.time, "sleep", lambda *_a, **_k: None), \
             patch.object(wd.requests, "post", post_mock):
            wd.run_one_watchdog_cycle(payload, 5.0)
        self.assertEqual(wd._last_pulse_sent_at, 200.0)

    def test_D_failed_pulse_does_not_advance_last_sent(self):
        payload = make_payload(total_frames=10)
        mock_resp = MagicMock(status_code=500, text="err")
        post_mock = MagicMock(return_value=mock_resp)
        restart_mock = MagicMock()
        wd._last_pulse_sent_at = 0.0
        with patch.object(wd, "restart_detector", restart_mock), \
             patch.object(wd.time, "time", return_value=200.0), \
             patch.object(wd.time, "sleep", lambda *_a, **_k: None), \
             patch.object(wd.requests, "post", post_mock):
            wd.run_one_watchdog_cycle(payload, 5.0)
        self.assertEqual(wd._last_pulse_sent_at, 0.0)

    def test_E_stuck_no_pulse_attempted_regardless_of_interval(self):
        # Force is_system_stuck() to return True by pre-seeding its
        # no-progress-count state at the threshold with unchanged frames
        # and a camera whose last-seen ts is far in the past.
        wd.is_system_stuck.prev_frames = 10
        wd.is_system_stuck.no_progress_count = wd.NO_PROGRESS_CHECKS - 1
        payload = make_payload(total_frames=10, camera_last_seen={"cam1": 0.0})
        post_mock = MagicMock()
        restart_mock = MagicMock()
        wd._last_pulse_sent_at = 0.0
        with patch.object(wd, "restart_detector", restart_mock), \
             patch.object(wd.time, "time", return_value=100000.0), \
             patch.object(wd.time, "sleep", lambda *_a, **_k: None), \
             patch.object(wd.requests, "post", post_mock):
            wd.run_one_watchdog_cycle(payload, None)
        post_mock.assert_not_called()
        restart_mock.assert_called_once()

    def test_F_payload_none_no_pulse_never_existed_file(self):
        post_mock = MagicMock()
        restart_mock = MagicMock()
        wd._last_pulse_sent_at = 0.0
        # Simulates a watchdog file that has never existed:
        # read_heartbeat_payload() -> None, read_heartbeat_age_seconds() -> None.
        with patch.object(wd, "restart_detector", restart_mock), \
             patch.object(wd.time, "time", return_value=100.0), \
             patch.object(wd.time, "sleep", lambda *_a, **_k: None), \
             patch.object(wd.requests, "post", post_mock):
            wd.run_one_watchdog_cycle(None, None)
        post_mock.assert_not_called()
        restart_mock.assert_not_called()

    def test_G_branch2_stale_file_no_pulse(self):
        payload = make_payload(total_frames=10)
        post_mock = MagicMock()
        restart_mock = MagicMock()
        wd._last_pulse_sent_at = 0.0
        stale_age = wd.WATCHDOG_TIMEOUT_SEC + 1
        with patch.object(wd, "restart_detector", restart_mock), \
             patch.object(wd.time, "time", return_value=100.0), \
             patch.object(wd.time, "sleep", lambda *_a, **_k: None), \
             patch.object(wd.requests, "post", post_mock):
            wd.run_one_watchdog_cycle(payload, stale_age)
        post_mock.assert_not_called()
        restart_mock.assert_called_once()

    def test_H_post_raises_returns_false_no_exception_propagates(self):
        wd.API_BASE = "https://example.test/api/v1"
        wd.DEVICE_KEY = "fake-device-key"
        with patch.object(wd.requests, "post", side_effect=TimeoutError("timed out")):
            try:
                result = wd.send_detector_health_pulse(make_payload())
            except Exception as e:
                self.fail(f"send_detector_health_pulse raised unexpectedly: {e}")
        self.assertFalse(result)

    def test_I_mocked_401_not_treated_as_success(self):
        wd.API_BASE = "https://example.test/api/v1"
        wd.DEVICE_KEY = "fake-device-key"
        mock_resp = MagicMock(status_code=401, text="unauthorized")
        with patch.object(wd.requests, "post", return_value=mock_resp):
            result = wd.send_detector_health_pulse(make_payload())
        self.assertFalse(result)

    def test_J_config_unset_returns_false_no_exception(self):
        wd.API_BASE = None
        wd.DEVICE_KEY = None
        try:
            result = wd.send_detector_health_pulse(make_payload())
        except Exception as e:
            self.fail(f"send_detector_health_pulse raised unexpectedly: {e}")
        self.assertFalse(result)

    def test_J_main_startup_does_not_raise_when_config_unset(self):
        # Run main()'s startup prints (not the infinite loop) with config
        # unset, confirming no hard-fail like edge_heartbeat_agent.py's
        # RuntimeError. We call the startup portion by invoking main()
        # with the while-loop body short-circuited via a single forced
        # KeyboardInterrupt from time.sleep, just to prove the startup
        # lines execute without raising before the loop begins.
        wd.API_BASE = None
        wd.DEVICE_KEY = None

        def _raise_to_stop(*_a, **_k):
            raise KeyboardInterrupt()

        with patch.object(wd, "read_heartbeat_payload", return_value=None), \
             patch.object(wd, "read_heartbeat_age_seconds", return_value=None), \
             patch.object(wd.time, "sleep", _raise_to_stop):
            try:
                wd.main()
            except KeyboardInterrupt:
                pass
            except Exception as e:
                self.fail(f"main() raised unexpectedly with config unset: {e}")


class RestartDetectorUnchangedTests(unittest.TestCase):
    def test_K_restart_detector_calls_reset_failed_then_restart(self):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return MagicMock(returncode=0, stdout="", stderr="")

        with patch.object(wd.subprocess, "run", side_effect=fake_run):
            wd.restart_detector()

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], ["systemctl", "reset-failed", wd.DETECTOR_SERVICE_NAME])
        self.assertEqual(calls[1], ["systemctl", "restart", wd.DETECTOR_SERVICE_NAME])


if __name__ == "__main__":
    unittest.main(verbosity=2)
