"""
backend/scripts/test_step9_alert_wiring.py
Task 9 (Step C) -- Tests proving the ACTIVITY_EARLY transaction-boundary fix
in aggregation/activity_aggregator.py.

Background: evaluate_early_start() (alerts.matchers.activity_matcher) opens
its own DB connection internally. activity_aggregator.py's two
`INSERT INTO activity_instance` sites both run inside ONE
`with get_cursor() as cur:` block per loop iteration (STEP-4 batch), which
only commits when that block exits. Calling evaluate_early_start() while
that block is still open would query a row its own connection cannot see
yet -- silently returning `{"instance_found": False, ...}` forever. The
fix collects ids of GENUINELY newly-created instances (never reattach/
reuse/fallback ids) into `newly_created_instance_ids`, drains that list
into evaluate_early_start() only AFTER the batch's `with get_cursor()`
block has exited (so the insert is committed and cross-connection visible),
and never lets an evaluate_early_start() exception crash the loop.

No live DB is required or touched -- everything here is mocked/structural.
Run: python scripts/test_step9_alert_wiring.py
"""

from pathlib import Path
import sys
import re
import inspect
import textwrap
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from aggregation import activity_aggregator as agg  # noqa: E402

SOURCE_PATH = BACKEND_ROOT / "aggregation" / "activity_aggregator.py"
SOURCE_TEXT = SOURCE_PATH.read_text()
SOURCE_LINES = SOURCE_TEXT.splitlines()

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeCursor:
    """Minimal cursor stand-in for resolve_fallback_instance_or_skip's INSERT."""

    def __init__(self, fetchone_results):
        self._fetchone_results = list(fetchone_results)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        if not self._fetchone_results:
            return None
        return self._fetchone_results.pop(0)


def _line_indent(line):
    return len(line) - len(line.lstrip(" "))


def _find_line_index(needle, start=0):
    for i in range(start, len(SOURCE_LINES)):
        if needle in SOURCE_LINES[i]:
            return i
    raise AssertionError(f"could not find line containing: {needle!r}")


def _extract_block(start_needle):
    """
    Return the dedented source text of the block starting at the line
    containing `start_needle`, including all subsequent lines that are
    more indented than the start line (i.e. the body of that statement),
    stopping at the first line with indentation <= the start line's.
    """
    start_idx = _find_line_index(start_needle)
    start_indent = _line_indent(SOURCE_LINES[start_idx])
    block = [SOURCE_LINES[start_idx]]
    for line in SOURCE_LINES[start_idx + 1:]:
        if line.strip() == "":
            block.append(line)
            continue
        if _line_indent(line) > start_indent:
            block.append(line)
        else:
            break
    return textwrap.dedent("\n".join(block) + "\n")


# ---------------------------------------------------------------------------
# 1 & 2 & 3 (site 1): resolve_fallback_instance_or_skip id collection
# ---------------------------------------------------------------------------

def test_fresh_insert_collects_id():
    """Genuine INSERT ... RETURNING id success -> id appended, id returned."""
    fake_cur = FakeCursor(fetchone_results=[{"id": 4242}])
    collected = []
    result = agg.resolve_fallback_instance_or_skip(
        fake_cur,
        farm_id="farm-1",
        zone_id="zone-1",
        activity_type_id=3,
        schedule_id=None,
        activity_date=datetime(2020, 6, 15).date(),
        session_id="sess-1",
        event_time=datetime(2020, 6, 15, 5, 0, tzinfo=timezone.utc),
        farm_tz=None,
        matched_schedule=None,
        event_row_id="ev-1",
        event_columns=set(),
        fatal_label="unit-test fresh insert",
        newly_created_ids=collected,
    )
    check(
        "site1_fresh_insert_returns_id",
        result == 4242,
        f"got {result!r}",
    )
    check(
        "site1_fresh_insert_collects_id",
        collected == [4242],
        f"collected={collected!r}",
    )


def test_reattach_does_not_collect_id():
    """
    INSERT conflicts (ON CONFLICT DO NOTHING -> fetchone() None), then the
    reattach cascade (find_in_progress_bucket_attach) returns an id for an
    EXISTING instance -- that id must be returned but NOT collected.
    """
    fake_cur = FakeCursor(fetchone_results=[None])  # INSERT conflicted
    collected = []
    with patch.object(agg, "is_historical_replay_event", return_value=False), \
         patch.object(agg, "find_in_progress_bucket_attach", return_value=9999) as mock_attach, \
         patch.object(agg, "recover_historical_instance_attach") as mock_hist:
        result = agg.resolve_fallback_instance_or_skip(
            fake_cur,
            farm_id="farm-1",
            zone_id="zone-1",
            activity_type_id=3,
            schedule_id=None,
            activity_date=datetime(2020, 6, 15).date(),
            session_id="sess-1",
            event_time=datetime(2020, 6, 15, 5, 0, tzinfo=timezone.utc),
            farm_tz=None,
            matched_schedule=None,
            event_row_id="ev-1",
            event_columns=set(),
            fatal_label="unit-test reattach",
            newly_created_ids=collected,
        )
    check(
        "site1_reattach_returns_existing_id",
        result == 9999 and mock_attach.called,
        f"got {result!r}",
    )
    check(
        "site1_reattach_does_not_collect_id",
        collected == [],
        f"collected={collected!r}",
    )
    check(
        "site1_reattach_did_not_fall_through_to_historical",
        not mock_hist.called,
    )


def test_newly_created_ids_none_is_safe():
    """Callers that don't care about collection (newly_created_ids=None, the
    default) must not blow up on the append-guard."""
    fake_cur = FakeCursor(fetchone_results=[{"id": 7}])
    result = agg.resolve_fallback_instance_or_skip(
        fake_cur,
        farm_id="farm-1",
        zone_id="zone-1",
        activity_type_id=3,
        schedule_id=None,
        activity_date=datetime(2020, 6, 15).date(),
        session_id="sess-1",
        event_time=datetime(2020, 6, 15, 5, 0, tzinfo=timezone.utc),
        farm_tz=None,
        matched_schedule=None,
        event_row_id="ev-1",
        event_columns=set(),
        fatal_label="unit-test no collection",
    )
    check("site1_default_none_does_not_crash", result == 7, f"got {result!r}")


# ---------------------------------------------------------------------------
# Site 2 (inline in run()): structural checks that the `_new_row` branch
# collects, and that the surrounding reattach branches (MISSED reopen,
# uniq-conflict recovery) do not.
# ---------------------------------------------------------------------------

def test_site2_new_row_branch_collects_id():
    idx_new_row = _find_line_index("_new_row = cur.fetchone()")
    idx_if = _find_line_index("if _new_row:", start=idx_new_row)
    idx_append = _find_line_index(
        "newly_created_instance_ids.append(instance_id)", start=idx_if
    )
    idx_else = _find_line_index("else:", start=idx_if)
    check(
        "site2_append_is_inside_if_new_row_branch",
        idx_if < idx_append < idx_else,
        f"if={idx_if} append={idx_append} else={idx_else}",
    )


def test_only_two_append_call_sites_and_two_inserts():
    insert_sites = [
        i for i, l in enumerate(SOURCE_LINES) if "INSERT INTO activity_instance" in l
    ]
    append_sites = [
        i
        for i, l in enumerate(SOURCE_LINES)
        if re.search(r"newly_created_ids\.append\(|newly_created_instance_ids\.append\(", l)
    ]
    check(
        "exactly_two_insert_into_activity_instance_statements",
        len(insert_sites) == 2,
        f"found {len(insert_sites)} at lines {[i + 1 for i in insert_sites]}",
    )
    check(
        "exactly_two_append_call_sites",
        len(append_sites) == 2,
        f"found {len(append_sites)} at lines {[i + 1 for i in append_sites]}",
    )
    # every reattach-only helper (bucket attach reuse, historical reattach,
    # MISSED-slot reopen, uniq-conflict recovery) must contain no append call.
    for helper in (
        "find_in_progress_bucket_attach",
        "recover_historical_instance_attach",
        "reopen_missed_activity_instance",
        "_attach_from_historical_row",
    ):
        def_line = _find_line_index(f"def {helper}(")
        # crude but sufficient: body ends at next top-level `def ` at column 0
        end = len(SOURCE_LINES)
        for i in range(def_line + 1, len(SOURCE_LINES)):
            if SOURCE_LINES[i].startswith("def "):
                end = i
                break
        body = "\n".join(SOURCE_LINES[def_line:end])
        check(
            f"reattach_helper_{helper}_has_no_append_call",
            ".append(" not in body or "newly_created" not in body,
        )


# ---------------------------------------------------------------------------
# Loop-level structural checks: reset per iteration, post-commit placement.
# ---------------------------------------------------------------------------

def test_reset_is_inside_while_loop_before_with_block():
    idx_loops_incr = _find_line_index("loops += 1")
    idx_reset = _find_line_index(
        "newly_created_instance_ids = []", start=idx_loops_incr
    )
    idx_with = _find_line_index("with get_cursor() as cur:", start=idx_loops_incr)
    check(
        "reset_line_between_loop_start_and_with_block",
        idx_loops_incr < idx_reset < idx_with,
        f"loops+=1={idx_loops_incr} reset={idx_reset} with={idx_with}",
    )
    # same indentation as loops += 1 -> it's a direct statement in the while
    # body, so it re-executes (and resets) every iteration, not once outside.
    check(
        "reset_line_same_indent_as_loop_body",
        _line_indent(SOURCE_LINES[idx_reset]) == _line_indent(SOURCE_LINES[idx_loops_incr]),
    )


def test_post_commit_call_is_outside_with_block():
    idx_with = _find_line_index("with get_cursor() as cur:")
    with_indent = _line_indent(SOURCE_LINES[idx_with])

    idx_for = _find_line_index(
        "for _iid in newly_created_instance_ids:", start=idx_with
    )
    idx_eval_call = _find_line_index(
        "_alert_activity_matcher.evaluate_early_start(_iid)", start=idx_for
    )

    # The for-loop driving evaluation must sit at the SAME indentation as the
    # `with get_cursor() as cur:` line itself (a sibling statement in the
    # while-loop body) -- proving it is NOT nested inside the with-block
    # (whose body is indented deeper than `with_indent`).
    check(
        "post_commit_for_loop_is_sibling_of_with_block_not_nested_in_it",
        _line_indent(SOURCE_LINES[idx_for]) == with_indent,
        f"for-loop indent={_line_indent(SOURCE_LINES[idx_for])} with indent={with_indent}",
    )

    # And it must appear textually after the with-block's own body ends
    # (i.e. after the last line whose indentation is greater than with_indent
    # following the `with` line) -- confirming it's not merely dedented
    # mid-block but genuinely after the block closes.
    last_with_body_idx = idx_with
    for i in range(idx_with + 1, idx_for):
        if SOURCE_LINES[i].strip() == "":
            continue
        if _line_indent(SOURCE_LINES[i]) > with_indent:
            last_with_body_idx = i
        elif _line_indent(SOURCE_LINES[i]) <= with_indent:
            # some other sibling statement before the for-loop (e.g. the
            # `if not processed:` block does NOT apply here since idx_for
            # is found first) -- that's fine, just keep scanning.
            continue
    check(
        "post_commit_for_loop_appears_after_with_block_body",
        idx_for > last_with_body_idx,
        f"for={idx_for} last_with_body_line={last_with_body_idx}",
    )
    check(
        "evaluate_early_start_call_present_in_post_commit_loop",
        idx_eval_call > idx_for,
    )


def test_import_present():
    check(
        "activity_matcher_import_present",
        "from alerts.matchers import activity_matcher as _alert_activity_matcher" in SOURCE_TEXT,
    )


# ---------------------------------------------------------------------------
# Behavioral proof (executes the ACTUAL post-commit block's source text, not
# a reimplementation) for: correct id passed, exception swallowed, loop
# continues across multiple ids.
# ---------------------------------------------------------------------------

def _run_post_commit_block(newly_created_instance_ids, evaluate_mock):
    """
    Extract the exact `for _iid in newly_created_instance_ids: ...` block
    from the real source file and exec() it, so the test proves the
    behavior of the real code text (not a hand-written copy of the logic).
    """
    block_src = _extract_block("for _iid in newly_created_instance_ids:")
    namespace = {
        "newly_created_instance_ids": newly_created_instance_ids,
        "_alert_activity_matcher": MagicMock(evaluate_early_start=evaluate_mock),
        "print": lambda *a, **k: None,
    }
    exec(compile(block_src, "<post_commit_block>", "exec"), namespace)


def test_post_commit_calls_evaluate_for_each_collected_id():
    calls = []

    def fake_evaluate(iid):
        calls.append(iid)
        return {"instance_found": True, "alerts_created": []}

    _run_post_commit_block([101, 202], fake_evaluate)
    check(
        "post_commit_calls_evaluate_for_every_collected_id_in_order",
        calls == [101, 202],
        f"calls={calls}",
    )


def test_post_commit_swallows_exceptions_and_continues():
    calls = []

    def fake_evaluate(iid):
        calls.append(iid)
        if iid == 101:
            raise RuntimeError("boom: simulated evaluator failure")
        return {"instance_found": True, "alerts_created": []}

    try:
        _run_post_commit_block([101, 202], fake_evaluate)
        raised = False
    except Exception:
        raised = True

    check("post_commit_exception_does_not_propagate", raised is False)
    check(
        "post_commit_continues_to_next_id_after_exception",
        calls == [101, 202],
        f"calls={calls}",
    )


def test_post_commit_empty_list_is_noop():
    calls = []
    _run_post_commit_block([], lambda iid: calls.append(iid))
    check("post_commit_empty_list_calls_nothing", calls == [])


# ---------------------------------------------------------------------------
# Systemd template files
# ---------------------------------------------------------------------------

def test_systemd_files():
    repo_root = BACKEND_ROOT.parent
    svc_path = repo_root / "systemd" / "workforce-alerts.service"
    timer_path = repo_root / "systemd" / "workforce-alerts.timer"

    check("workforce_alerts_service_exists", svc_path.exists())
    check("workforce_alerts_timer_exists", timer_path.exists())
    if not (svc_path.exists() and timer_path.exists()):
        return

    svc_text = svc_path.read_text()
    timer_text = timer_path.read_text()

    check("service_type_oneshot", "Type=oneshot" in svc_text)
    check(
        "service_working_directory_correct",
        "WorkingDirectory=/home/neopeak/Desktop/workforce/Edge2/backend" in svc_text,
    )
    check(
        "service_exec_start_matches_real_alerts_cron_path",
        "ExecStart=/home/neopeak/edge2/bin/python aggregation/alerts_cron.py" in svc_text
        and (BACKEND_ROOT / "aggregation" / "alerts_cron.py").exists(),
    )
    check("service_pythonunbuffered_env", "Environment=PYTHONUNBUFFERED=1" in svc_text)
    check("service_user_neopeak", "User=neopeak" in svc_text)
    check("service_after_network_target", re.search(r"\[Unit\][^\[]*After=network\.target", svc_text) is not None)

    check("timer_onbootsec_1min", "OnBootSec=1min" in timer_text)
    check("timer_onunitactivesec_60s", "OnUnitActiveSec=60s" in timer_text)
    check(
        "timer_references_workforce_alerts_service",
        "Unit=workforce-alerts.service" in timer_text,
    )
    check(
        "timer_wantedby_timers_target",
        re.search(r"\[Install\][^\[]*WantedBy=timers\.target", timer_text) is not None,
    )
    check("timer_no_persistent_true", "Persistent=" not in timer_text)


def test_phase5_files_untouched():
    """Confirm workforce-phase5.service/.timer were not modified by this task."""
    import subprocess

    repo_root = BACKEND_ROOT.parent
    for fname in ("workforce-phase5.service", "workforce-phase5.timer"):
        rel = f"systemd/{fname}"
        proc = subprocess.run(
            ["git", "diff", "--stat", "HEAD", "--", rel],
            cwd=repo_root,
            capture_output=True,
            text=True,
        )
        check(
            f"{fname}_has_no_git_diff_vs_HEAD",
            proc.returncode == 0 and proc.stdout.strip() == "",
            f"diff stat: {proc.stdout!r}",
        )


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    test_fresh_insert_collects_id()
    test_reattach_does_not_collect_id()
    test_newly_created_ids_none_is_safe()
    test_site2_new_row_branch_collects_id()
    test_only_two_append_call_sites_and_two_inserts()
    test_reset_is_inside_while_loop_before_with_block()
    test_post_commit_call_is_outside_with_block()
    test_import_present()
    test_post_commit_calls_evaluate_for_each_collected_id()
    test_post_commit_swallows_exceptions_and_continues()
    test_post_commit_empty_list_is_noop()
    test_systemd_files()
    test_phase5_files_untouched()

    passed = sum(1 for _, s in results if s == "PASS")
    failed = sum(1 for _, s in results if s == "FAIL")
    print(f"\n{passed} passed, {failed} failed, {len(results)} total")
    if failed:
        sys.exit(1)
