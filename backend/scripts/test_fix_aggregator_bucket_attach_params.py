"""
backend/scripts/test_fix_aggregator_bucket_attach_params.py

Regression test for a pre-existing (2026-05-20, commit 1b451c5e), Step-C-
unrelated bug in aggregation/activity_aggregator.py::find_in_progress_bucket_attach()
STEP 2 ("soft live attach"): the SQL had 7 `%s` placeholders but was passed
8 parameters (`*sched[:4]` instead of `*sched[:3]`), causing
`psycopg2.extras.execute()` to raise
"TypeError: not all arguments converted during string formatting" whenever
STEP 1's strict live-attach match missed and execution fell through to
STEP 2 -- crash-looped workforce-aggregator.service on 2026-09-01 and
2026-09-02 against the same real production schedule/zone.

This test uses a fake cursor (no real DB connection) to capture the exact
SQL text and parameter tuple STEP 2 builds, and proves:
  1. The placeholder count in the STEP 2 query text equals the parameter
     count in the tuple passed to execute() -- the actual invariant that
     was violated.
  2. Python's own `%` formatting (the same substitution style psycopg2's
     C extension mirrors for this exact error class) succeeds against the
     query/params STEP 2 now builds, and would raise the exact same
     TypeError against the pre-fix parameter count -- directly
     reproducing the failure mode without needing a live database.
  3. STEP 1 (unaffected, always correct) still has a matching
     placeholder/parameter count of 15.

Run: python scripts/test_fix_aggregator_bucket_attach_params.py
No database connection is opened; no production data is touched.
"""

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import uuid
from datetime import date, datetime, timezone

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


class _FakeCursor:
    """Captures (query, params) pairs passed to execute(); never touches a real DB.

    STEP 1 and STEP 2 are SELECT ... LIMIT 1 lookups -- returning None from
    fetchone() (no match) is exactly what forces execution from STEP 1 into
    STEP 2, and from STEP 2 into the COUNT(*) queries that follow it. Those
    COUNT(*) queries need a dict-shaped row back, so detect them and answer
    accordingly rather than hand-copying the full function's later control
    flow (which is out of scope for this parameter-count regression test).
    """

    def __init__(self):
        self.calls = []
        self._last_query = None

    def execute(self, query, params):
        self.calls.append((query, params))
        self._last_query = query

    def fetchone(self):
        if self._last_query and "COUNT(*)" in self._last_query:
            return {"c": 0}
        return None


def main():
    from aggregation.activity_aggregator import find_in_progress_bucket_attach

    fake_cur = _FakeCursor()
    farm_id = str(uuid.uuid4())
    zone_id = str(uuid.uuid4())
    activity_type_id = 3
    activity_date = date.today()
    schedule_id = str(uuid.uuid4())
    event_time = datetime.now(timezone.utc)

    # STEP 1's SELECT will return no row (fake cursor's fetchone() defaults to
    # None), forcing execution to fall through to STEP 2 -- exactly the code
    # path that used to crash.
    find_in_progress_bucket_attach(
        fake_cur,
        farm_id,
        zone_id,
        activity_type_id,
        activity_date,
        schedule_id,
        event_time,
        event_row_id=None,
    )

    check("at least 2 cur.execute() calls captured (STEP 1 + STEP 2)", len(fake_cur.calls) >= 2,
          detail=f"got {len(fake_cur.calls)}")

    step1_query, step1_params = fake_cur.calls[0]
    step2_query, step2_params = fake_cur.calls[1]
    check("call[0] is STEP 1 (strict live attach, has the CASE ORDER BY)",
          "CASE" in step1_query and "ABS(" in step1_query)
    check("call[1] is STEP 2 (soft live attach, ORDER BY last_seen_at DESC)",
          "ORDER BY last_seen_at DESC" in step2_query)

    step1_placeholders = step1_query.count("%s")
    step2_placeholders = step2_query.count("%s")

    check("STEP 1 (unaffected): placeholder count == param count",
          step1_placeholders == len(step1_params),
          detail=f"placeholders={step1_placeholders} params={len(step1_params)}")
    check("STEP 1 placeholder count is 15 (unchanged)", step1_placeholders == 15,
          detail=f"got {step1_placeholders}")

    check("STEP 2 (the fix): placeholder count == param count",
          step2_placeholders == len(step2_params),
          detail=f"placeholders={step2_placeholders} params={len(step2_params)}")
    check("STEP 2 placeholder count is 7", step2_placeholders == 7,
          detail=f"got {step2_placeholders}")
    check("STEP 2 param count is 7 (was 8 before the fix)", len(step2_params) == 7,
          detail=f"got {len(step2_params)}")

    # Direct reproduction of the actual failure mode: psycopg2's C extension
    # mirrors Python's own %-style substitution for this exact error class.
    # A query with N '%s' and a tuple of N elements always formats cleanly;
    # a tuple with MORE elements than placeholders raises exactly
    # "TypeError: not all arguments converted during string formatting".
    try:
        "x" * step2_placeholders % step2_params if False else None
        # Use the real substitution semantics: build a %s-only string of the
        # same shape and format it with the exact params tuple.
        ("%s " * step2_placeholders) % step2_params
        post_fix_ok = True
        post_fix_error = None
    except TypeError as e:
        post_fix_ok = False
        post_fix_error = str(e)
    check("post-fix params format cleanly against a %s-only string of the same shape",
          post_fix_ok, detail=str(post_fix_error))

    # Prove the OLD (buggy) shape genuinely reproduces the reported crash,
    # confirming this test would have caught the original bug.
    old_step2_params = step2_params + (schedule_id,)  # what *sched[:4] used to add
    try:
        ("%s " * step2_placeholders) % old_step2_params
        old_shape_raised = False
        old_shape_error = None
    except TypeError as e:
        old_shape_raised = True
        old_shape_error = str(e)
    check("pre-fix shape (8 params vs 7 placeholders) reproduces the exact reported TypeError",
          old_shape_raised and "not all arguments converted" in (old_shape_error or ""),
          detail=str(old_shape_error))

    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)


if __name__ == "__main__":
    main()
