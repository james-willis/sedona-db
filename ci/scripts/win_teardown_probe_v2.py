# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Probe v2 for the Windows wheel shutdown crash.

Probe v1 (synthetic ``python -c`` teardowns) never reproduced the 0xC0000409
process-exit crash, so it needs the real pytest suite's accumulated state. This
harness runs the actual test suite as repeated subprocesses and tallies the
crash exits, across scenarios that isolate the two prime suspects:

* faulthandler: the real CI runs ``pytest -o faulthandler_timeout=600``, which
  spawns a watchdog thread (dump_traceback_later). Compare the real command vs.
  the same suite with faulthandler fully disabled — if the crash only happens
  with the watchdog, the fix is to drop the timeout.
* subsystem: raster/GDAL-heavy tests vs. everything else, to localize which
  teardown leaves the bad native state (or show it is cumulative/interaction).

Usage: win_teardown_probe_v2.py <tests_dir>
Always exits 0 — the signal is the printed tally.
"""

import subprocess
import sys

FASTFAIL = 3221226505  # 0xC0000409, Windows __fastfail / STATUS_STACK_BUFFER_OVERRUN

TESTS = sys.argv[1]

# Each scenario is (label, extra pytest args, reps). `-q -p no:cacheprovider`
# keeps output small; the real CI adds `-o faulthandler_timeout=600`.
SCENARIOS = [
    # Faithful replica of the CI command (faulthandler watchdog thread present).
    ("full_faulthandler", ["-o", "faulthandler_timeout=600"], 12),
    # Same suite, faulthandler plugin fully disabled (no watchdog, no handlers).
    ("full_no_faulthandler", ["-p", "no:faulthandler"], 12),
    # Localize: raster/GDAL-heavy vs. the rest.
    ("raster_only", ["-k", "raster or rst or Raster or gdal or GDAL or tiff or tif"], 10),
    ("no_raster", ["-k", "not (raster or rst or Raster or gdal or GDAL or tiff or tif)"], 10),
]


def base_cmd(extra):
    return [sys.executable, "-m", "pytest", TESTS, "-q", "-p", "no:cacheprovider"] + extra


def run_scenario(label, extra, reps):
    codes = {}
    first_crash_tail = None
    passed_example = None
    for _ in range(reps):
        proc = subprocess.run(base_cmd(extra), capture_output=True, text=True)
        rc = proc.returncode
        codes[rc] = codes.get(rc, 0) + 1
        # Capture the pytest summary line once, to confirm the suite actually ran.
        if passed_example is None:
            for line in reversed(proc.stdout.splitlines()):
                if "passed" in line or "no tests ran" in line:
                    passed_example = line.strip()
                    break
        if rc != 0 and first_crash_tail is None:
            first_crash_tail = (proc.stdout[-600:], proc.stderr[-1200:])
    crashes = sum(n for rc, n in codes.items() if rc != 0)
    print(f"[{label}] {crashes}/{reps} nonzero exits  codes={codes}")
    if passed_example:
        print(f"    (suite ran: {passed_example})")
    if first_crash_tail:
        out, err = first_crash_tail
        print(f"    first-crash stdout tail:\n{out}")
        print(f"    first-crash stderr tail:\n{err}")
    return label, crashes, reps


def main():
    print(f"python: {sys.version}")
    print(f"tests dir: {TESTS}\n")
    results = [run_scenario(*s) for s in SCENARIOS]
    print("\n=== SUMMARY (fastfail=0xC0000409) ===")
    for label, crashes, reps in results:
        print(f"  {label:22s} {crashes}/{reps}")
    sys.exit(0)


if __name__ == "__main__":
    main()
