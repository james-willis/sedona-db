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

"""Diagnostic probe for the Windows wheel shutdown crash.

The Windows wheel job passes every pytest test, then a random subset of
interpreters aborts at *process exit* with 0xC0000409 (Windows __fastfail --
how a Rust abort / corrupted-teardown surfaces). This harness pins down which
teardown path triggers it by running many short-lived subprocesses per
scenario and tallying their exit codes. Each subprocess exercises one teardown
shape (bare import, one connect, connect+query, drop-before-exit, os._exit,
many connects); comparing crash rates across scenarios isolates the cause
without needing a symbolized dump.

Always exits 0 -- read the printed tally from the CI log.
"""

import subprocess
import sys

FASTFAIL = 3221226505  # 0xC0000409, STATUS_STACK_BUFFER_OVERRUN / __fastfail

# Each scenario is a snippet run in its own fresh interpreter. The crash is at
# interpreter finalization, so every repetition must be a separate process.
SCENARIOS = {
    # Static/global teardown only: import auto-configures PROJ + GDAL.
    "import_only": "import sedonadb",
    # One per-session Tokio runtime built, then normal interpreter shutdown.
    "connect": "import sedonadb; sedonadb.connect()",
    # Runtime actually drives a query before shutdown.
    "connect_query": (
        "import sedonadb; sd = sedonadb.connect();"
        " sd.sql('SELECT 1').to_arrow_table()"
    ),
    # Drop the context (runs the RuntimeHandle janitor) *before* finalization.
    "connect_del_gc": (
        "import sedonadb, gc; sd = sedonadb.connect(); del sd; gc.collect()"
    ),
    # Skip CPython finalization + static teardown entirely.
    "connect_osexit": (
        "import sedonadb, os; sedonadb.connect(); os._exit(0)"
    ),
    # Many runtimes alive at once, mirroring a suite that opens many sessions.
    "many_connects": (
        "import sedonadb; ctxs = [sedonadb.connect() for _ in range(8)]"
    ),
}

REPS = 40


def run_scenario(name, snippet):
    codes = {}
    first_crash_stderr = None
    for _ in range(REPS):
        proc = subprocess.run(
            [sys.executable, "-c", snippet],
            capture_output=True,
            text=True,
        )
        rc = proc.returncode
        codes[rc] = codes.get(rc, 0) + 1
        if rc != 0 and first_crash_stderr is None:
            first_crash_stderr = proc.stderr[-2000:]
    crashes = sum(n for rc, n in codes.items() if rc != 0)
    print(f"[{name}] {crashes}/{REPS} nonzero exits  codes={codes}")
    if first_crash_stderr:
        print(f"    first-crash stderr tail:\n{first_crash_stderr}")
    return name, crashes


def main():
    print(f"python: {sys.version}")
    print(f"executable: {sys.executable}")
    print(f"reps per scenario: {REPS}\n")
    results = []
    for name, snippet in SCENARIOS.items():
        results.append(run_scenario(name, snippet))
    print("\n=== SUMMARY (fastfail=0xC0000409) ===")
    for name, crashes in results:
        print(f"  {name:16s} {crashes}/{REPS}")
    # Never fail the job: this is a diagnostic, the signal is the tally above.
    sys.exit(0)


if __name__ == "__main__":
    main()
