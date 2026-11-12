#!/usr/bin/env python3
"""Compare a Frost run of the gadget recipe with the recorded ice-sim trace.

Usage:
    python3 script/compare_trace.py <eventi.log> <frost_events.log> [--tol 5]

<eventi.log>        output of ICE_DIR/linea_fittizia/live.py (any speed). Its FASE lines
                    give the trace time of each wall-clock timestamp.
<frost_events.log>  written by Recorder.lf (src/GadgetRecorded.lf).

Both logs are mapped to the same machine/variable names. Operations (machine
missions) are derived the same way from both, then matched in order per
machine and name. The report gives start and duration deltas, then the value
sequences of the key state variables (QC results, tray in bay, pallet arrivals).
Operations are also derived from the trace's ground-truth FASE labels, to
check the derivation itself.
"""

import argparse
import re
from collections import defaultdict

TRACE_MACHINES = {"fufi": "kairos_a", "cram": "kairos_b", "ETENSIL": "etensil"}
# Frost (ice-frost data models) path prefix -> trace (real OPC UA) path prefix
FROST_PREFIX = [("Objects/", ""), ("Cell1/", ""), ("Cell2/MES/", "OPCUA/MES/")]
NAME_MAP = {"StartTest": "TestingBoard"}  # cell1 mission names: ice-frost model -> real machine
# trace value -> ice-frost value, where the real machine uses another type
TRACE_VALUE_MAP = {("cell1", "FlyingProbeTester/LastResult"): {"success": True, "": False}}

KAIROS_MISSION = "RBKairos/Mission_Methods/Ongoing_Mission_Name"
OPS = {  # machine -> (variable, kind, op name for boolean kinds)
    "kairos_a": (KAIROS_MISSION, "name", None),
    "kairos_b": (KAIROS_MISSION, "name", None),
    "cell1": ("FlyingProbeTester/CurrentMission", "name", None),
    "cell2": ("OPCUA/MES/CurrentMission", "name", None),
    "cell5": ("Cell5/Status/CurrentMission", "name", None),
    "cell4": ("Cell_4_Robot_Coordinator_PLC/DataBlocksGlobal/DB_ROBOTS/plcOperationRunning", "bool", "plcOperation"),
    "etensil": ("eTensil/MES/busy", "bool", "screw"),
}
WAREHOUSE_CALLS, WAREHOUSE_TRAY = "Warehouse/Statistics/TrayCallsCount", "Warehouse/Status/trayInBay"
SEQUENCES = [  # (label, machine, variable regex)
    ("cell5 LastResult", "cell5", r"^Cell5/Status/LastResult$"),
    ("cell1 LastResult", "cell1", r"^FlyingProbeTester/LastResult$"),
    ("warehouse trayInBay", "warehouse", r"^Warehouse/Status/trayInBay$"),
    ("pallet arrivals (bay = pallet)", "conveyor", r"Destinations/destination_palletid_"),
]


def parse_value(v):
    v = v.strip()
    if v in ("True", "False"):
        return v == "True"
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except ValueError:
        return v


def wall(s):
    h, m, x = s.split(":")
    return int(h) * 3600 + int(m) * 60 + float(x)


def load_trace(path):
    """Return (events [(t, machine, var, value)], ground-truth ops)."""
    lines = open(path, encoding="utf-8", errors="replace").read().splitlines()
    anchors = [(wall(l[:12]), float(re.search(r"t=([\d.]+)s", l).group(1))) for l in lines if " FASE " in l]
    n = len(anchors)
    mx, my = sum(a for a, _ in anchors) / n, sum(b for _, b in anchors) / n
    k = sum((a - mx) * (b - my) for a, b in anchors) / sum((a - mx) ** 2 for a, _ in anchors)
    q = my - k * mx
    events, truth, active = [], [], {}
    for l in lines:
        if " FASE " in l and "operations:" in l:
            t = float(re.search(r"t=([\d.]+)s", l).group(1))
            cur = {x.strip() for x in l.split("operations:")[1].split(",")} - {"", "-"}
            for op in [o for o in active if o not in cur]:
                truth.append((active.pop(op), t, op))
            for op in cur - active.keys():
                active[op] = t
            continue
        m = re.match(r"(\S+)\s+(\S+)\s+(.+?) = (.*)$", l)
        if m:
            mach, var, val = TRACE_MACHINES.get(m.group(2), m.group(2)), m.group(3), parse_value(m.group(4))
            val = TRACE_VALUE_MAP.get((mach, var), {}).get(val, val)
            events.append((k * wall(m.group(1)) + q, mach, var, val))
    return sorted(events, key=lambda e: e[0]), sorted(truth)


def load_frost(path):
    events = []
    for l in open(path, encoding="utf-8"):
        m = re.match(r"([\d.]+)\s+(\S+)\s+(.+?) = (.*)$", l.rstrip("\n"))
        if not m:
            continue
        var = m.group(3)
        for a, b in FROST_PREFIX:
            if var.startswith(a):
                var = b + var[len(a):]
                break
        events.append((float(m.group(1)), m.group(2), var, parse_value(m.group(4))))
    return sorted(events, key=lambda e: e[0])


def derive_ops(events):
    """[(start, end, "machine:op")] from mission-name / busy variables."""
    ops, open_ops = [], {}
    calls_t = None
    for t, mach, var, val in events:
        if mach == "warehouse":
            if var == WAREHOUSE_CALLS:
                calls_t = t
            elif var == WAREHOUSE_TRAY and val and calls_t is not None:
                ops.append((calls_t, t, "warehouse:extractTray"))
                calls_t = None
            continue
        spec = OPS.get(mach)
        if not spec or var != spec[0]:
            continue
        _, kind, bool_name = spec
        name = (bool_name if val is True else "") if kind == "bool" else NAME_MAP.get(str(val), str(val))
        prev = open_ops.pop(mach, None)
        if prev and prev[1] != name:
            ops.append((prev[0], t, f"{mach}:{prev[1]}"))
        elif prev:
            open_ops[mach] = prev
            continue
        if name:
            open_ops[mach] = (t, name)
    return sorted(ops)


def by_occurrence(ops):
    seen, out = defaultdict(int), {}
    for a, b, op in ops:
        out[(op, seen[op])] = (a, b)
        seen[op] += 1
    return out


def compare_ops(ref_ops, run_ops, tol, ref_label, run_label):
    ref, run = by_occurrence(ref_ops), by_occurrence(run_ops)
    print(f"{'operation':44s} {ref_label + ' start':>11s} {'dur':>6s} {run_label + ' start':>11s} {'dur':>6s} {'dStart':>7s} {'dDur':>6s}")
    dstarts, ddurs = [], []
    for key in sorted(set(ref) | set(run), key=lambda k: (ref.get(k) or run.get(k))[0]):
        op, i = key
        label = f"{op}" + (f" #{i + 1}" if i else "")
        r, f = ref.get(key), run.get(key)
        if r and f:
            ds, dd = f[0] - r[0], (f[1] - f[0]) - (r[1] - r[0])
            dstarts.append(abs(ds)); ddurs.append(abs(dd))
            flag = "  <-- off" if abs(dd) > tol else ""
            print(f"{label:44s} {r[0]:11.1f} {r[1] - r[0]:6.1f} {f[0]:11.1f} {f[1] - f[0]:6.1f} {ds:+7.1f} {dd:+6.1f}{flag}")
        elif r:
            print(f"{label:44s} {r[0]:11.1f} {r[1] - r[0]:6.1f} {'MISSING':>11s}")
        else:
            print(f"{label:44s} {'EXTRA':>11s} {'':6s} {f[0]:11.1f} {f[1] - f[0]:6.1f}")
    missing = len(set(ref) - set(run)); extra = len(set(run) - set(ref))
    if dstarts:
        print(f"\nmatched {len(dstarts)}/{len(ref)}  missing {missing}  extra {extra}  "
              f"mean |dStart| {sum(dstarts) / len(dstarts):.1f} s  mean |dDur| {sum(ddurs) / len(ddurs):.1f} s  "
              f"durations off by > {tol} s: {sum(d > tol for d in ddurs)}")


def sequence(events, machine, pattern):
    out, last = [], {}
    for t, mach, var, val in events:
        if mach != machine or not re.search(pattern, var):
            continue
        if "destination_palletid_" in var:
            if val:  # arrival: bay <- pallet
                out.append((t, f"{var.rsplit('_palletid_', 1)[1]}={val}"))
            continue
        if last.get(var) != val:
            last[var] = val
            out.append((t, val))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("trace")
    ap.add_argument("frost")
    ap.add_argument("--tol", type=float, default=5.0, help="duration tolerance [s] for flagging (default 5)")
    a = ap.parse_args()

    trace_events, truth = load_trace(a.trace)
    frost_events = load_frost(a.frost)
    trace_ops = derive_ops(trace_events)
    frost_ops = derive_ops(frost_events)
    if trace_ops and frost_ops:  # the trace has idle time before the recipe: align the first operation
        shift = trace_ops[0][0] - frost_ops[0][0]
        frost_events = [(t + shift, m, v, x) for t, m, v, x in frost_events]
        frost_ops = derive_ops(frost_events)
        print(f"(frost times shifted by {shift:+.1f} s to align the first operation)\n")

    print("=== Derivation check: trace operations from variables vs ground-truth FASE labels")
    compare_ops(truth, trace_ops, a.tol, "FASE", "vars")

    print("\n=== Operations: trace vs Frost")
    compare_ops(trace_ops, frost_ops, a.tol, "trace", "frost")

    print("\n=== Key value sequences (time [s]: value)")
    for label, machine, pattern in SEQUENCES:
        r, f = sequence(trace_events, machine, pattern), sequence(frost_events, machine, pattern)
        same = [x for _, x in r] == [x for _, x in f]
        print(f"\n{label}: {'same sequence' if same else 'DIFFERENT sequence'}")
        print("  trace: " + ", ".join(f"{t:.0f}:{v}" for t, v in r))
        print("  frost: " + ", ".join(f"{t:.0f}:{v}" for t, v in f))


if __name__ == "__main__":
    main()
