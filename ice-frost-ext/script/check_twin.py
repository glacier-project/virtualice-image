#!/usr/bin/env python3
"""Replays the pallet commands that KafkaBridge recorded (ice_data_conveyor_twin in kafka_bridge.jsonl)
the way FrostTwin moves the Unity pallets, and reports pallets that come closer than MIN_GAP (overlaps
on screen). Two queued pallets sit ~0.2 m apart (slot spacing of the scene), hence MIN_GAP below it.

    python3 script/check_twin.py [kafka_bridge.jsonl]
"""
import json
import math
import sys

MIN_GAP = 0.18
STEP = 0.1  # [s]


class Motion:
    def __init__(self, start, pts, t0, d):
        self.pts = [start] + [p if len(p) == 3 else [p[0], start[1], p[1]] for p in pts]
        self.cum = [0.0]
        for a, b in zip(self.pts, self.pts[1:]):
            self.cum.append(self.cum[-1] + math.dist(a, b))
        self.t0, self.d = t0, d

    def at(self, t):
        total = self.cum[-1]
        if self.d <= 0 or total <= 0:
            return self.pts[-1]
        s = min(1.0, max(0.0, (t - self.t0) / self.d)) * total
        i = 1
        while i < len(self.cum) - 1 and self.cum[i] < s:
            i += 1
        seg = self.cum[i] - self.cum[i - 1]
        u = (s - self.cum[i - 1]) / seg if seg > 0 else 1.0
        return [a + u * (b - a) for a, b in zip(self.pts[i - 1], self.pts[i])]


def main(path):
    cmds = []
    for line in open(path):
        r = json.loads(line)
        if r["topic"] == "ice_data_conveyor_twin":
            cmds.append({**json.loads(r["value"]), "t": r["t"]})
    if not cmds:
        sys.exit(f"no ice_data_conveyor_twin commands in {path}")
    print(len(cmds), "commands, pallets:", sorted({c["p"] for c in cmds}))
    motions, i, t, end, worst = {}, 0, 0.0, cmds[-1]["t"] + 30, {}
    while t <= end:
        while i < len(cmds) and cmds[i]["t"] <= t:
            c = cmds[i]
            m = motions.get(c["p"])
            start = m.at(c["t"]) if m else list(c["pts"][-1])
            motions[c["p"]] = Motion(start, c["pts"], c["t"], c["d"])
            i += 1
        pos = {p: m.at(t) for p, m in motions.items()}
        ps = sorted(pos)
        for a in range(len(ps)):
            for b in range(a + 1, len(ps)):
                gap = math.dist([pos[ps[a]][0], pos[ps[a]][2]], [pos[ps[b]][0], pos[ps[b]][2]])
                if gap < MIN_GAP and ((ps[a], ps[b]) not in worst or gap < worst[ps[a], ps[b]][1]):
                    worst[ps[a], ps[b]] = (t, gap, pos[ps[a]], pos[ps[b]])
        t += STEP
    for (a, b), (t, gap, pa, pb) in sorted(worst.items(), key=lambda kv: kv[1][0]):
        print(f"pallets {a} and {b}: {gap:.3f} m apart at t={t:.1f}s  {[round(v, 3) for v in pa]} {[round(v, 3) for v in pb]}")
    print("no overlaps" if not worst else f"{len(worst)} overlapping pairs")
    return 1 if worst else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "kafka_bridge.jsonl"))
