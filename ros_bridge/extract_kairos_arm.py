#!/usr/bin/env python3
"""Extract the RB-Kairos arm trajectories of each mission from an MQTT recording of the gadget demo.

Usage:
    python3 ros_bridge/extract_kairos_arm.py ~/ICE_DIR/remote-ice-controller/traces/demo_v4b_complete.csv \
        ros_bridge/kairos_arm_trajectories.json

The recording (mqtt-recorder CSV: topic, payload, qos, retain, timestamp, delta) has
ice/kairos_<aa|ab>/status with the mission name and ice/kairos_<aa|ab>/arm with the six UR
joints [rad]. Each mission becomes one trajectory: samples [s, q1..q6] with s = time / mission
duration in [0, 1], resampled every `--step` of s so kairos_arm.ArmReplay can stretch it to a mission's own
duration. Repeated missions keep one trajectory per occurrence, in order.
"""

import argparse
import bisect
import csv
import json

ROBOTS = {"aa": "kairos_a", "ab": "kairos_b"}
JOINTS = ["shoulder_pan", "shoulder_lift", "elbow", "wrist_1", "wrist_2", "wrist_3"]


def resample(samples, t0, t1, n):
    """n + 1 joint vectors at evenly spaced times in [t0, t1], linear interpolation."""
    times = [t for t, _ in samples]
    out = []
    for k in range(n + 1):
        t = t0 + (t1 - t0) * k / n
        i = bisect.bisect_left(times, t)
        if i == 0:
            q = samples[0][1]
        elif i == len(samples):
            q = samples[-1][1]
        else:
            (ta, qa), (tb, qb) = samples[i - 1], samples[i]
            a = (t - ta) / (tb - ta) if tb > ta else 0.0
            q = [x + a * (y - x) for x, y in zip(qa, qb)]
        out.append([round(k / n, 4)] + [round(x, 5) for x in q])
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("recording")
    ap.add_argument("out")
    ap.add_argument("--step", type=float, default=0.2, help="resampling step [s of recording] (default 0.2)")
    a = ap.parse_args()

    arm = {k: [] for k in ROBOTS}
    status = {k: [] for k in ROBOTS}
    for row in csv.reader(open(a.recording, newline="")):
        if len(row) < 5:
            continue
        for k in ROBOTS:
            if row[0] == f"ice/kairos_{k}/arm":
                arm[k].append((float(row[4]), json.loads(row[1])["position"]))
            elif row[0] == f"ice/kairos_{k}/status":
                status[k].append((float(row[4]), json.loads(row[1]).get("currentMission", "")))

    out = {"joints": JOINTS, "source": a.recording.rsplit("/", 1)[-1]}
    for k, robot in ROBOTS.items():
        samples = sorted(arm[k])
        if not samples:
            continue
        missions, current = {}, None
        for t, name in status[k]:
            if name:
                current = (name, t)
            elif current:
                name, t0 = current
                n = max(1, round((t - t0) / a.step))
                missions.setdefault(name, []).append(resample(samples, t0, t, n))
                current = None
        out[robot] = {"rest": [round(x, 5) for x in samples[0][1]], "missions": missions}
        print(robot, {m: len(v) for m, v in missions.items()})
    json.dump(out, open(a.out, "w"), separators=(",", ":"))


if __name__ == "__main__":
    main()
