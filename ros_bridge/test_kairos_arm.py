"""Self-check for src/python_utils/kairos_arm.py.

Run from the repository root:  .venv/bin/python test/test_kairos_arm.py
"""

import json
import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from kairos_arm import ArmReplay  # noqa: E402


def test_replay_stretch_and_occurrences():
    robot = {"rest": [0.0] * 6, "missions": {
        "pick": [[[0.0] + [0.0] * 6, [0.5] + [1.0] * 6, [1.0] + [0.0] * 6],   # out and back
                 [[0.0] + [0.0] * 6, [1.0] + [2.0] * 6]]}}                      # second occurrence
    arm = ArmReplay(robot)
    assert not arm.start("drive", 0, 0.0, 10.0), "missions without a recording leave the arm alone"
    assert arm.start("pick", 0, 100.0, 20.0)  # recorded at any speed, stretched to 20 s
    arm.update(105.0)
    assert abs(arm.joints[0] - 0.5) < 1e-9 and abs(arm.velocities[0] - 0.1) < 1e-9, (arm.joints, arm.velocities)  # 1 rad in 10 s
    arm.update(110.0)
    assert abs(arm.joints[0] - 1.0) < 1e-9
    arm.update(130.0)  # past the end: clamped
    assert abs(arm.joints[0]) < 1e-9
    arm.stop()
    assert arm.velocities == [0.0] * 6 and not arm.busy
    assert arm.start("pick", 5, 0.0, 4.0), "more occurrences than recorded: the last one is replayed"
    arm.update(2.0)
    assert abs(arm.joints[0] - 1.0) < 1e-9


def test_recorded_file():
    data = json.load(open(os.path.join(ROOT, "kairos_arm_trajectories.json")))
    for robot in ("kairos_a", "kairos_b"):
        for mission, runs in data[robot]["missions"].items():
            for run in runs:
                assert run[0][0] == 0.0 and run[-1][0] == 1.0 and all(len(p) == 7 for p in run), (robot, mission)
                assert all(a[0] < b[0] for a, b in zip(run, run[1:])), (robot, mission)
    # the gadget recipe missions are all there (src/GadgetMain.lf)
    assert {"pickCovertopFromWarehouse", "placeBoardToPallet", "shiftRight", "home"} <= data["kairos_a"]["missions"].keys()
    assert {"pickCovertopFromPallet", "dropCoversToTrashBin"} <= data["kairos_b"]["missions"].keys()


if __name__ == "__main__":
    test_replay_stretch_and_occurrences()
    test_recorded_file()
    print("KAIROS ARM OK")
