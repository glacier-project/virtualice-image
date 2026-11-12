"""Self-check of the KafkaBridge helpers (src/bridge). From ice-frost-ext:  python3 test/test_bridge.py"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path[:0] = [os.path.join(HERE, "..", "src", "bridge"), os.path.join(HERE, "..", "..", "ros_bridge"),
                os.path.join(HERE, "..", "..", "utils")]

import conveyor_twin as ct  # noqa: E402
import ros_bridge  # noqa: E402
from kairos_motion import KairosMotion  # noqa: E402

FIFO = "Objects/ConveyorHMI/ConveyorObjects/Segments/Segment{}/FIFO/item{}"
BAY = "Objects/ConveyorHMI/ConveyorObjects/Bayes/Bay{}/PalletPosition{}/palletNum"
LU = "Objects/ConveyorHMI/ConveyorObjects/Bayes/LoadUnloadZone/IndexPosition/palletNum"


def cmds(twin, now):
    return [json.loads(c) for c in twin.tick(now)]


def test_conveyor_twin():
    t = ct.Timings({"reactors": {"conveyor": {"reactors": {"conveyor_belt": {"reactors": {
        "Segment_2": {"parameters": {"segment_time": 20200}}, "HB": {"parameters": {"interchange_time": 4800}}}}}}}})
    assert t.segment[2] == 20.2 and t.segment[1] == 2.0 and t.interchange["HB"] == 4.8 and t.interchange["LD"] == 7.0
    twin = ct.ConveyorTwin(t)
    for k, p in enumerate((7, 6)):
        twin.feed(FIFO.format(1, k), p)
    twin.feed(FIFO.format(3, 0), 9)
    twin.feed(FIFO.format(4, 0), 5)
    start = cmds(twin, 1.0)
    assert {c["p"]: (c["pts"], c["d"]) for c in start}[6] == ([ct.slot(1, 1)], 0)  # initial layout

    # gate of Segment 1 opens: 7 goes on to Segment 2 (same update), 6 moves up
    twin.feed(FIFO.format(1, 0), 6), twin.feed(FIFO.format(1, 1), 0), twin.feed(FIFO.format(2, 0), 7)
    c = {x["p"]: x for x in cmds(twin, 2.0)}
    assert c[7]["pts"] == [ct.switch("L"), ct.slot(2, 0)] and c[7]["d"] == 20.2
    assert c[6]["pts"] == [ct.slot(1, 0)] and c[6]["d"] == ct.SHIFT_S

    # 9 leaves Segment 3 and is nowhere: across HB once the decision time is over
    twin.feed(FIFO.format(3, 0), 0)
    assert cmds(twin, 3.0) == []
    (c,) = cmds(twin, 3.0 + 2 * twin.PENDING_S)
    assert c["pts"] == ct.cross_path("HB", "H") and c["pts"][-1] == ct.switch("B")
    twin.feed(FIFO.format(6, 0), 9)  # arrives on Segment 6
    (c,) = cmds(twin, 8.0)
    assert c["pts"] == [ct.switch("B"), ct.slot(6, 0)]

    # 5 leaves Segment 4 into the load/unload zone, later out of it across FA
    twin.feed(FIFO.format(4, 0), 0)
    (c,) = cmds(twin, 9.0)
    assert c["pts"] == ct.BAY[ct.LU] and c["d"] == 2.0
    twin.feed(LU, 5)
    assert cmds(twin, 11.0) == []  # arrival already drawn
    twin.feed(LU, 0)
    (c,) = cmds(twin, 20.0)
    assert c["pts"][-1] == ct.switch("A") and c["d"] == 1.5 + 7.0

    # 6 into bay 3, platform 1 -> 2, back to 1, out across IC (the switch of bay 3 is I)
    twin.feed(FIFO.format(1, 0), 0), twin.feed(BAY.format(3, 1), 6)
    (c,) = cmds(twin, 21.0)
    assert c["pts"][-1] == ct.BAY["Bay3_1"][-1]
    twin.feed(BAY.format(3, 1), 0), twin.feed(BAY.format(3, 2), 6)
    assert cmds(twin, 22.0)[0]["pts"] == ct.BAY["Bay3_2"]
    twin.feed(BAY.format(3, 2), 0), twin.feed(BAY.format(3, 1), 6)
    cmds(twin, 23.0)
    twin.feed(BAY.format(3, 1), 0)
    cmds(twin, 24.0)
    (c,) = cmds(twin, 24.5)
    assert c["pts"][-1] == ct.switch("C") and c["pts"][0][2] < ct.ZA  # out of the bay, then across

    # 4 at the gate of Segment 8: across EM to M, then into bay 1 without waiting for its arrival report
    twin.feed(FIFO.format(8, 0), 4)
    cmds(twin, 30.0)
    twin.feed(FIFO.format(8, 0), 0)
    cmds(twin, 30.1)
    (c,) = cmds(twin, 30.3)
    assert c["pts"][-1] == ct.switch("M")
    assert cmds(twin, 30.3 + c["d"]) == []  # still deciding: Segment 1 or bay 1
    (c,) = cmds(twin, 30.4 + c["d"] + twin.PENDING_S)
    assert c["pts"] == ct.BAY["Bay1_1"] and c["d"] == 2.0
    twin.feed(BAY.format(1, 1), 4)
    assert cmds(twin, 40.0) == []  # the arrival is already drawn
    assert ct.BAY["Bay1_1"][-1][2] == 8.4434 and ct.BAY["Bay2_1"][-1][2] == 8.2939  # DrivePallet.MoveInBay


def test_kairos_mission():
    ros_bridge.OUTBOX.clear()
    k = KairosMotion("a", {"initial": [0, 0, 0], "go": [2, 0, 0]}, {"go": [10, 20]}, 5.0,
                     {"rest": [0.0] * 6, "missions": {}})
    k.tick(0.0, 0)
    assert ros_bridge.OUTBOX[0][0] == "ice_data_rbkairos_a_pose"
    k.feed("Control_Busy", True, 1.0)  # busy before the name, in the same update
    k.feed("Ongoing_Mission_Name", "go", 1.0)
    k.tick(6.0, 0)
    assert abs(k.pose[0] - 1.0) < 1e-9, k.pose  # half way through the 10 s of the first occurrence
    k.feed("Control_Busy", False, 11.0)
    assert k.pose == [2, 0, 0]
    k.feed("Control_Busy", True, 12.0)
    k.tick(12.0, 0)
    assert k.move[3] == 20  # second occurrence


def test_content_and_qc_snap():
    twin = ct.ConveyorTwin(ct.Timings({}), {7: "yellow"})
    twin.feed("Objects/ConveyorHMI/ConveyorObjects/Pallets/Pallet7/contentPosition2", 11)
    twin.feed(FIFO.format(4, 0), 7)
    c = cmds(twin, 1.0)
    assert c[0] == {"p": 7, "content": [0, 11, 0], "color": "yellow"} and "snap" not in c[1]  # initial layout
    twin.feed(FIFO.format(4, 0), 0), twin.feed(BAY.format(4, 1), 7)
    (c,) = cmds(twin, 5.0)
    assert "snap" not in c  # platform 1: no camera
    twin.feed(BAY.format(4, 1), 0), twin.feed(BAY.format(4, 2), 7)
    (c,) = cmds(twin, 7.0)
    assert c["pts"] == ct.BAY["Bay4_2"] and c["snap"] is True  # quality control platform


if __name__ == "__main__":
    test_conveyor_twin()
    test_content_and_qc_snap()
    test_kairos_mission()
    print("BRIDGE OK")
