"""Pallet motion for ICE Teach (FrostTwin, ice_teach/frost_twin) from the ice-frost conveyor model.

The model is observed from outside, through the variables it already publishes (segment FIFOs, bay
platforms), and every change of a pallet's place becomes one command on ice_data_conveyor_twin:

    {"p": 7, "pts": [[x, y, z] or [x, z], ...], "d": 2.0}            (+ "snap": true into the QC bay)
    {"p": 7, "content": [0, 11, 12], "color": "yellow"}              pallet content changed

pallet 7 goes from where it is through the points and reaches the last one in d seconds (d = 0:
placed there). "snap": FrostTwin photographs the pallet when it stops on the quality control
platform (bay 4.2, ice_data_qc_image). Content codes as Pallets/PalletN/contentPosition1-3 (0 empty,
1-3 Duplo 2x2/2x4/2x8, 10 board, 11/12 top/bottom cover, 13 gadget, 14 engraved gadget); the model
has no colours, "color" comes from the bridge parameter cover_colors (pallet -> colour). The durations are the model's own (segment, interchange, bay times of the same
configuration), so the pallet stands still on screen exactly while the model keeps it in place.

Coordinates: pallet-parent local [x, y, z] of the ICE Teach scene, from the constants of its
DrivePallet (belts, slots, switch columns, bay platforms). Belt layout:

    upper z=ZA, x decreasing:  M -S1- L -S2- I -S3- H -S4- LU - F
                               |EM    |LD    |IC    |HB         |FA
    lower z=ZB, x increasing:  E -S8- D -S7- C -S6- B -S5------ A
"""

import json
import re

ZA, ZB = 8.7884, 9.1334    # upper / lower belt
Y, Y_LIFT = 0.8755, 0.896   # belt height, raised on the cross transfers
SWITCH_X = {"M": 9.5338, "E": 9.5338, "L": 7.2108, "D": 7.2108, "I": 3.457, "C": 3.457,
            "H": 1.229, "B": 1.229, "F": -0.2183, "A": -0.2183}
UPPER = set("MLIHF")
# slot 0 (stop gate) .. 3 of each segment (DrivePallet.MovePallet); Segment 4 ends at the load/unload
# zone (x=0.2363, DrivePallet.MoveInLoadUnload), so its queue starts one step before it
SLOTS = {1: (7.5602, 7.8151, 8.0712, 8.3231), 2: (4.002, 4.257, 4.5131, 4.765), 3: (1.687, 1.939, 2.1951, 2.447),
         4: (0.4863, 0.7363, 0.9863), 5: (0.686, 0.434, 0.182, -0.065), 6: (3.062, 2.8528, 2.6004, 2.3493),
         7: (6.905, 6.7001, 6.4471, 6.196), 8: (9.079, 8.8796, 8.63, 8.3789)}
UPSTREAM = {1: "M", 2: "L", 3: "I", 4: "H", 5: "A", 6: "B", 7: "C", 8: "D"}    # switch feeding the segment
DOWNSTREAM = {1: "L", 2: "I", 3: "H", 5: "B", 6: "C", 7: "D", 8: "E"}          # switch at its gate (4: LU)
INTERCHANGE = {"LD": ("L", "D"), "IC": ("I", "C"), "HB": ("H", "B"), "EM": ("M", "E"), "FA": ("F", "A")}
CROSSING = {"L": "LD", "D": "LD", "I": "IC", "C": "IC", "H": "HB", "B": "HB", "E": "EM", "F": "FA"}
BAY_SWITCH = {"Bay1_1": "M", "Bay2": "L", "Bay3": "I", "Bay4": "H", "LoadUnloadZone": "F"}
SHIFT_S = 2.5  # a queue step (0.2-0.25 m): slower than a pallet leaving the gate, so it never closes in
LU = "LoadUnloadZone"
QC = "Bay4_2"  # quality control platform (cell 5 camera), destination 34


def switch(name, y=Y):
    return [SWITCH_X[name], y, ZA if name in UPPER else ZB]


def slot(seg, k):
    s = SLOTS[seg]
    x = s[k] if k < len(s) else s[-1] + (k - len(s) + 1) * (s[1] - s[0])
    return [x, Y, ZA if seg <= 4 else ZB]


def _bay(x, z):
    return [[x, Y_LIFT, ZA], [x, Y_LIFT, z], [x, Y, z]]


# bay platforms (DrivePallet.MoveInBay): off the upper belt at the switch column, towards -z, lifted
# while crossing. Path from the belt switch; the last point is where the pallet stops.
PLATFORM_Z = (8.2939, 7.979, 7.4289)  # platforms 1-3 of bays 2-4; bay 1 (cell 1) is shallower
BAY = {"Bay1_1": _bay(SWITCH_X["M"], 8.4434), LU: [[0.2363, Y, ZA]]}
for _name, _sw in (("Bay2", "L"), ("Bay3", "I"), ("Bay4", "H")):
    BAY[_name + "_1"] = _bay(SWITCH_X[_sw], PLATFORM_Z[0])
    BAY[_name + "_2"] = [[SWITCH_X[_sw], Y, PLATFORM_Z[1]]]
    BAY[_name + "_3"] = [[SWITCH_X[_sw], Y, PLATFORM_Z[2]]]


def bay_of(platform):
    return platform if platform in ("Bay1_1", LU) else platform.rsplit("_", 1)[0]


def cross_path(interchange, frm):
    a, b = INTERCHANGE[interchange]
    if frm != a:
        a, b = b, a
    return [switch(a), switch(a, Y_LIFT), switch(b, Y_LIFT), switch(b)]


def leave_path(platform):
    """From a platform back to its belt switch (the way in, reversed)."""
    return list(reversed(BAY[platform][:-1])) + [switch(BAY_SWITCH[bay_of(platform)])]


class Timings:
    """Model times [s] from the run configuration (gadgetmain.yml), ice-frost defaults otherwise."""

    def __init__(self, config: dict):
        belt = ((config.get("reactors") or {}).get("conveyor") or {}).get("reactors", {}).get("conveyor_belt", {}).get("reactors") or {}

        def par(reactor, name, default, sub=None):
            r = belt.get(reactor) or {}
            if sub is not None:
                r = (r.get("reactors") or {}).get(sub) or {}
            return float((r.get("parameters") or {}).get(name, default)) / 1000

        self.segment = {n: par(f"Segment_{n}", "segment_time", 2000) for n in SLOTS}
        self.interchange = {i: par(i, "interchange_time", 7000) for i in INTERCHANGE}
        self.single_in = {b: par(b, "incoming_time", 2000) for b in ("Bay1_1", LU)}
        self.single_out = {b: par(b, "outgoing_time", 2000) for b in ("Bay1_1", LU)}
        self.lu_to_f = 1.5  # conveyor.lf: LUBay.to_belt -> F.from_bay after 1500 msec
        self.bay_step = 1.5  # seen when the platform is reached (the model has no start event)
        self.bay_entry = 2.0  # the model hands the pallet to platform 1 at once


FIFO_RE = re.compile(r"Segments/Segment(\d)/FIFO/item(\d+)$")
PLATFORM_RE = re.compile(r"Bayes/Bay(\d)/PalletPosition(\d)/palletNum$")
LU_RE = re.compile(r"Bayes/LoadUnloadZone/IndexPosition/palletNum$")
CONTENT_RE = re.compile(r"Pallets/Pallet(\d+)/contentPosition([123])$")


class ConveyorTwin:
    """feed() the conveyor variable updates as they come, tick() once per model update batch;
    commands collect in `out` (JSON strings)."""

    # a pallet out of every FIFO and bay for longer than this is crossing; the model sends a release and
    # the arrival elsewhere in the same update (every 100 ms), so just over one update
    PENDING_S = 0.12

    def __init__(self, timings: Timings, colors=None):
        self.t = timings
        self.colors = {int(p): c for p, c in (colors or {}).items()}
        self.content = {}    # pallet -> [code of position 1, 2, 3]
        self.fifo = {n: [0] * 10 for n in SLOTS}
        self.platform = {}   # platform -> pallet
        # pallet -> ("seg", n, k) | ("bay", platform) | ("cross", switch reached) | ("gone", seg | None, t, platform | None)
        self.where = {}
        self.arrive = {}     # pallet -> time its current move ends
        self.out = []

    def feed(self, path: str, value):
        m = FIFO_RE.search(path)
        if m:
            n, k = int(m[1]), int(m[2])
            if n in self.fifo and k < 10:
                self.fifo[n][k] = int(value or 0)
            return True
        m = PLATFORM_RE.search(path)
        if m:
            bay, pos = int(m[1]), int(m[2])
            self.platform["Bay1_1" if bay == 1 else f"Bay{bay}_{pos}"] = int(value or 0)
            return True
        if LU_RE.search(path):
            self.platform[LU] = int(value or 0)
            return True
        m = CONTENT_RE.search(path)
        if m:
            p = int(m[1])
            c = self.content.setdefault(p, [0, 0, 0])
            c[int(m[2]) - 1] = int(value or 0)
            self.out.append(json.dumps({"p": p, "content": list(c), "color": self.colors.get(p, "")}))
            return True
        return False

    def _send(self, p, pts, d, now):
        cmd = {"p": p, "pts": [[round(v, 4) for v in q] for q in pts], "d": round(max(d, 0.0), 3)}
        if d > 0 and pts[-1] == BAY[QC][-1]:
            cmd["snap"] = True
        self.out.append(json.dumps(cmd))
        self.arrive[p] = now + d

    def _places(self):
        places = {}
        for n, items in self.fifo.items():
            k = 0
            for p in items:
                if p:
                    places[p] = ("seg", n, k)
                    k += 1
        for platform, p in self.platform.items():
            if p:
                places[p] = ("bay", platform)
        return places

    def tick(self, now: float):
        places = self._places()
        for p, new in places.items():
            old = self.where.get(p)
            if old == new:
                continue
            if new[0] == "seg":
                _, n, k = new
                if old is None:
                    self._send(p, [slot(n, k)], 0, now)
                elif old[0] == "seg" and old[1] == n:  # queue moved up
                    self._send(p, [slot(n, k)], max(self.arrive.get(p, 0) - now, SHIFT_S), now)
                else:  # entered the segment (from a gate, an interchange or a bay)
                    platform = old[1] if old[0] == "bay" else old[3] if old[0] == "gone" else None
                    lead = leave_path(platform) if platform else []
                    self._send(p, lead + [switch(UPSTREAM[n]), slot(n, k)], self.t.segment[n], now)
            else:
                platform = new[1]
                bay = bay_of(platform)
                if old is None:
                    self._send(p, BAY[platform][-1:], 0, now)
                elif old[0] == "bay" and bay_of(old[1]) == bay:  # next platform
                    self._send(p, BAY[platform][-1:], self.t.bay_step, now)
                elif bay == LU and old[0] == "gone" or old == ("entering", platform):  # already on its way
                    pass
                elif bay in ("Bay1_1", LU):  # reported on arrival, after the incoming time
                    self._send(p, [switch(BAY_SWITCH[bay])] + BAY[platform], 1.0, now)
                else:
                    self._send(p, [switch(BAY_SWITCH[bay])] + BAY[platform], self.t.bay_entry, now)
            self.where[p] = new

        for p, old in list(self.where.items()):
            if p in places:
                continue
            if old[0] == "seg":  # left its segment and is nowhere yet
                if old[1] == 4:  # Segment 4 ends in the load/unload zone
                    self._send(p, BAY[LU], self.t.single_in[LU], now)
                self.where[p] = ("gone", old[1], now, None)
            elif old[0] == "bay" and bay_of(old[1]) == LU:  # out of the load/unload zone: to F, across FA
                self._send(p, [switch("F")] + cross_path("FA", "F")[1:], self.t.lu_to_f + self.t.interchange["FA"], now)
                self.where[p] = ("cross", "A")
            elif old[0] == "bay":  # back to its belt switch: into a segment at once, or across
                self.where[p] = ("gone", None, now, old[1])
            elif old == ("cross", "M") and now - self.arrive.get(p, now) > self.PENDING_S:
                # across EM and not into Segment 1: switch M sends it to bay 1, reported only on arrival
                self._send(p, BAY["Bay1_1"], self.t.single_in["Bay1_1"], now)
                self.where[p] = ("entering", "Bay1_1")
            elif old[0] == "gone" and old[1] != 4 and now - old[2] > self.PENDING_S:
                # nowhere after a gate or a bay: the switch sent it across the interchange
                sw = DOWNSTREAM[old[1]] if old[1] is not None else BAY_SWITCH[bay_of(old[3])]
                ic = CROSSING.get(sw)
                if ic is None:
                    continue
                lead = leave_path(old[3])[:-1] if old[3] else []
                self._send(p, lead + cross_path(ic, sw), self.t.interchange[ic] - (now - old[2]), now)
                self.where[p] = ("cross", INTERCHANGE[ic][1] if sw == INTERCHANGE[ic][0] else INTERCHANGE[ic][0])
        out, self.out = self.out, []
        return out
