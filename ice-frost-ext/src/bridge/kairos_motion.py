"""RB-Kairos pose and UR arm for the twin, from the missions of the ice-frost Kairos model.

On the line the robots publish pose and joints from ROS (ros_bridge/); the Frost model has only the
mission variables. A mission starts when Control_Busy rises (name: Ongoing_Mission_Name) and ends when
it falls; meanwhile the pose goes in a straight line to the recorded end pose of that mission
(ros_bridge/kairos_waypoints.json) and the arm replays its recorded motion (kairos_arm_trajectories.json),
both stretched to the mission duration of the run configuration. Lines go to ros_bridge.OUTBOX.
"""

import collections

import ros_bridge
from kairos_arm import ArmReplay

JOINTS = ("shoulder_pan", "shoulder_lift", "elbow", "wrist_1", "wrist_2", "wrist_3")  # trajectory order
POSE_PERIOD, ARM_PERIOD, FULL_PERIOD = 0.5, 0.2, 5.0  # [s]; full state for a twin started late


class KairosMotion:
    def __init__(self, robot: str, waypoints: dict, durations: dict, default_duration: float, arm: dict):
        self.robot = robot                  # "a" / "b"
        self.waypoints = waypoints
        self.durations = durations          # mission -> seconds or [seconds per occurrence]
        self.default_duration = default_duration
        self.pose = list(waypoints.get("initial", [0.0, 0.0, 0.0]))
        self.arm = ArmReplay(arm)
        self.count = collections.Counter()
        self.busy, self.name = False, ""
        self.starting = None                # mission started, name maybe not seen yet (same update)
        self.move = None                    # (start pose, target pose, t0, duration)
        self.sent_arm = {}
        self.next_pose = self.next_arm = self.next_full = 0.0

    def duration(self, mission: str, occurrence: int) -> float:
        d = self.durations.get(mission, self.default_duration)
        return float(d[min(occurrence, len(d) - 1)] if isinstance(d, list) else d)

    def feed(self, variable: str, value, now: float):
        """Ongoing_Mission_Name / Control_Busy updates of this robot."""
        if variable == "Ongoing_Mission_Name":
            self.name = str(value or "")
        elif variable == "Control_Busy" and bool(value) != self.busy:
            self.busy = bool(value)
            if self.busy:
                self.starting = now  # started at the next tick, with the name of the same update
            else:
                if self.starting is not None:
                    self._start(self.starting)
                self._finish(now)

    def _start(self, now):
        self.starting = None
        n = self.count[self.name]
        self.count[self.name] += 1
        d = self.duration(self.name, n)
        target = self.waypoints.get(self.name)
        self.move = (list(self.pose), target, now, d) if target else None
        self.arm.start(self.name, n, now, d)

    def _finish(self, now):
        if self.move:
            self.pose = list(self.move[1])
            self.move = None
        self.arm.stop()
        self.next_pose = self.next_arm = 0.0  # publish the end state now

    def tick(self, now: float, ts: int):
        if self.starting is not None:
            self._start(self.starting)
        if self.move:
            start, target, t0, d = self.move
            self.pose = ros_bridge.pose_between(start, target, min(1.0, (now - t0) / d) if d > 0 else 1.0)
        full = now >= self.next_full
        if full:
            self.next_full = now + FULL_PERIOD
            self.sent_arm.clear()
        if full or now >= self.next_pose and (self.move or self.next_pose == 0.0):
            ros_bridge.pose(self.robot, *self.pose, ts)
            self.next_pose = now + POSE_PERIOD
        if full or now >= self.next_arm and (self.arm.busy or self.next_arm == 0.0):
            self.arm.update(now)
            for name, q, v in zip(JOINTS, self.arm.joints, self.arm.velocities):
                last = self.sent_arm.get(name)
                if last is None or abs(last[0] - q) > 1e-3 or (last[1] == 0) != (v == 0):  # on change, as ROS
                    ros_bridge.joint(self.robot, name, q, v, ts)
                    self.sent_arm[name] = (q, v)
            self.next_arm = now + ARM_PERIOD
