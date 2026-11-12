"""Replay of the recorded RB-Kairos arm motion during a mission.

kairos_arm_trajectories.json (extract_kairos_arm.py) holds, per robot and per
mission, one trajectory per recorded occurrence: samples [s, q1..q6] with s in [0, 1] the
fraction of the mission, q the UR joints [rad]. A mission replays the trajectory of the
same occurrence (the last one if it ran more often than recorded), stretched to the actual
mission duration. Missions with no recording leave the arm where it is.
"""

import bisect


class ArmReplay:
    def __init__(self, robot: dict):
        self.missions = robot.get("missions", {})
        self.joints = list(robot.get("rest", [0.0] * 6))
        self.velocities = [0.0] * len(self.joints)
        self._traj = None  # (s values, joint vectors, t0 [s], duration [s])

    @property
    def busy(self) -> bool:
        return self._traj is not None

    def start(self, mission: str, occurrence: int, t0: float, duration: float) -> bool:
        """Start replaying `mission` (0-based occurrence) at time t0. False if it was not recorded."""
        runs = self.missions.get(mission)
        if not runs or duration <= 0:
            return False
        run = runs[min(occurrence, len(runs) - 1)]
        self._traj = ([p[0] for p in run], [p[1:] for p in run], t0, duration)
        return True

    def stop(self):
        """End of the mission: the arm is where the recording ends, and still."""
        if self._traj:
            self.joints = list(self._traj[1][-1])
        self.velocities = [0.0] * len(self.joints)
        self._traj = None

    def update(self, t: float):
        """Move to time t. Velocities are those of the recorded segment, rescaled to the actual mission duration."""
        if not self._traj:
            return
        s_values, qs, t0, duration = self._traj
        s = min(1.0, max(0.0, (t - t0) / duration))
        i = min(max(1, bisect.bisect_left(s_values, s)), len(s_values) - 1)
        sa, sb = s_values[i - 1], s_values[i]
        a = (s - sa) / (sb - sa) if sb > sa else 1.0
        self.joints = [x + a * (y - x) for x, y in zip(qs[i - 1], qs[i])]
        ds = (sb - sa) * duration
        self.velocities = [(y - x) / ds if ds > 0 else 0.0 for x, y in zip(qs[i - 1], qs[i])]
