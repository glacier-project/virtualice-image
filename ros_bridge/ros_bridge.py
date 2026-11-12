"""Kafka lines of the RB-Kairos ROS bridge: robot pose and UR arm joint states.

On the real line these do not go through OPC UA: each robot publishes them from ROS straight to Kafka
(ice_data_rbkairos_<a|b>_pose / _arm, see ICE_DIR/kafka-record/record/raw), only when they change.
pose() and joint() queue the lines in OUTBOX for a Kafka producer. Formats as recorded on the line;
no `effort` field on the arm (a simulated arm has no torques), position.z of the pose is the heading.
"""
import collections
import math

# ponytail: bounded queue, drops the oldest lines when no producer drains it
OUTBOX = collections.deque(maxlen=10000)  # (topic, line), thread-safe append/popleft


def pose(robot: str, x: float, y: float, th: float, ts: int):
    OUTBOX.append((f"ice_data_rbkairos_{robot}_pose",
                   f"RBKairos,category=robot_pose,robot={robot},topic=/robot/robot_local_control/LocalizationComponent/world_frame/pose "
                   f'frame_id="world",position.x={float(x)},position.y={float(y)},position.z={float(th)} {ts}'))


def joint(robot: str, name: str, position: float, velocity: float, ts: int):
    OUTBOX.append((f"ice_data_rbkairos_{robot}_arm",
                   f"RBKairos,category=joint_state,joint=robot_arm_{name}_joint,part=arm,robot={robot},topic=/robot/arm/joint_states "
                   f"position={float(position)},velocity={float(velocity)} {ts}"))


def pose_between(start, target, a):
    """Pose a fraction a in [0, 1] of the way from start to target ([x, y, th]): straight line, heading on
    the shortest arc. ponytail: real paths bend around the line; replay a recorded path if that matters."""
    dth = (target[2] - start[2] + math.pi) % (2 * math.pi) - math.pi
    return [start[0] + a * (target[0] - start[0]), start[1] + a * (target[1] - start[1]),
            (start[2] + a * dth + math.pi) % (2 * math.pi) - math.pi]


if __name__ == "__main__":
    p = pose_between([0, 0, 3.0], [2, 4, -3.0], 0.5)
    assert p[:2] == [1.0, 2.0] and abs(abs(p[2]) - math.pi) < 0.01, p  # across +-pi, not through 0
    pose("a", 1, 2.5, -0.5, 7)
    joint("b", "wrist_1", 0.1, 0, 8)
    assert OUTBOX.popleft() == ("ice_data_rbkairos_a_pose", 'RBKairos,category=robot_pose,robot=a,topic=/robot/robot_local_control/'
                                'LocalizationComponent/world_frame/pose frame_id="world",position.x=1.0,position.y=2.5,position.z=-0.5 7')
    assert OUTBOX.popleft() == ("ice_data_rbkairos_b_arm", "RBKairos,category=joint_state,joint=robot_arm_wrist_1_joint,part=arm,robot=b,"
                                "topic=/robot/arm/joint_states position=0.1,velocity=0.0 8")
    print("ok")
