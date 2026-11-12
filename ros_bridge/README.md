# RB-Kairos ROS bridge

On the real line the Kairos pose and UR arm joints do not go through OPC UA: each robot publishes them
from ROS straight to Kafka (`ice_data_rbkairos_<a|b>_pose`, `ice_data_rbkairos_<a|b>_arm`), only when they
change. The recorded traces already contain them; this folder is for publishing them when they are not
recorded (a simulated mission).

| File | What |
|---|---|
| `ros_bridge.py` | the two line formats as recorded (`pose()`, `joint()` queue `(topic, line)` in `OUTBOX`), `pose_between()` |
| `kairos_arm.py` | `ArmReplay`: replays the arm motion recorded for a mission, stretched to the mission duration |
| `kairos_arm_trajectories.json` | recorded arm motion per robot and mission (`extract_kairos_arm.py`) |
| `kairos_waypoints.json` | pose at the end of each mission in the recorded gadget demo |

Not wired to a source yet: the missions (start, name, end) would come from `ice_data_rbkairos_<a|b>_control`.

    python3 ros_bridge/ros_bridge.py        # self-check
    python3 ros_bridge/test_kairos_arm.py
