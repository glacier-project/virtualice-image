# ice-frost-ext

LF reactors and programs built on the ice-frost models (`../ice-frost`, submodule, imported as is) for
this stack. ice-frost keeps only the models of the line; demos, recordings, trace comparisons and the
publishing on Kafka/RabbitMQ live here.

| File | What |
|---|---|
| `src/GadgetMain.lf` | gadget demo on the ice-frost models, with Recorder and KafkaBridge |
| `src/KafkaBridge.lf` | publishes the line as the real ICE line does (see below); no-op without brokers |
| `src/bridge/bridge_map.py` | Frost data model paths ↔ real OPC UA paths, contract lines, RPC routing |
| `src/bridge/bridge_bus.py` | Kafka producer and RabbitMQ RPC queues, on an asyncio thread |
| `src/bridge/conveyor_twin.py` | pallet motion for FrostTwin, observed from the conveyor model's FIFOs and bays |
| `src/bridge/kairos_motion.py` | Kairos pose and arm from the missions (`../ros_bridge` formats and recordings) |
| `src/Recorder.lf` | writes every update of the variables in `resources/recorder_watch.yml` to `frost_events.log` |
| `resources/gadgetmain.yml` | run configuration: model timings measured on the recorded run, bridge settings |
| `script/compare_trace.py` | compares `frost_events.log` with a recorded trace (`ICE_DIR/linea_fittizia/eventi.log`) |
| `script/check_twin.py` | replays the pallet commands of `kafka_bridge.jsonl` and reports overlaps |
| `test/test_bridge.py` | self-check of the bridge helpers |

KafkaBridge observes the machines through the Frost link, like Recorder, and publishes:
- every variable the real line publishes (`../kafka/contract.json`), as the line writes it, on change,
  plus the current values at start: `python3 utils/kafka_contract.py check ice-frost-ext/kafka_bridge.jsonl`;
- the RB-Kairos pose and arm as their ROS bridge does (`../ros_bridge`), driven by the Kairos missions;
- answers to ICE Teach's RabbitMQ RPC requests (reads, writes, methods) on `rpc_queues`;
- `ice_data_conveyor_twin` (not a line topic): pallet motion for FrostTwin (`../ice_teach`).
Every published message is also written to `kafka_bridge.jsonl`.

`lf-packages` links to the frost package of the ice-frost submodule. Setup and run from this folder:

    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    source .venv/bin/activate && lfc src/GadgetMain.lf
    .venv/bin/python src-gen/GadgetMain/GadgetMain.py                 # offline, fast
    python3 script/compare_trace.py ~/ICE_DIR/linea_fittizia/eventi.log frost_events.log
    python3 script/check_twin.py && python3 test/test_bridge.py

With the stack (`source ../.envrc`), `--fast false` publishes in real time; `../ice_teach/demo.sh` does
that with ICE Teach.
