# ICE Teach on the local stack and on the real line

Tools to run the ICE Teach Unity build (the digital twin of the ICE line). Its data comes from the
Frost line of `../ice-frost-ext` (GadgetMain + KafkaBridge, on the ice-frost models), from a recorded
capture of the real line (`replay.sh`, captures in `~/ICE_DIR/kafka-record/record`) or from the real
line itself (`lab.sh`). The build itself is not in the repository: point `ICE_TEACH_DIR` at it.

```
ice_teach/
├── config.sh           paths: ICE_TEACH_DIR (Unity build), ice-frost-ext and its Python (PY); start_unity
├── demo.sh             fresh run: twin topic, restart Unity, then the Frost gadget demo
├── replay.sh           restart Unity, replay_rpc.py, then a recorded capture of the line (ice_player)
├── lab.sh              restart Unity with -lab: digital shadow of the real line
├── prepare_kafka.py    creates ice_data_conveyor_twin (demo.sh)
├── replay_rpc.py       RPC answers during a replay: last value of each opcua_path seen on Kafka
├── patch_ice_teach.py  byte patches of the stock build (local brokers, message type fixes, ...)
└── frost_twin/
    ├── install.sh      installs everything below into the build: drive | trace | off
    ├── FrostTwin/      C# code loaded by Unity: Driver (pallets follow the Frost conveyor model),
    │                   Endpoints (brokers: local stack or -lab)
    └── Injector/       Mono.Cecil tool: hooks FrostTwin into DrivePallet, KafkaManager, RabbitClient
```

## What Unity receives

KafkaBridge (`../ice-frost-ext/src/KafkaBridge.lf`) publishes the Frost line as the real line does:
every variable the line publishes (`../kafka/contract.json`), the Kairos pose and arm as their ROS
bridge (`../ros_bridge`), and answers Unity's RabbitMQ RPC requests. On top of that it publishes
`ice_data_conveyor_twin`, the pallet motion of the conveyor model, read only by FrostTwin in drive mode.

## Setup (once per build)

Needs the .NET 8 SDK (`~/.dotnet`, e.g. `dotnet-install.sh --channel 8.0 --install-dir ~/.dotnet`)
and WSL interop (`cmd.exe`, `tasklist.exe`).

```sh
export ICE_TEACH_DIR="$HOME/ICE_DIR/purple/VICE_main_builds (2)/VICE_main_builds"   # default
ice_teach/frost_twin/install.sh drive
```

`install.sh` always starts again from `Managed/Assembly-CSharp.dll.orig` (saved by the first
`patch_ice_teach.py` run): it applies the byte patches, copies `FrostTwin.dll`, registers it in
`ScriptingAssemblies.json`, hooks it into the build (see Injector/Program.cs) and writes the mode to
`ICE Teach_Data/FrostTwin.enabled`. Unity must be closed. Modes:

- `drive`: pallets move only on the `ice_data_conveyor_twin` commands; Lego bricks on pallets 1-6;
- `trace`: stock pallet logic (FIFO messages), positions logged to `ICE Teach_Data/FrostTwin.trace.csv`;
- `off`: stock pallet logic. Use it (or `trace`) with `replay.sh` and `lab.sh`: they have no twin topic
  (with `-lab` drive mode falls back to the stock logic by itself).

The byte patches use fixed offsets of this build: run `patch_ice_teach.py` on its own only on the
original assembly, never after `install.sh` (Mono.Cecil rewrites the file).

## Run

Build the Frost line once (see `../ice-frost-ext/README.md`), then:

```sh
ice_teach/demo.sh            # Unity with graphics
ice_teach/demo.sh headless   # -batchmode -nographics
ice_teach/replay.sh <capture.jsonl> [headless]   # e.g. ~/ICE_DIR/kafka-record/record/simple-run/2min/gadget_2m_04_G_CQFAB.jsonl
ice_teach/lab.sh [headless]                      # real line: lab network / VPN
```

Unity has to be restarted for every run (pallets carry over): the scripts do it, and return once
Unity's Kafka consumer has its partitions (`ICE TEACH: Kafka assigned N partitions` in the log).
Unity log: `%TEMP%\iceteach.log`; Frost log: `$FROST_LOG` (default `ice-frost-ext/gadget_unity.log`);
replay RPC log: `ice_teach/replay_rpc.log`.

## Brokers (FrostTwin/Endpoints.cs)

| | `ICE Teach.exe` (default, double click too) | `ICE Teach.exe -lab` |
|---|---|---|
| Kafka | `localhost:9093`; Unity's missing topics are created at start | `10.197.21.10:30529` |
| RabbitMQ RPC | `127.0.0.1:30690` (KafkaBridge or replay_rpc.py) | `10.197.21.10:30690`, **reads only**: every RPC write is refused and logged |
| consumer group | `unity-digitalshadow-<host>-<random>` per run, from the end, no commits | same |

The stock build used the fixed group `unity-digitalshadow-group` from the earliest offset: a restart
replayed everything since its last commit, and two instances (ours and the lab's digital shadow)
split the partitions, each seeing only part of the line. The stock build also writes to the line
by RPC (`bay5Light` reset every time it reads it true, the pallet destination field): `-lab` blocks
both, so the shadow never acts on the plant. RPC reads still reach the line's PLCs, as in the stock
build (`bay5Light` every 2 s, a few reads per mission).
