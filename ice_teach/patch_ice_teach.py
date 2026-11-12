#!/usr/bin/env python3
"""Byte patches that make the ICE Teach Unity build work with the local ice-frost / virtualice-image stack.

Usage:  python3 patch_ice_teach.py [--check] ["<build dir>"]
        (default build dir: $ICE_TEACH_DIR, see config.sh). Normally run by frost_twin/install.sh.

Every patch checks the original bytes first, so a different build is refused, not corrupted.
Already patched sites are skipped; the first run saves <file>.orig next to each patched file.
All patches keep the instruction stream length, so no offsets move. Port them to the Unity
sources when available (method names below).

level0 (scene):
  L1  KafkaManager.UseLocal = 1   Kafka at localhost:9093 instead of the lab cluster
  L2  RabbitClient.UseLocal = 1   RabbitMQ at 127.0.0.1 instead of 10.197.21.10 (the real line!)
  L3/L4  "LegoBrick 7" / "LegoBrick 6" inactive: static Lego bricks on pallets 7 and 6, left on in the
         scene. Pallet content is not rendered (DrivePallet.FillPalletContentInfo is empty), so they
         showed on any recipe; pallet 7 carries the covers in the gadget demo.

Assembly-CSharp.dll:
  KafkaManager.<HandleRoboticCell>d__42.MoveNext (ice_data_cell4_state)
  A1/A2  ABB_Active / Kuka_Active: the line-protocol parser keeps field values as strings, so
         `isinst Boolean` always failed and the "mission started" branch never ran.
         -> Convert.ToBoolean(object) ("True"/"False").
  B1/B2  ABB_programID / KUKA_programID: RPC values are deserialized as Dictionary<string, object>
         (Newtonsoft: integers are Int64), `unbox.any Int32` threw. -> unbox.any Int64; conv.i4.
  C1/C2  plcOperationError: false (no error) was shown as "fail". -> inverted test.
  RabbitClient.<CheckVariable>d__20.MoveNext (bay5Light polling)
  D1     expected the value as the ToString() of a JSON list ("[\\r\\n  1\\r\\n]", '1' at index 5);
         the RPC server answers a scalar. -> length >= 1 and character 0.
  KafkaManager.HandleKairosBusy (ice_data_rbkairos_<a|b>_control, field Control_Busy)
  E1     looked for "Control_Busy=true" inside the field value, which the parser has already split
         on '=': the Kairos was never busy. -> looks for "true" (the line writes Control_Busy=true).
  KafkaManager.HandleConveyorFifos (ice_data_conveyor_fifos)
  F1     parsed the pallet's slot in the segment queue (FIFO item index) and then forced it to 3.
         DrivePallet spawns a pallet at (segment, slot) on its first update, so two pallets first seen
         in the same segment spawned on the same point and blocked each other's colliders for good
         (all pallets jammed on one segment). -> the real slot is kept, as on the MQTT path
         (positions 2 -> 1 -> 0 while a pallet moves up the queue).
  DrivePallet.Update
  G1     pallet speed = travelled distance / newDataElapsedSeconds, the time since the pallet's previous
         position change: a pallet that waited 60 s at a stop gate then took 60 s to cross the next
         segment, while the data moved the pallets behind it in seconds -> pallets piled up and crossed
         switches together; a fixed move time made long catch-up moves race. -> inferredSpeed = 0, so
         Update falls back to GetSegmentSpeed: the real belt speeds of the build (speedsForSegment,
         0.17-0.22 m/s, switches 0.10-0.15). ice-frost src/Conveyor/segment.lf uses the same lengths and
         speeds for its segment travel times, and the pallet enters the next FIFO when it leaves the
         switch, so the twin moves in step with the model.
  DrivePallet.OnTriggerEnter
  H1     "diagonal" rule: block if the other pallet is ahead in both x and z (or behind in both) and not
         on a switch column. It ignores the moving direction, so it is symmetric: two pallets meeting
         diagonally (cross connectors at x=0.896, corners) blocked each other, and a block is only
         released by OnTriggerExit -> permanent jam. -> rule off; the direction/priority rule after it
         (only the follower or the lower priority stops) still keeps pallets apart.
"""

import argparse
import os
import shutil
import sys


# (file, label, method or None, IL offset or absolute file offset, original bytes, patched bytes)
NOP = "00 "


def h(s):
    return bytes.fromhex(s)


ABB_ACTIVE = ("11 04 75 dd 00 00 01 2c 0c 11 04 a5 dd 00 00 01 13 05", "11 04 28 90 03 00 0a 13 05" + " 00" * 9)
KUKA_ACTIVE = ("11 0b 75 dd 00 00 01 2c 0c 11 0b a5 dd 00 00 01 13 0c", "11 0b 28 90 03 00 0a 13 0c" + " 00" * 9)
# brfalse.s; ldloc.s n; unbox.any Int32; ldc.i4.3; bne.un.s; ldstr DemoGadget; stloc.s m; br.s; ldstr DemoLego; stloc.s m
ABB_PROG = ("2c 13 11 07 a5 c9 00 00 01 19 33 09 72 c9 e7 00 70 13 06 2b 07 72 df e7 00 70 13 06",
            "2c 12 11 07 a5 f4 00 00 01 69 19 33 07 72 c9 e7 00 70 2b 05 72 df e7 00 70 13 06 00")
KUKA_PROG = ("2c 13 11 0e a5 c9 00 00 01 19 33 09 72 c9 e7 00 70 13 0d 2b 07 72 df e7 00 70 13 0d",
             "2c 12 11 0e a5 f4 00 00 01 69 19 33 07 72 c9 e7 00 70 2b 05 72 df e7 00 70 13 0d 00")
ABB_ERR = ("11 0a a5 dd 00 00 01 2c 09 72 2b 2e 00 70", "11 0a a5 dd 00 00 01 2d 09 72 2b 2e 00 70")
KUKA_ERR = ("11 10 a5 dd 00 00 01 2c 09 72 2b 2e 00 70", "11 10 a5 dd 00 00 01 2d 09 72 2b 2e 00 70")
CELL4 = "<HandleRoboticCell>d__42.MoveNext"
CHECK = "<CheckVariable>d__20.MoveNext"
KAIROS_BUSY = "KafkaManager.HandleKairosBusy"
FIFOS = "KafkaManager.HandleConveyorFifos"
DRIVE = "DrivePallet.Update"
TRIGGER = "DrivePallet.OnTriggerEnter"

DLL_PATCHES = [
    ("A1", CELL4, 0x0096, *ABB_ACTIVE), ("A2", CELL4, 0x026F, *KUKA_ACTIVE),
    ("B1", CELL4, 0x0135, *ABB_PROG), ("B2", CELL4, 0x030E, *KUKA_PROG),
    ("C1", CELL4, 0x01FE, *ABB_ERR), ("C2", CELL4, 0x03D7, *KUKA_ERR),
    ("D1", CHECK, 0x00DF, "1c 2f", "17 2f"), ("D1", CHECK, 0x0120, "1b 17 6f", "16 17 6f"),
    ("E1", KAIROS_BUSY, 0x0001, "72 70 16 00 70 6f", "72 94 16 00 70 6f"),  # ldstr "Control_Busy=true" -> "true"
    ("F1", FIFOS, 0x00EF, "2d 01 2a 19 13 09 28", "2d 01 2a 00 00 00 28"),  # ldc.i4.3; stloc.s 9 -> nop x3
    # ldloc.s 6; or; brfalse.s -> pop; ldc.i4.0; nop; brfalse.s (always skips the diagonal block)
    ("H1", TRIGGER, 0x0108, "11 06 60 2c 0f", "26 16 00 2c 0f"),
    # ldarg.0; ldfld lastMovementDistance; ldarg.0; ldfld newDataElapsedSeconds; div -> ldc.r4 0.0; nop x8
    ("G1", DRIVE, 0x01E6, "02 7b 25 02 00 04 02 7b 1f 02 00 04 5b", "22 00 00 00 00" + " 00" * 8),
]
LEVEL0_PATCHES = [("L1", 89818688, "00", "01"), ("L2", 89796608, "00", "01"),
                  ("L3", 89346326, "01", "00"), ("L4", 89417174, "01", "00")]  # GameObject.m_IsActive


# IL code offsets in the exact build described above. apply() checks every site's bytes
# before writing, so another build is rejected even if its method layout differs.
CODE_OFFSETS = {
    CELL4: 0x248A4,
    CHECK: 0x252BC,
    KAIROS_BUSY: 0xDCA8,
    FIFOS: 0xD71C,
    DRIVE: 0x12604,
    TRIGGER: 0x12214,
}


def apply(path, sites, check_only):
    data = bytearray(open(path, "rb").read())
    changed = False
    for label, off, old, new in sites:
        cur = bytes(data[off:off + len(old)])
        if cur == new:
            print(f"  {label} @{off:#x}: already patched")
        elif cur == old:
            print(f"  {label} @{off:#x}: {'would patch' if check_only else 'patched'}")
            data[off:off + len(old)] = new
            changed = True
        else:
            sys.exit(f"  {label} @{off:#x}: unexpected bytes {cur.hex(' ')} - different build? nothing written")
    if changed and not check_only:
        if not os.path.exists(path + ".orig"):
            shutil.copy2(path, path + ".orig")
        open(path, "wb").write(data)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("build", nargs="?", default=os.environ.get("ICE_TEACH_DIR"))
    ap.add_argument("--check", action="store_true", help="only report")
    a = ap.parse_args()
    if not a.build:
        sys.exit("give the build dir or set ICE_TEACH_DIR")
    data_dir = os.path.join(a.build, "ICE Teach_Data")
    dll = os.path.join(data_dir, "Managed", "Assembly-CSharp.dll")

    print("level0")
    apply(os.path.join(data_dir, "level0"), [(l, o, h(x), h(y)) for l, o, x, y in LEVEL0_PATCHES], a.check)
    print("Assembly-CSharp.dll")
    sites = [(label, CODE_OFFSETS[meth] + il, h(old), h(new))
             for label, meth, il, old, new in DLL_PATCHES]
    apply(dll, sites, a.check)


if __name__ == "__main__":
    main()
