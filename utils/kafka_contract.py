#!/usr/bin/env python3
"""Kafka contract: what virtualice-image publishes must be a true subset of what the real ICE line publishes.

    python3 utils/kafka_contract.py catalog <raw.jsonl>... [-o kafka/contract.json]
    python3 utils/kafka_contract.py check <out.jsonl> [-c kafka/contract.json]
    python3 utils/kafka_contract.py self-test

<raw.jsonl>  real line captures (ICE_DIR/kafka-record/record/raw/*.jsonl: topic, value in base64).
<out.jsonl>  messages to check: the same capture format, or {"topic", "value"} with the value as text.

A line protocol message has a shape: topic, measurement, tag keys, field names and types, and the
opcua_path with digits replaced by N. `check` reports every shape the line never produced, and tag
values (datatype, category, element, ...) the line never used for that shape. Missing shapes are fine:
the output is a subset. Opaque topics (not line protocol: JSON, binary) are checked by name.

contract_line() builds, from the contract, the line the real ICE line publishes for an OPC UA variable.
"""
import argparse
import base64
import collections
import json
import re
import sys

PER_PATH_TAGS = {"opcua_path"}  # tags whose values are not compared (one per variable)


def real_path(node: str) -> str:
    """Node path -> OPC UA path as the line writes it ("/Warehouse/Status/x" -> "Objects/Warehouse/Status/x")."""
    node = node.lstrip("/")
    return node if node.startswith("Objects/") else "Objects/" + node


def render(template: dict, path: str) -> dict:
    """Fill a pattern template for one path: {j} is the j-th number in the path, {j+1} that number plus one
    (e.g. .../Segment6/FIFO/item0 -> segment=6, position=0; .../ABBPositions/0 -> field BufferABB[1])."""
    groups = [int(g) for g in re.findall(r"\d+", path)]

    def fill(s):
        return re.sub(r"\{(\d+)(\+1)?\}", lambda m: str(groups[int(m[1])] + (1 if m[2] else 0)), s)
    return {**template, "field": fill(template["field"]), "tags": {k: fill(v) for k, v in template["tags"].items()}}


def contract_line(contract: dict, prefix: str, path: str, value, ts: int):
    """(topic, line) the real ICE line publishes for this variable, None if it never publishes it.

    `contract` is kafka/contract.json (catalog): "paths" has, per
    "<topic prefix>|<opcua_path>" seen on the line, its topic, measurement, tags, field name and type;
    "patterns" the same per path with numbers replaced by N, for the slots the recordings never touched.
    """
    t = contract["paths"].get(f"{prefix}|{path}")
    if t is None:
        p = contract["patterns"].get(prefix + "|" + re.sub(r"\d+", "N", path))
        if p is None:
            return None
        t = render(p, path)
    if t["type"] == "int":
        v = f"{int(value)}i"
    elif t["type"] == "bool":
        v = "true" if value else "false"
    elif t["type"] == "float":
        v = repr(float(value))
    else:
        v = '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'
    tags = "".join(f",{k}={x}" for k, x in sorted({**t["tags"], "opcua_path": path}.items()))
    return t["topic"], f"{t['measurement']}{tags} {t['field']}={v} {ts}"


def split_unescaped(s, sep):
    """Split on `sep` outside double quotes and not escaped with a backslash."""
    out, cur, quoted, i = [], [], False, 0
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            cur += s[i:i + 2]
            i += 2
            continue
        if c == '"':
            quoted = not quoted
        if c == sep and not quoted:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(c)
        i += 1
    out.append("".join(cur))
    return out


def field_type(v):
    if v.startswith('"'):
        return "string"
    if re.fullmatch(r"-?\d+i", v):
        return "int"
    if v in ("t", "T", "true", "True", "TRUE", "f", "F", "false", "False", "FALSE"):
        return "bool:" + v  # consumers may parse the raw text: the spelling is part of the format
    try:
        float(v)
        return "float"
    except ValueError:
        return "?" + v


def parse(line):
    """Influx line -> (measurement, {tag: value}, {field: type}) or None if it is not line protocol."""
    parts = [p for p in split_unescaped(line.strip(), " ") if p]
    if len(parts) not in (2, 3) or (len(parts) == 3 and not parts[2].isdigit()):
        return None
    head = split_unescaped(parts[0], ",")
    tags = dict(t.split("=", 1) for t in head[1:] if "=" in t)
    fields = {}
    for f in split_unescaped(parts[1], ","):
        if "=" not in f:
            return None
        k, v = f.split("=", 1)
        fields[k] = field_type(v)
    return head[0], tags, fields


def shape_key(topic, measurement, tags, fields):
    path = re.sub(r"\d+", "N", tags.get("opcua_path", ""))
    return f"{topic} | {measurement} | {path} | tags={','.join(sorted(tags))} | fields={','.join(f'{k}:{t}' for k, t in sorted(fields.items()))}"


def catalog(files, out):
    shapes = collections.defaultdict(lambda: collections.defaultdict(set))
    paths = collections.defaultdict(collections.Counter)  # "<topic prefix>|<opcua_path>" -> templates seen
    opaque = collections.Counter()
    for name in files:
        with open(name) as f:
            for raw in f:
                o = json.loads(raw)
                topic = o["topic"]
                try:
                    value = base64.b64decode(o["value"] or "").decode()
                except UnicodeDecodeError:
                    opaque[topic] += 1
                    continue
                for line in value.splitlines():
                    p = parse(line)
                    if p is None:
                        opaque[topic] += 1
                        continue
                    m, tags, fields = p
                    s = shapes[shape_key(topic, m, tags, fields)]
                    for k, v in tags.items():
                        if k not in PER_PATH_TAGS:
                            s[k].add(v)
                    s["#example"] = {line[:300]}
                    if "opcua_path" in tags and len(fields) == 1:
                        (field, ftype), = fields.items()
                        t = {"topic": topic, "measurement": m, "field": field, "type": ftype.split(":")[0],
                             "tags": {k: v for k, v in tags.items() if k != "opcua_path"}}
                        paths[f"{topic.rsplit('_', 1)[0]}|{tags['opcua_path']}"][json.dumps(t, sort_keys=True)] += 1
    paths = {k: json.loads(c.most_common(1)[0][0]) for k, c in sorted(paths.items())}
    patterns, rejected = generalize_paths(paths)
    data = {"shapes": {k: {t: sorted(v) for t, v in s.items()} for k, s in sorted(shapes.items())},
            "opaque_topics": sorted(opaque),
            # per variable, the line the real ICE line writes for it (contract_line)
            "paths": paths, "patterns": patterns}
    with open(out, "w") as f:
        json.dump(data, f, indent=1, sort_keys=True)
    print(f"{len(data['shapes'])} shapes, {len(paths)} variables, {len(patterns)} patterns "
          f"({len(rejected)} not generalizable: {rejected}), {len(data['opaque_topics'])} opaque topics -> {out}")


def generalize_paths(paths):
    """Per path pattern (numbers -> N), a template whose numbers come from the path ({j}, {j+1}, see
    render). Kept only if it reproduces every variable of that pattern seen on the line."""
    groups = collections.defaultdict(list)
    for key, t in paths.items():
        prefix, path = key.split("|", 1)
        groups[f"{prefix}|{re.sub(r'[0-9]+', 'N', path)}"].append((path, t))
    patterns, rejected = {}, []
    for pkey, items in sorted(groups.items()):
        candidates = collections.Counter()
        for path, t in items:
            nums = [int(g) for g in re.findall(r"[0-9]+", path)]
            ambiguous = False

            def sub(s):
                nonlocal ambiguous

                def one(m):
                    nonlocal ambiguous
                    n = int(m[0])
                    c = [f"{{{j}}}" for j, g in enumerate(nums) if n == g] + [f"{{{j}+1}}" for j, g in enumerate(nums) if n == g + 1]
                    ambiguous |= len(c) > 1
                    return c[0] if c else m[0]
                return re.sub(r"[0-9]+", one, s)
            g = {**t, "field": sub(t["field"]), "tags": {k: sub(v) for k, v in t["tags"].items()}}
            if not ambiguous:
                candidates[json.dumps(g, sort_keys=True)] += 1
        best = next((json.loads(c) for c, _ in candidates.most_common() if all(render(json.loads(c), p) == t for p, t in items)), None)
        if best is None:
            rejected.append(pkey)
        else:
            patterns[pkey] = best
    return patterns, rejected


def by_pattern(contract, topic, parsed):
    """The line is what the contract pattern of its opcua_path gives (variables never seen on the line,
    e.g. FIFO slot 9: tag values are taken from the path as for every recorded variable of the pattern)."""
    measurement, tags, fields = parsed
    path = tags.get("opcua_path", "")
    pat = contract["patterns"].get(topic.rsplit("_", 1)[0] + "|" + re.sub(r"[0-9]+", "N", path))
    if pat is None or len(fields) != 1:
        return False
    t = render(pat, path)
    (field, ftype), = fields.items()
    return (t["topic"], t["measurement"], t["field"], t["type"], t["tags"]) == (
        topic, measurement, field, ftype.split(":")[0], {k: v for k, v in tags.items() if k != "opcua_path"})


def check(recording, contract):
    c = json.load(open(contract))
    shapes, opaque = c["shapes"], set(c["opaque_topics"])
    topics = {k.split(" | ")[0] for k in shapes} | opaque
    by_base = collections.defaultdict(dict)  # shape without fields -> {shape key: set of "field:type"}
    for k in shapes:
        base, fields = k.rsplit(" | fields=", 1)
        by_base[base][k] = set(fields.split(","))
    errors = collections.Counter()
    examples = {}
    n = extrapolated = 0
    with open(recording) as f:
        for raw in f:
            o = json.loads(raw)
            topic, value = o["topic"], o["value"]
            if "partition" in o:  # capture format: value in base64
                value = base64.b64decode(value or "").decode(errors="replace")
            n += 1
            if topic not in topics:
                err = f"topic not on the line: {topic}"
            elif topic in opaque and parse(value) is None:
                continue
            else:
                p = parse(value)
                if p is None:
                    err = f"{topic}: not line protocol"
                else:
                    # a subset of the fields of a real shape is fine (e.g. no effort on a modelled arm)
                    key = shape_key(topic, *p)
                    base, fields = key.rsplit(" | fields=", 1)
                    fields = set(fields.split(","))
                    match = [k for k in by_base.get(base, ()) if fields <= by_base[base][k]]
                    if not match:
                        err = f"shape not on the line: {key}"
                    else:
                        bad = [f"{t}={v}" for t, v in p[1].items()
                               if t not in PER_PATH_TAGS and not any(v in shapes[k][t] for k in match)]
                        if not bad:
                            continue
                        if by_pattern(c, topic, p):
                            extrapolated += 1
                            continue
                        err = f"tag values not on the line {bad}: {key}"
            errors[err] += 1
            examples.setdefault(err, value[:200])
    for err, k in errors.most_common():
        print(f"{k:6}  {err}\n        e.g. {examples[err]}")
    print(f"{n} messages, {sum(errors.values())} outside the contract, {len(errors)} distinct problems; "
          f"{extrapolated} on slots the recordings never touched, by a pattern that reproduces all the recorded ones")
    return 1 if errors else 0


def self_test():
    m, tags, fields = parse(r'Energy\ Meter,machine_name=Ferretto\ Vertimag,phase=1 voltage=226.3 1793720908370000000')
    assert m == r"Energy\ Meter" and tags["machine_name"] == r"Ferretto\ Vertimag" and fields == {"voltage": "float"}
    assert parse('RBKairos,robot=a Ongoing_Mission_Name="go to, cell 4" 1')[2] == {"Ongoing_Mission_Name": "string"}
    assert parse("Conveyor,segment=6 Pallet=5i 1")[2] == {"Pallet": "int"}
    assert parse("Conveyor Opened=false 1")[2] == {"Opened": "bool:false"}
    assert parse('{"p": 7, "pts": [[1, 2]]}') is None
    fifo = {"topic": "ice_data_conveyor_fifos", "measurement": "Conveyor", "field": "Pallet", "type": "int",
            "tags": {"category": "state", "datatype": "Integer", "position": "{1}", "segment": "{0}"}}
    paths = {"paths": {"ice_data_conveyor|Objects/C/Segment6/FIFO/item0": render(fifo, "Objects/C/Segment6/FIFO/item0")},
             "patterns": {"ice_data_conveyor|Objects/C/SegmentN/FIFO/itemN": fifo}}
    assert real_path("/Objects/C/Segment6/FIFO/item0") == "Objects/C/Segment6/FIFO/item0"
    assert real_path("RBKairos/Pose/x") == "Objects/RBKairos/Pose/x"
    assert contract_line(paths, "ice_data_conveyor", "Objects/C/Segment6/FIFO/item0", 5, 7) == (
        "ice_data_conveyor_fifos",
        "Conveyor,category=state,datatype=Integer,opcua_path=Objects/C/Segment6/FIFO/item0,position=0,segment=6 Pallet=5i 7")
    assert contract_line(paths, "ice_data_conveyor", "Objects/C/x", 5, 7) is None
    assert contract_line(paths, "ice_data_conveyor", "Objects/C/Segment1/FIFO/item7", 0, 7)[1] == (
        "Conveyor,category=state,datatype=Integer,opcua_path=Objects/C/Segment1/FIFO/item7,position=7,segment=1 Pallet=0i 7")
    assert render({"field": "BufferABB[{1+1}]", "tags": {}}, "Objects/Cell_4_X/ABBPositions/3")["field"] == "BufferABB[4]"
    assert contract_line(paths | {"paths": {"x|Objects/s": {"topic": "t", "measurement": "M", "field": "f", "type": "string", "tags": {}}}},
                         "x", "Objects/s", 'a "b" \\', 1)[1] == 'M,opcua_path=Objects/s f="a \\"b\\" \\\\" 1'
    print("ok")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("catalog")
    a.add_argument("files", nargs="+")
    a.add_argument("-o", default="kafka/contract.json")
    b = sub.add_parser("check")
    b.add_argument("recording")
    b.add_argument("-c", default="kafka/contract.json")
    sub.add_parser("self-test")
    args = ap.parse_args()
    if args.cmd == "catalog":
        catalog(args.files, args.o)
    elif args.cmd == "check":
        sys.exit(check(args.recording, args.c))
    else:
        self_test()
