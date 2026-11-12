"""RabbitMQ RPC answers for ICE Teach during a replay (replay.sh), when no KafkaBridge runs.

Unity reads some variables by RPC (contentPosition*, ABB/KUKA_programID, Last_Mission_Result, ...).
On the real line they are also published on Kafka with their opcua_path tag, so this keeps the last
value of every opcua_path seen on the local ice_data_* topics and answers reads with it. Unknown
variables get a null value at once instead of Unity's 10 s timeout. Writes are acknowledged and
stored. Credentials from .envrc in the environment.
"""
import asyncio, json, logging, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "utils"))
from kafka_contract import split_unescaped  # noqa: E402
import aio_pika  # noqa: E402
from aiokafka import AIOKafkaConsumer, TopicPartition  # noqa: E402
from aiokafka.admin import AIOKafkaAdminClient  # noqa: E402

QUEUES = ["conveyor_rpc_queue", "rbkairos_a_rpc_queue", "rbkairos_b_rpc_queue", "cell2_rpc_queue",
          "cell4_rpc_queue", "quality_control_rpc_queue"]  # the queues Unity's RabbitClient uses
log = logging.getLogger("replay_rpc")


def value(raw):
    if raw.endswith("i") and raw[:-1].lstrip("-").isdigit():
        return int(raw[:-1])
    if raw in ("t", "T", "true", "True", "TRUE", "f", "F", "false", "False", "FALSE"):
        return raw[0] in "tT"
    if raw.startswith('"'):
        return raw[1:-1].replace('\\"', '"')
    try:
        return float(raw)
    except ValueError:
        return raw


def path_value(line):
    """(opcua_path, value) of a one-field line-protocol message with an opcua_path tag, else None."""
    parts = [p for p in split_unescaped(line.strip(), " ") if p]
    if len(parts) < 2:
        return None
    tags = dict(t.split("=", 1) for t in split_unescaped(parts[0], ",")[1:] if "=" in t)
    fields = split_unescaped(parts[1], ",")
    if "opcua_path" not in tags or len(fields) != 1 or "=" not in fields[0]:
        return None
    return tags["opcua_path"].strip("/"), value(fields[0].split("=", 1)[1])


def reply(key, v, ok=True):
    # same body as ice-frost-ext/src/bridge/bridge_map.py rpc_reply (KafkaBridge)
    return {"key": key, "body": {"result": ok, "value": v, "returns": {}}, "value": v}


async def main():
    kw = dict(bootstrap_servers="localhost:9093", security_protocol="SASL_PLAINTEXT", sasl_mechanism="SCRAM-SHA-512",
              sasl_plain_username=os.environ["KAFKA_CLIENT_USER"], sasl_plain_password=os.environ["KAFKA_CLIENT_PASSWORD"])
    last = {}
    a = AIOKafkaAdminClient(**kw)  # partitions from the broker: a fresh consumer may have no metadata yet
    await a.start()
    try:
        topics = [t for t in await a.list_topics() if t.startswith("ice_data_")]
        tps = [TopicPartition(d["topic"], p["partition"]) for d in await a.describe_topics(topics) for p in d["partitions"]]
    finally:
        await a.close()
    c = AIOKafkaConsumer(**kw)  # no group: all ice_data_* partitions from the end
    await c.start()
    c.assign(tps)
    await c.seek_to_end(*tps)

    conn = await aio_pika.connect(host="127.0.0.1", port=30690, virtualhost="/rpc",
                                  login=os.environ["RABBITMQ_USERNAME"], password=os.environ["RABBITMQ_PASSWORD"])
    ch = await conn.channel()

    async def serve(name):
        q = await ch.declare_queue(name, durable=False, auto_delete=True)
        async with q.iterator() as msgs:
            async for m in msgs:
                async with m.process():
                    req = json.loads(m.body)
                    path = str((req.get("args") or [""])[0]).strip("/")
                    if req.get("method") == "write":
                        last[path] = req["args"][1] if len(req["args"]) > 1 else None
                        body = reply(req.get("key"), last[path])
                    else:
                        body = reply(req.get("key"), last.get(path), path in last)
                    log.info("%s %s %s -> %r", name, req.get("method"), path, body["value"])
                    await ch.default_exchange.publish(
                        aio_pika.Message(json.dumps(body).encode(), correlation_id=m.correlation_id), routing_key=m.reply_to)

    for name in QUEUES:
        asyncio.ensure_future(serve(name))
    log.info("answering %s from %d partitions", ", ".join(QUEUES), len(tps))
    async for r in c:
        pv = path_value(r.value.decode("utf-8", "replace"))
        if pv:
            last[pv[0]] = pv[1]


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        assert path_value('Cell4,category=mission,opcua_path=Objects/Cell_4/Outputs/ABB_programID ABB_programID=3i 17937') \
            == ("Objects/Cell_4/Outputs/ABB_programID", 3)
        assert path_value('Cell5,opcua_path=Objects/Cell5/Status/LastResult LastResult=true 1') == ("Objects/Cell5/Status/LastResult", True)
        assert path_value('Conveyor,segment=1 SegmentSize=1i 1') is None
        assert path_value('Kraken,opcua_path=/a/b a=1i,b=2i 1') is None
        assert value('-0.5') == -0.5 and value('"x y"') == "x y"
        print("ok")
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
        logging.getLogger("aiokafka").setLevel(logging.WARNING)
        asyncio.run(main())
