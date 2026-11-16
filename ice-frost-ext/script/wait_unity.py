"""Waits until ICE Teach (Unity: Editor or build) has its Kafka consumer on the stack, then exits.
Unity reads from the end of the topics (a new consumer group per run, FrostTwin/Endpoints.cs), so the
Frost line must start publishing only after that: the frost-ext container runs this before GadgetMain.

Never gives up (the container would exit and the line never start):
- Kafka not reachable yet (container started before the brokers): retried;
- groups of earlier ICE Teach runs stay on the stack, Empty (the stack is stopped right after ICE Teach,
  before Kafka cleans them up), and aiokafka fails to decode some of them ("Buffer underrun decoding
  string", connection closed): each group is described alone, the failing ones skipped.
"""
import asyncio, logging, os
from aiokafka.admin import AIOKafkaAdminClient

PREFIX = "unity-digitalshadow-"
logging.getLogger("aiokafka").setLevel(logging.CRITICAL)   # tracebacks of the skipped groups above


def admin():
    return AIOKafkaAdminClient(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
                               security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
                               sasl_mechanism=os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
                               sasl_plain_username=os.environ["KAFKA_CLIENT_USER"],
                               sasl_plain_password=os.environ["KAFKA_CLIENT_PASSWORD"])


async def stable_groups(a):
    stable = []
    for group in [g[0] for g in await a.list_consumer_groups() if g[0].startswith(PREFIX)]:
        try:   # describe: (error_code, group_id, state, ...); Stable = partitions assigned
            stable += [g[1] for r in await a.describe_consumer_groups([group]) for g in r.groups if g[2] == "Stable"]
        except Exception:
            pass   # a stale group aiokafka cannot decode: not ICE Teach's
    return stable


async def main():
    print("waiting for ICE Teach (Unity) on Kafka: open it / press Play", flush=True)
    while True:
        a = admin()
        try:
            await a.start()
            while not (stable := await stable_groups(a)):
                await asyncio.sleep(2)
            print("ICE Teach connected (" + ", ".join(stable) + "), starting the Frost line", flush=True)
            await asyncio.sleep(5)  # FrostTwin assigns its twin partition right after the group
            return
        except Exception as e:   # Kafka not up yet, connection dropped: again with a new client
            print(f"Kafka not ready ({type(e).__name__}), retrying", flush=True)
            await asyncio.sleep(3)
        finally:
            try:
                await a.close()
            except Exception:
                pass

asyncio.run(main())
