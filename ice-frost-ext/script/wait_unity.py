"""Waits until ICE Teach (Unity: Editor or build) has its Kafka consumer on the stack, then exits.
Unity reads from the end of the topics (a new consumer group per run, FrostTwin/Endpoints.cs), so the
Frost line must start publishing only after that: the frost-ext container runs this before GadgetMain.
"""
import asyncio, os
from aiokafka.admin import AIOKafkaAdminClient

PREFIX = "unity-digitalshadow-"


async def main():
    a = AIOKafkaAdminClient(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
                            security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
                            sasl_mechanism=os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
                            sasl_plain_username=os.environ["KAFKA_CLIENT_USER"],
                            sasl_plain_password=os.environ["KAFKA_CLIENT_PASSWORD"])
    await a.start()
    try:
        print("waiting for ICE Teach (Unity) on Kafka: open it / press Play", flush=True)
        while True:
            groups = [g[0] for g in await a.list_consumer_groups() if g[0].startswith(PREFIX)]
            # describe: (error_code, group_id, state, ...); Stable = partitions assigned
            stable = [g[1] for r in (await a.describe_consumer_groups(groups) if groups else [])
                      for g in r.groups if g[2] == "Stable"]
            if stable:
                print("ICE Teach connected (" + ", ".join(stable) + "), starting the Frost line", flush=True)
                await asyncio.sleep(5)  # FrostTwin assigns its twin partition right after the group
                return
            await asyncio.sleep(2)
    finally:
        await a.close()

asyncio.run(main())
