"""Creates the FrostTwin topics on the local stack (demo.sh), with the credentials of .envrc in the
environment: ice_data_conveyor_twin (FrostTwin reads its partition 0 by assignment, so it must exist
before Unity starts) and ice_data_qc_image (QC camera pictures, read by qc_vision.py).
Unity's own topics need nothing: every run is a new consumer group from the end of the topics, which
creates the missing ones (FrostTwin/Endpoints.cs).
"""
import asyncio, os
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

TOPICS = ["ice_data_conveyor_twin", "ice_data_qc_image"]


async def main():
    a = AIOKafkaAdminClient(bootstrap_servers="localhost:9093",
                            security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
                            sasl_mechanism=os.environ.get("KAFKA_MECHANISM", "SCRAM-SHA-512"),
                            sasl_plain_username=os.environ["KAFKA_CLIENT_USER"],
                            sasl_plain_password=os.environ["KAFKA_CLIENT_PASSWORD"])
    await a.start()
    try:
        missing = [t for t in TOPICS if t not in await a.list_topics()]
        if missing:
            await a.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in missing])
            print("created", ", ".join(missing))
    finally:
        await a.close()

asyncio.run(main())
