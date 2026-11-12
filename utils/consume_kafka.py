import os
import asyncio
import argparse
import pickle
from aiokafka import AIOKafkaConsumer, ConsumerRecord

KAFKA_CLIENT_USER = os.getenv("KAFKA_CLIENT_USER", "user")
KAFKA_CLIENT_PASSWORD = os.getenv("KAFKA_CLIENT_PASSWORD", "password")


async def consume_messages(bootstrap_servers: str, topics: list[str] | None, out: str | None, idle_timeout: float):
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=None,  # no committed offsets: every run re-reads everything
        auto_offset_reset="earliest",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=KAFKA_CLIENT_USER,
        sasl_plain_password=KAFKA_CLIENT_PASSWORD,
    )
    if topics:
        consumer.subscribe(topics=topics)
    else:
        consumer.subscribe(pattern=r"^(?!__).*")  # all non-internal topics

    records: list[ConsumerRecord] = []
    await consumer.start()
    try:
        while True:
            # ponytail: "done" = no new message for idle_timeout s; use end_offsets() if exact cut-off matters
            batch = await consumer.getmany(timeout_ms=int(idle_timeout * 1000) if idle_timeout else 1000)
            if not batch:
                if idle_timeout:
                    break
                continue
            for msgs in batch.values():
                for msg in msgs:
                    records.append(msg)
                    value = msg.value.decode("utf-8", errors="replace") if msg.value else None
                    print(f"[{msg.topic}:{msg.partition}@{msg.offset}] {value}")
    finally:
        await consumer.stop()
        records.sort(key=lambda m: m.timestamp)  # player.py replays in list order
        print(f"Consumed {len(records)} messages from {len({m.topic for m in records})} topics")
        if out:
            with open(out, "wb") as f:
                pickle.dump(records, f)
            print(f"Saved to {out} (replayable with ice_player/player.py)")


def main():
    parser = argparse.ArgumentParser(description="Dump Kafka topics from the beginning")
    parser.add_argument("--bootstrap-servers", default="localhost:9093")
    parser.add_argument("--topics", nargs="+", help="Topics to read (default: all non-internal topics)")
    parser.add_argument("--out", help="Save records to this pickle file")
    parser.add_argument("--idle-timeout", type=float, default=10,
                        help="Exit after N seconds without new messages; 0 = follow forever (Ctrl+C to stop)")
    args = parser.parse_args()
    try:
        asyncio.run(consume_messages(args.bootstrap_servers, args.topics, args.out, args.idle_timeout))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
