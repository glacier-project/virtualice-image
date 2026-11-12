import os 
import asyncio
import base64
import json
from types import SimpleNamespace
from aiokafka import AIOKafkaProducer, ConsumerRecord
import argparse
import pickle
from datetime import datetime
import time
from typing import AsyncGenerator, Generator
import logging

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger(__name__)

def load_pickle(path: str) -> list[ConsumerRecord]:
    with open(path, 'rb') as f:
        return pickle.load(f)


def load_jsonl(path: str) -> list[SimpleNamespace]:
    """File .jsonl di cattura_traffico.py --registra: una riga per messaggio, key/value/header in base64."""
    def b(v):
        return None if v is None else base64.b64decode(v)
    out = []
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            try:
                d = json.loads(line)
            except json.JSONDecodeError:   # ultima riga troncata se la cattura è stata uccisa
                logger.warning(f"{path}:{n}: riga non valida, saltata")
                continue
            d.update(key=b(d["key"]), value=b(d["value"]), headers=[(k, b(v)) for k, v in d["headers"]])
            out.append(SimpleNamespace(**d))
    return out


class ICEPlayer():

    def __init__(self, paths: list[str], topics: list[str]):
        msgs_list: list[ConsumerRecord] = []
        for path in paths:
            msgs_list += load_jsonl(path) if path.endswith(".jsonl") else load_pickle(path)

        self._topics = topics
        # ponytail: tutto in RAM come prima; per registrazioni di ore serve un merge in streaming dei file
        self._msgs_list = sorted((msg for msg in msgs_list if msg.topic in topics), key=lambda m: m.timestamp)
        if not self._msgs_list:
            raise SystemExit(f"nessun messaggio dei topic {topics} in {paths}")

    def get_messages(self, simulate_delays: bool=True) -> Generator[ConsumerRecord, None, None]:
        msgs_list = self._msgs_list
        delay = 0

        yield msgs_list[0]

        for i in range(1,len(msgs_list)):
            if simulate_delays:
                delay = datetime.fromtimestamp(msgs_list[i].timestamp/1000)-datetime.fromtimestamp(msgs_list[i-1].timestamp/1000)
                delay = delay.total_seconds()
                if delay > 0:
                    time.sleep(delay)
            
            yield msgs_list[i]


    async def get_messages_async(self, simulate_delays: bool=True) -> AsyncGenerator[ConsumerRecord, None]:
        msgs_list = self._msgs_list
        delay = 0

        yield msgs_list[0]

        for i in range(1,len(msgs_list)):
            if simulate_delays:
                delay = datetime.fromtimestamp(msgs_list[i].timestamp/1000)-datetime.fromtimestamp(msgs_list[i-1].timestamp/1000)
                delay = delay.total_seconds()
                if delay > 0:
                    await asyncio.sleep(delay)

            yield msgs_list[i]

    async def produce_messages(self, producer: AIOKafkaProducer, simulate_delays: bool=True):
        async for msg in self.get_messages_async(simulate_delays):
            result = await producer.send(
                topic=msg.topic,
                value=msg.value,
                key=msg.key,
                headers=list(getattr(msg, "headers", None) or []) or None,
            )
            result = await result

            logger.info(f"Produced message to {msg.topic} with value {msg.value} at offset {result.offset}")


async def start_producer(path: list[str], topics: list[str], simulate_delays: bool, bootstrap_servers: str, infinite: bool):
    username = os.getenv("KAFKA_CLIENT_USER", "user")
    password = os.getenv("KAFKA_CLIENT_PASSWORD", "password")
    logger.info(f"Using Kafka broker: {bootstrap_servers}")
    logger.info(f"Using Kafka username: {username}")
    logger.info(f"Selected topics: {topics}")
    if infinite:
        logger.warning("Infinite mode is enabled. Messages will be produced indefinitely.")

    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers,     
        security_protocol="SASL_PLAINTEXT", 
        sasl_mechanism="SCRAM-SHA-512", 
        sasl_plain_username=username, 
        sasl_plain_password=password,
        metadata_max_age_ms=30000,  # 30 seconds
    )
    try:
        await producer.start()
        ice_player = ICEPlayer(path, topics)
        
        while True:
            await ice_player.produce_messages(producer, simulate_delays)
            if not infinite:
                break
            # Sleep for a while before producing the next batch of messages
            await asyncio.sleep(1)
    
    finally:
        await producer.stop()


def main():
    parser = argparse.ArgumentParser(description="ICE Player")
    parser.add_argument("--path", type=str, required=True, nargs='+',
                        help="Recording(s): .pkl from utils/consume_kafka.py or .jsonl from cattura_traffico.py --registra")
    parser.add_argument("--topics", type=str, nargs='+', required=True, help="List of topics to filter messages")
    parser.add_argument("--simulate-delays", action='store_true', help="Simulate delays between messages")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9093", help="Kafka broker address")
    parser.add_argument("--infinite", action='store_true', help="Run the producer in infinite mode")
    args = parser.parse_args()

    asyncio.run(start_producer(args.path, args.topics, args.simulate_delays, args.bootstrap_servers, args.infinite))

if __name__ == "__main__":
    main()