"""Kafka producer and RabbitMQ RPC queues of KafkaBridge.lf, on an asyncio loop in a thread.

The LF reactions never block on the network: publish() and reply() queue, RPC requests arrive in
`requests` (a thread-safe queue the reactor polls). Without a broker (offline runs) the bus logs
once and does nothing. Settings from the environment, as on the virtualice-image stack (.envrc).
"""

import asyncio
import collections
import json
import logging
import os
import queue
import threading

log = logging.getLogger("kafka_bridge")


def _env(name, default):
    return os.environ.get(name, default)


async def _reachable(host: str, port: int) -> bool:
    try:
        _, w = await asyncio.wait_for(asyncio.open_connection(host, port), 2)
        w.close()
        return True
    except (OSError, asyncio.TimeoutError):
        return False


class Bus:
    def __init__(self, rpc_queues=()):
        self.rpc_queues = list(rpc_queues)
        self.requests = queue.SimpleQueue()  # (queue name, reply_to, correlation_id, request dict)
        self.kafka = self.rabbit = None
        self._out = collections.deque()      # (topic, value)
        self._wake = None
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="kafka_bridge", daemon=True)

    # --- reactor side -----------------------------------------------------------------------------

    def start(self):
        self._thread.start()
        asyncio.run_coroutine_threadsafe(self._start(), self._loop).result()
        return self.kafka is not None, self.rabbit is not None

    def publish(self, topic: str, value: str):
        if self.kafka is not None:
            self._out.append((topic, value))
            self._loop.call_soon_threadsafe(self._wake.set)

    def reply(self, reply_to: str, correlation_id: str, body: dict):
        if self.rabbit is not None:
            asyncio.run_coroutine_threadsafe(self._reply(reply_to, correlation_id, body), self._loop)

    def stop(self):
        if self._thread.is_alive():
            asyncio.run_coroutine_threadsafe(self._stop(), self._loop).result(10)
            self._loop.call_soon_threadsafe(self._loop.stop)

    # --- loop side --------------------------------------------------------------------------------

    async def _start(self):
        from aiokafka import AIOKafkaProducer
        servers = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
        host, _, port = servers.split(",")[0].rpartition(":")
        if await _reachable(host, int(port)):
            producer = AIOKafkaProducer(
                bootstrap_servers=servers,
                security_protocol=_env("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
                sasl_mechanism=_env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
                sasl_plain_username=_env("KAFKA_CLIENT_USER", "username"),
                sasl_plain_password=_env("KAFKA_CLIENT_PASSWORD", "password"))
            try:
                await producer.start()
                self.kafka = producer
                self._wake = asyncio.Event()
                asyncio.ensure_future(self._sender())
            except Exception as e:
                await producer.stop()
                log.warning("Kafka at %s refused the producer, not publishing: %r", servers, e)
        else:
            log.warning("Kafka not reachable at %s, not publishing", servers)

        host, port = _env("RABBITMQ_HOST", "localhost"), int(_env("RABBITMQ_PORT", 30690))
        if not self.rpc_queues:
            return
        if not await _reachable(host, port):
            log.warning("RabbitMQ not reachable at %s:%s, no RPC", host, port)
            return
        import aio_pika
        try:
            self.rabbit = await aio_pika.connect(
                host=host, port=port, virtualhost=_env("RABBITMQ_VIRTUAL_HOST", "/rpc"),
                login=_env("RABBITMQ_USERNAME", "guest"), password=_env("RABBITMQ_PASSWORD", "guest"), heartbeat=600)
        except Exception as e:
            log.warning("RabbitMQ at %s:%s refused the connection, no RPC: %r", host, port, e)
            return
        self._channel = await self.rabbit.channel()
        for name in self.rpc_queues:
            asyncio.ensure_future(self._listen(name))

    async def _sender(self):
        # one coroutine sends in order (per topic order matters: FIFO slots, twin commands)
        while True:
            await self._wake.wait()
            self._wake.clear()
            while self._out:
                topic, value = self._out.popleft()
                try:
                    await self.kafka.send(topic, value.encode("utf-8"))
                except Exception as e:  # keep the bridge alive, the next message may go through
                    log.error("Kafka send to %s failed: %r", topic, e)

    async def _listen(self, name: str):
        q = await self._channel.declare_queue(name, durable=False, auto_delete=True)
        async with q.iterator() as messages:
            async for m in messages:
                try:
                    self.requests.put((name, m.reply_to, m.correlation_id, json.loads(m.body.decode("utf-8"))))
                except ValueError:
                    log.error("RPC request on %s is not JSON: %r", name, m.body[:200])
                await m.ack()

    async def _reply(self, reply_to, correlation_id, body):
        import aio_pika
        await self._channel.default_exchange.publish(
            aio_pika.Message(json.dumps(body).encode("utf-8"), content_type="text/plain", correlation_id=correlation_id),
            routing_key=reply_to)

    async def _stop(self):
        if self.kafka is not None:
            while self._out:  # flush what the run produced last
                await asyncio.sleep(0.05)
            await self.kafka.stop()
            self.kafka = None
        if self.rabbit is not None:
            await self.rabbit.close()
            self.rabbit = None
