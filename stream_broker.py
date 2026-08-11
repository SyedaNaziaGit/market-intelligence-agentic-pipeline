import asyncio
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EmbeddedStreamBroker")

class EmbeddedQueueBroker:
    """
    Replicates the Kafka production streaming API using native high-speed
    asynchronous memory queues. Bypasses Docker friction completely on Windows.
    """
    _queue = asyncio.Queue()

    @classmethod
    async def produce(cls, topic: str, value: bytes):
        """Replicates KafkaProducer.produce()"""
        await cls._queue.put((topic, value))
        logger.info(f" [Producer] Emitted event packet to topic string: '{topic}'")

    @classmethod
    async def poll(cls, timeout: float = 1.0):
        """Replicates KafkaConsumer.poll()"""
        try:
            # Non-blocking get with timeout parameters
            return await asyncio.wait_for(cls._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    @classmethod
    def task_done(cls):
        cls._queue.task_done()
