from typing import Any

from pydantic import BaseModel
from taskiq_aio_pika import AioPikaBroker
from taskiq_redis import RedisAsyncResultBackend
from taskiq.serializers import JSONSerializer, PickleSerializer
from taskiq.abc.serializer import TaskiqSerializer
from config import config


task_result_serializer = PickleSerializer()

broker = (
    AioPikaBroker(
        f"amqp://{config.rabbitmq.user}:{config.rabbitmq.password}"
        f"@{config.rabbitmq.host}:{config.rabbitmq.port}",
        # todo: indicate remaining results space to roducer
        max_stored_results=200_000,
        sync_tasks_pool_size=4,
    )
    .with_result_backend(RedisAsyncResultBackend(
        f"redis://{config.redis.host}:{config.redis.port}/1",
        serializer=task_result_serializer,
    )))
