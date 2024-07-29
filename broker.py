from taskiq_aio_pika import AioPikaBroker
from taskiq_redis import RedisAsyncResultBackend

from config import config

broker = (
    AioPikaBroker(
        f"amqp://{config.rabbitmq.user}:{config.rabbitmq.password}"
        f"@{config.rabbitmq.host}:{config.rabbitmq.port}",
        max_stored_results=20000,
        sync_tasks_pool_size=5,
    )
    .with_result_backend(RedisAsyncResultBackend(
        f"redis://{config.redis.host}:{config.redis.port}/1"
    )))
