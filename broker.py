from taskiq_aio_pika import AioPikaBroker
from taskiq_redis import RedisAsyncResultBackend

broker = (
    AioPikaBroker(
        "amqp://guest:guest@rabbitmq:5672",
        max_stored_results=20000,
        sync_tasks_pool_size=5,
    )
    .with_result_backend(RedisAsyncResultBackend(
        "redis://redis/1"
    )))
