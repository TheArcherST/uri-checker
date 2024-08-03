import taskiq
from redis.asyncio import Redis
from taskiq_aio_pika import AioPikaBroker
from taskiq_redis import RedisAsyncResultBackend
from taskiq.serializers import PickleSerializer
from config import config


CONSUME_QUEUE_KEY = "consume_queue"


redis = Redis(
    host=config.redis.host,
    port=config.redis.port,
    db=1,
)


class URICheckReportsRegistryMiddleware(taskiq.TaskiqMiddleware):
    async def post_save(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> "Union[None, Coroutine[Any, Any, None]]":
        if result is None:
            return
        print(message, result)
        await redis.lpush(CONSUME_QUEUE_KEY, message.task_id)


task_result_serializer = PickleSerializer()

broker = (
    AioPikaBroker(
        f"amqp://{config.rabbitmq.user}:{config.rabbitmq.password}"
        f"@{config.rabbitmq.host}:{config.rabbitmq.port}",
        # todo: indicate remaining results space to roducer
        max_stored_results=200_000,
        sync_tasks_pool_size=2,
    )
    .with_result_backend(RedisAsyncResultBackend(
        f"redis://{config.redis.host}:{config.redis.port}/1",
        serializer=task_result_serializer,
    ))
    .with_middlewares(URICheckReportsRegistryMiddleware(
    ))
)
