from typing import Iterable

import taskiq
from redis.asyncio import Redis
from taskiq import TaskiqResult
from taskiq_aio_pika import AioPikaBroker
from taskiq_redis import RedisAsyncResultBackend
from taskiq.serializers import PickleSerializer
from config import config


class RedisKeys:
    CONSUME_QUEUE = "consume_queue"
    """
    List of pointers to tasks
    """

    CONSUME_QUEUE_TRAILING_INDEX = "consume_queue_trailing_index"
    """
    Uses for pointing on index in internal represeentation of last
    member of consume queue, is member was not consumed complitely.
    """

    CONSUME_QUEUE_ITEMS_COUNT = "consume_queue_items_count"
    """
    Not just count of task results to be consumed but total count of 
    reports that wainitng for consume.  This value increasing by
    workers.
    """

    IS_FLUSH_PLUGIN_ENABLED = 'is_flush_plugin_enabled'


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
        from protocol import URICheckReport

        if result.return_value is None:
            return
        if not isinstance(result.return_value, list):
            return
        if len(result.return_value) == 0:
            return
        if not isinstance(result.return_value[0], URICheckReport):
            return

        await redis.lpush(RedisKeys.CONSUME_QUEUE, message.task_id)
        await redis.incrby(RedisKeys.CONSUME_QUEUE_ITEMS_COUNT, len(result.return_value))


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
        timeout=50,
        result_ex_time=60*60*1,
    ))
    .with_middlewares(URICheckReportsRegistryMiddleware(
    ))
)
