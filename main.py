from itertools import batched
from typing import Literal

import uvicorn
from fastapi import FastAPI, UploadFile, BackgroundTasks
from redis.asyncio import Redis
from logging import getLogger

from pydantic import BaseModel
from redis.asyncio.lock import Lock

from broker import broker, task_result_serializer, RedisKeys
from config import config

from protocol import (
    URICheckReport
)
from resolvers import (
    dns_resolver,
)


logger = getLogger("MAIN")

app = FastAPI(title="URI Checker")


def validate_url(url: str):
    if "//" not in url:
        url = "http://" + url

    return url


type URIField = str
type HTTPMethodField = Literal["HEAD", "GET"]


redis = Redis(
    host=config.redis.host,
    port=config.redis.port,
    db=1,
)


class CheckerFeedRequest(BaseModel):
    uris: list[URIField]


class CheckerFeedResponse(BaseModel):
    queued_count: int


class CheckerConsumeResponse(BaseModel):
    results: list[URICheckReport]
    pending: int


class GetStatusResponse(BaseModel):
    pending_consume: int



@app.post(
    "/checker/feed",
    response_model=CheckerFeedResponse,
)
async def ping_feed(
        payload: CheckerFeedRequest,
):
    for i in batched(payload.uris, config.app.dns.batch_size):
        task_payload = [URICheckReport(uri=j) for j in i]
        await dns_resolver.kiq(payload=task_payload)

    return CheckerFeedResponse(
        queued_count=len(payload.uris),
    )


@app.post(
    "/checker/feed/file",
    response_model=CheckerFeedResponse,
)
async def feed_requests(
        payload: UploadFile,
):
    text = await payload.read()
    uris = list(filter(None, map(lambda x: x.strip(), text.splitlines())))
    internal_payload = CheckerFeedRequest(
        uris=uris,
    )
    return await ping_feed(
        payload=internal_payload,
    )


consume_lock = Lock(redis, name="web_consume_lock")


async def apply_consume(
        consumed_names: list[str],
        consumed_items_count: int,
        trailing_index: int,
):
    try:
        await redis.lpop(RedisKeys.CONSUME_QUEUE, len(consumed_names))

        if consumed_names:
            await redis.delete(*consumed_names)

        if trailing_index is None:
            await redis.delete(RedisKeys.CONSUME_QUEUE_TRAILING_INDEX)
        else:
            await redis.set(RedisKeys.CONSUME_QUEUE_TRAILING_INDEX, trailing_index)

        await redis.decrby(RedisKeys.CONSUME_QUEUE_ITEMS_COUNT, consumed_items_count)
    finally:
        await consume_lock.release()


@app.get(
    "/resolver/status",
    response_model=GetStatusResponse,
)
async def get_status(
):
    pending_consume = await redis.get(RedisKeys.CONSUME_QUEUE_ITEMS_COUNT)
    if pending_consume is None:
        pending_consume = "0"

    return GetStatusResponse(
        pending_consume=pending_consume,
    )


@app.post(
    "/resolver/consume",
    response_model=CheckerConsumeResponse,
)
async def consume_responses(
        background_tasks: BackgroundTasks,
        limit: int = 1_000,
):
    await consume_lock.acquire()
    try:
        results = []
        consumed_names = []
        remaining_items_quota = limit

        trailing_index = await redis.get(RedisKeys.CONSUME_QUEUE_TRAILING_INDEX)
        initial_trailing_index = trailing_index
        all_names = await redis.lrange(
            RedisKeys.CONSUME_QUEUE,
            0, -1,
        )
        if all_names is None:
            all_names = []
        for name in all_names:
            # I think that latency here will be so small that
            #  conviency is prefered than bulk optimisations profit.

            data = await redis.get(name)
            data = task_result_serializer.loadb(data)
            data = data["return_value"]
            if trailing_index is not None:
                trailing_index = int(trailing_index)
                data = data[trailing_index:]
                trailing_index = None

            remaining_items_quota -= len(data)

            if remaining_items_quota == 0:
                new_trailing_index = None
                results.extend(data)
                consumed_names.append(name)
                break
            elif remaining_items_quota < 0:
                data = data[:remaining_items_quota]
                results.extend(data)
                new_trailing_index = len(data)
                if initial_trailing_index is not None:
                    new_trailing_index += int(initial_trailing_index)
                break
            else:
                consumed_names.append(name)
                results.extend(data)
        else:
            new_trailing_index = None

        background_tasks.add_task(
            apply_consume,
            consumed_names=consumed_names,
            consumed_items_count=len(results),
            trailing_index=new_trailing_index,
        )
        total_items_count = await redis.get(RedisKeys.CONSUME_QUEUE_ITEMS_COUNT)

        if total_items_count is None:
            total_items_count = "0"
        total_items_count = int(total_items_count)

        pending = total_items_count - len(results)
        response = CheckerConsumeResponse(
            results=results,
            pending=pending,
        )
        return response
    except BaseException:
        # note: background task will not be run due error, so you need
        #  to release lock in order to prevent deadlock
        await consume_lock.release()
        raise


@app.on_event("startup")
async def on_startup() -> None:
    await broker.startup()


@app.on_event("shutdown")
async def on_startup() -> None:
    await broker.shutdown()


def main():
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == '__main__':
    main()
