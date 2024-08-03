from itertools import islice
from typing import Literal, Coroutine, Union, Any

import taskiq
import uvicorn
from fastapi import FastAPI, UploadFile
from redis.asyncio import Redis
from logging import getLogger

from pydantic import BaseModel
from taskiq import TaskiqMessage, TaskiqResult

from broker import broker, task_result_serializer, redis, CONSUME_QUEUE_KEY
from config import config

from protocol import (
    URICheckReport
)
from resolvers import (
    dns_resolver,
)


logger = getLogger("MAIN")

app = FastAPI(name="TheArcherST's HTTP ping for FL")

Protocol = Literal["icmp", "tcp", "udp"]


def validate_url(url: str):
    if "//" not in url:
        url = "http://" + url

    return url


type URIField = str
type HTTPMethodField = Literal["HEAD", "GET"]


class CheckerFeedRequest(BaseModel):
    uris: list[URIField]


class CheckerFeedResponse(BaseModel):
    pass


class CheckerConsumeResponse(BaseModel):
    results: list[URICheckReport]
    consume_queue_len: int


def batched(iterable, n):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch


@app.post(
    "/checker/feed",
    response_model=CheckerFeedResponse,
)
async def ping_feed(
        payload: CheckerFeedRequest,
):
    for i in batched(payload.uris, config.app.dns.batch_size):
        payload = [URICheckReport(uri=j) for j in i]
        await dns_resolver.kiq(payload=payload)

    return CheckerFeedResponse(
    )


@app.post(
    "/checker/feed/file",
    response_model=CheckerFeedResponse,
)
async def feed_requests(
        payload: UploadFile,
):
    text = await payload.read()
    uris = text.splitlines()
    internal_payload = CheckerFeedRequest(
        uris=uris,
    )
    return await ping_feed(
        payload=internal_payload,
    )


@app.post(
    "/ping/consume",
    response_model=CheckerConsumeResponse,
)
async def consume_responses(
        limit: int = 1_000,
):
    results = []

    # todo: ensure that consumer recieved data before remove keys and
    #  values
    names = await redis.rpop(CONSUME_QUEUE_KEY, int(limit / config.app.http.batch_size))
    if names is None:
        names = []
    for name, i in zip(names, await redis.mget(names)):
        if i is None:
            names.remove(name)
            continue
        msg = task_result_serializer.loadb(i)
        if msg["is_err"]:
            logger.warning(f"Recieved task with err response: {msg}")
            continue
        data: list[URICheckReport] = msg["return_value"]
        if data is None:
            logger.error(f"Bad task result: {msg}")
            continue
        results.extend(data)
    if names:
        await redis.delete(*names)
    response = CheckerConsumeResponse(
        results=results,
        consume_queue_len=await redis.llen(CONSUME_QUEUE_KEY),
    )
    return response


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
