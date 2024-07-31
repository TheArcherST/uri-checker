import asyncio
import json
import logging
import ssl
from itertools import islice
from typing import Literal, Iterable

import aiodns
import httpx
import uvicorn
from fastapi import FastAPI, UploadFile, Query
from fastapi.openapi.models import Response
from redis.asyncio import Redis
from logging import getLogger

from pydantic import BaseModel
from starlette.responses import JSONResponse

from broker import broker, task_result_serializer
from config import config

logger = getLogger(__name__)

CONSUME_QUEUE_KEY = "consume_queue"


app = FastAPI(name="TheArcherST's HTTP ping for FL")

Protocol = Literal["icmp", "tcp", "udp"]


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


class DiscoverRequests(BaseModel):
    method: HTTPMethodField
    uris: list[URIField]


class URIResponse(BaseModel):
    method: HTTPMethodField
    uri: str
    is_exists: bool | None
    ips: list[str] | None
    is_reachable: bool | None
    status_code: int | None
    content_length: int | None
    content: str | None = None
    redirects: list[tuple[int, str]] | None


class DiscoverResponse(BaseModel):
    results: list[URIResponse]


async def discover_uri(
        transport: httpx.AsyncHTTPTransport,
        uri: str,
        ips: Iterable[str] | None,
        method: HTTPMethodField,
) -> URIResponse:
    async with httpx.AsyncClient(
        transport=transport,
        follow_redirects=True,
    ) as client:
        # redirects in general can't be optimized to be parallel etc.
        #  the only external optimisation is possible if domain name
        #  is already resolved and/or connection to this ip is already
        #  established in transport pool.  also I've rejected any
        #  attempts to use lower layer in order to optimize using
        #  CPU cycles.  for this type of optimisation I need to
        #  rewrite this in other language :)

        if config.app.http.use_manual_dns:
            response = None
            if ips is None:
                return URIResponse(
                    method=method,
                    uri=uri,
                    is_exists=False,
                    ips=None,
                    is_reachable=None,
                    status_code=None,
                    content_length=None,
                    content=None,
                    redirects=None,
                )

            for i in ips:
                try:
                    response = await client.request(
                        method, httpx.URL(
                            scheme="https",
                            host=i,
                            path="/",
                        ),
                        extensions=dict(
                            sni_hostname=uri,
                        ),
                    )
                except httpx.TransportError:
                    if config.app.http.try_all_ips:
                        continue
                    break
                else:
                    break
        else:
            try:
                response = await client.request(
                    method, httpx.URL(
                        scheme="https",
                        host=uri,
                        path="/",
                    ),
                )
            except httpx.TransportError:
                pass
            except ssl.SSLError:
                # todo: any error markers?
                pass

        if response is None:
            return URIResponse(
                method=method,
                uri=uri,
                is_exists=True,
                ips=ips,
                is_reachable=False,
                status_code=None,
                content_length=None,
                content=None,
                redirects=None,
            )
        content = await response.aread()
        return URIResponse(
            method=method,
            uri=uri,
            is_exists=ips is not None,
            ips=ips,
            is_reachable=True,
            status_code=response.status_code,
            content_length=response.headers.get("content-length"),
            content=content.decode() or None,
            redirects=(
                    [
                        (i.status_code, i.headers.get("location"))
                        for i in response.history
                    ]
            ),
        )


http_limits = httpx.Limits(
    max_connections=config.app.http.transport.max_connections,
    max_keepalive_connections=config.app.http.transport.max_keppalive_connections,
)
http_transport = httpx.AsyncHTTPTransport(
    limits=http_limits,
)


@broker.task
async def pipeline__http(
        domains: list[tuple[str, Iterable[str] | None]],
        method: HTTPMethodField,
) -> list[URIResponse]:
    tasks = []

    if not config.app.http.global_transport:
        current_http_transport = httpx.HTTPTransport(
            limits=http_limits,
        )
    else:
        current_http_transport = http_transport

    for domain, ips, in domains:
        tasks.append(asyncio.create_task(discover_uri(
            transport=current_http_transport,
            uri=domain,
            ips=ips,
            method=method,
        )))

    results = []
    while tasks:
        await asyncio.sleep(0.2)
        c = 0
        for i in tasks.copy():
            if not i.done():
                continue
            tasks.remove(i)
            c += 1

            result: URIResponse = i.result()
            results.append(result)

        logger.info(f"{c} finished")
    logger.info("Return results!")
    return results


def batched(iterable, n):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        yield batch


@broker.task
async def pipeline__dns(
        domains: list[str],
        method: HTTPMethodField,
) -> None:
    results = list()
    resolver = aiodns.DNSResolver(
        [
            "92.53.116.26",
            "92.53.98.100",
            "139.45.232.67",
            "139.45.249.139",
            "8.8.8.8",
            "8.8.4.4",
            "1.1.1.1",
            "1.0.0.1",
            "9.9.9.9",
            "149.112.112.112",
            "76.76.2.0",
            "76.76.10.0",
            "76.76.19.19",
            "76.223.122.150",
            "185.228.168.9",
            "185.228.169.9",
        ],
        timeout=config.app.dns.timeout,
        tries=config.app.dns.tries,
    )

    tasks = []
    domains = list(domains)

    for i in domains:
        task = resolver.query(i.strip(), "A")
        tasks.append(task)
        task.domain = i

    while tasks:
        await asyncio.sleep(0.2)
        c = 0
        for i in tasks.copy():
            if not i.done():
                continue
            tasks.remove(i)
            c += 1
            log_level = logging.DEBUG
            try:
                result = i.result()
            except aiodns.error.DNSError as e:
                status, message = e.args
                if status == 1:
                    # domain name not found
                    # result is an empty set
                    results.append((i.domain, set()))
                elif status == 4:
                    # domain name not exits
                    results.append((i.domain, None))
                elif status in (
                        12,  # Timeout while contacting DNS servers
                        11,  #
                        10,  # Misformatted DNS reply
                        8,   # Misformatted domain name
                ):
                    results.append((i.domain, None))
                    log_level = logging.WARNING
                else:
                    log_level = logging.ERROR
                logger.log(
                    log_level,
                    f"Domain name {i.domain} not handled because of DNS"
                    f" error `{status}`."
                )
            else:
                hosts = [i.host for i in result]
                results.append((i.domain, hosts))

        logger.info(f"{c} finished")

        if len(results) >= config.app.http.batch_size:
            logger.info(f"{len(results)} domains supplied to http pipeline")
            task = await pipeline__http.kiq(
                domains=results,
                method=method,
            )
            await redis.lpush(CONSUME_QUEUE_KEY, task.task_id)
            results = []
    else:
        logger.info(f"{len(results)} domains supplied to http pipeline")
        task = await pipeline__http.kiq(
            domains=results,
            method=method,
        )
        await redis.lpush(CONSUME_QUEUE_KEY, task.task_id)

    return None


@broker.task
async def discover_uris(
        request: DiscoverRequests,
) -> DiscoverResponse:
    tasks = []
    results = []

    for i in tasks:
        while not i.done():
            await asyncio.sleep(0.2)
        result = i.result()
        resource_availability = result
        results.append(resource_availability)

    return results


class PingFeedRequest(BaseModel):
    request: DiscoverRequests


class PingFeedResponse(BaseModel):
    pass


@app.post(
    "/ping/feed",
    response_model=PingFeedResponse,
)
async def ping_feed(
        payload: PingFeedRequest,
):
    for i in batched(payload.request.uris, config.app.dns.batch_size):
        await pipeline__dns.kiq(domains=i, method=payload.request.method)

    return PingFeedResponse(
    )


@app.post(
    "/ping/feed/file",
    response_model=PingFeedResponse,
)
async def feed_requests(
        payload: UploadFile,
        method: HTTPMethodField = Query(default="HEAD"),
):
    text = await payload.read()
    uris = text.splitlines()
    internal_payload = PingFeedRequest(
        request=DiscoverRequests(
            method=method,
            uris=uris,
        )
    )
    return await ping_feed(
        payload=internal_payload,
    )


ping_consume_lock = asyncio.Lock()


@app.post(
    "/ping/consume",
    response_model=DiscoverResponse,
)
async def consume_responses(
        limit: int = 1_000,
):
    results = []

    # todo: ensure that consumer recieved data before remove keys and
    #  values
    names = await redis.lpop(CONSUME_QUEUE_KEY, int(limit / config.app.http.batch_size))
    if names is None:
        names = []
    for name, i in zip(names, await redis.mget(names)):
        if i is None:
            names.remove(name)
            continue
        msg = task_result_serializer.loadb(i)
        if msg["is_err"]:
            logger.warning(f"Recieved task with err response: {msg}")
        data: URIResponse = msg["return_value"]
        results.extend(data)
    if names:
        await redis.delete(*names)
    response = DiscoverResponse(
        results=results,
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
