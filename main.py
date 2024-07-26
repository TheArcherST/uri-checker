import asyncio
import urllib.parse
from typing import Literal
from urllib.parse import urljoin

import uvicorn
from aioping import ping
from fastapi import FastAPI, Query, UploadFile

import taskiq
from taskiq_aio_pika import AioPikaBroker
from pydantic import BaseModel
import httpx


app = FastAPI(name="TheArcherST's HTTP ping for FL")

Protocol = Literal["icmp", "tcp", "udp"]


def validate_url(url: str):
    if "//" not in url:
        url = "http://" + url

    return url


type URIField = str


class URIAvailability(BaseModel):
    protocol: Protocol
    uri: URIField
    is_reachable: bool
    delay: int | None


class HTTPURIAvailability(URIAvailability):
    protocol: Protocol = "http"
    status_code: int | None


class ICMPURIAvailability(URIAvailability):
    protocol: Protocol = "icmp"


class DiscoverURIRequest(BaseModel):
    uri: URIField
    type: Literal["website"] = "website"


class DiscoverURIResponse(BaseModel):
    uri: URIField
    results: list[HTTPURIAvailability | ICMPURIAvailability]


class BatchURIDiscoverResponse(BaseModel):
    results: list[DiscoverURIResponse]


class BatchURIDiscoverRequest(BaseModel):
    requests: list[DiscoverURIRequest]


broker = AioPikaBroker(
    "amqp://guest:guest@localhost:5672",
    max_async_tasks=500,
    max_stored_results=2000,
    sync_tasks_pool_size=4,
)


def yield_possible_http_resources(request: DiscoverURIRequest) -> list[str]:
    if request.type == "website":
        postfixes = [
            "/",
            "index.html",
            "index",
            # "index.php",
            # "info.html",
            # "info",
        ]

        for i in postfixes:
            yield urljoin(str(validate_url(request.uri)), i)


@broker.task
async def ping_uri(request: DiscoverURIRequest, is_verbose: bool) -> list[HTTPURIAvailability | ICMPURIAvailability]:
    tasks = []
    splited = urllib.parse.urlsplit(validate_url(request.uri))
    tasks.append(await ping_icmp_uri.kiq(splited.netloc, is_verbose=is_verbose))
    for i in yield_possible_http_resources(request):
        task = await ping_http_uri.kiq(i, is_verbose=is_verbose)
        tasks.append(task)

    results = []
    for i in tasks:
        i: taskiq.AsyncTaskiqTask
        result = await i.wait_result()
        resource_availability = result.return_value
        results.append(resource_availability)

    return results


c = 0


@broker.task
async def ping_icmp_uri(
        uri: str,
        timeout: int = 2,
        is_verbose: bool = True,
) -> ICMPURIAvailability:
    global c
    print(f"[ICMP] [{uri}]: START")
    c += 1
    print(f"[ICMP] [system]: {c} requests in parallel")
    try:
        result = await ping(uri, timeout=timeout)
    except (TimeoutError, OSError):  # todo: handle os error properly os err
        is_reachable = False
        delay = None
    else:
        is_reachable = True
        delay = result
    c -= 1
    print(f"[ICMP] [system]: {c} requests in parallel")
    print(f"[ICMP] [{uri}]: DONE")
    return ICMPURIAvailability.model_validate(dict(
        uri=uri,
        is_reachable=is_reachable,
        delay=int(delay * 1000) if delay is not None else None,
    ))

c2 = 0


@broker.task
async def ping_http_uri(
        uri: str,
        timeout: int = 1,
        is_verbose: bool = True
) -> HTTPURIAvailability:
    global c2
    print(f"[HTTP] [{uri}]: START")
    c2 += 1
    print(f"[HTTP] [system]: {c2} requests in parallel")
    async with httpx.AsyncClient() as session:
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        try:
            response = await session.get(uri, timeout=timeout)
        except httpx.ConnectTimeout:
            is_reachable = False
            status_code = None
        else:
            is_reachable = True
            status_code = response.status
        ended_at = loop.time()
        c2 -= 1
        print(f"[HTTP] [{uri}]: DONE")
        print(f"[HTTP] [system]: {c2} requests in parallel")

        return HTTPURIAvailability.model_validate(dict(
            uri=uri,
            is_reachable=is_reachable,
            status_code=status_code,
            delay=int((ended_at-started_at) * 1000),
        ))


@app.post(
    "/ping",
    response_model=BatchURIDiscoverResponse,
)
async def root(
        payload: BatchURIDiscoverRequest,
        is_verbose: bool = Query(validation_alias="v", default=True),
):
    tasks = []
    results = []
    for i in payload.requests:
        i: DiscoverURIRequest
        task = await ping_uri.kiq(i, is_verbose=True)
        tasks.append((i, task))
    for request, task in tasks:
        result_ = await task.wait_result()
        results.append(DiscoverURIResponse(
            uri=request.uri,
            results=result_.return_value,
        ))

    return BatchURIDiscoverResponse.model_validate(dict(
        results=results,
    ))


@app.post(
    "/ping/file",
    response_model=BatchURIDiscoverResponse,
)
async def root_file(
        payload: UploadFile,
        is_verbose: bool = Query(validation_alias="v", default=False),
):
    text = await payload.read()
    uris = text.splitlines()
    requests = []
    for i in uris:
        requests.append(DiscoverURIRequest.model_validate(dict(
            uri=i,
        )))
    payload = BatchURIDiscoverRequest(requests=requests)
    return await root(
        payload=payload,
        is_verbose=is_verbose,
    )


def main():
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == '__main__':
    main()
