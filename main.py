import asyncio
import urllib.parse
from typing import Literal
from urllib.parse import urljoin

import uvicorn
from aioping import ping
from fastapi import FastAPI, Query, UploadFile

import taskiq
from pydantic import BaseModel, AnyUrl
import httpx

from broker import broker

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
    protocol: Protocol = "tcp"
    status_code: int | None


class ICMPURIAvailability(URIAvailability):
    protocol: Protocol = "icmp"


class DiscoverURIRequest(BaseModel):
    uri: URIField
    type: Literal["website"] = "website"


class DiscoverURIResponse(BaseModel):
    uri: URIField
    results: list[HTTPURIAvailability | ICMPURIAvailability]


class DiscoverURIShortResponse(BaseModel):
    uri: str
    is_icmp_reachable: bool
    is_http_reachable: bool
    available_urls: set[AnyUrl]


class BatchURIDiscoverResponse(BaseModel):
    results: list[DiscoverURIResponse | DiscoverURIShortResponse]


class BatchURIDiscoverRequest(BaseModel):
    requests: list[DiscoverURIRequest]


def yield_possible_http_resources(request: DiscoverURIRequest, is_detailed: bool) -> list[str]:
    if request.type == "website":
        if is_detailed:
            postfixes = [
                "/",
                "index",
                "about",
            ]
        else:
            postfixes = [
                "/",
            ]

        for i in postfixes:
            yield urljoin(str(validate_url(request.uri)), i)


@broker.task
async def ping_uri(request: DiscoverURIRequest, is_verbose: bool, is_detailed: bool) -> list[HTTPURIAvailability | ICMPURIAvailability]:
    tasks = []
    splited = urllib.parse.urlsplit(validate_url(request.uri))
    tasks.append(asyncio.create_task(ping_icmp_uri(splited.netloc, is_verbose=is_verbose)))
    for i in yield_possible_http_resources(request, is_detailed=is_detailed):
        task = asyncio.create_task(ping_http_uri(i, is_verbose=is_verbose, is_detailed=is_detailed))
        tasks.append(task)

    results = []
    for i in tasks:
        while not i.done():
            await asyncio.sleep(0.2)
        result = i.result()
        resource_availability = result
        results.append(resource_availability)

    return results


async def ping_icmp_uri(
        uri: str,
        timeout: int = 2,
        is_verbose: bool = True,
) -> ICMPURIAvailability:
    try:
        result = await ping(uri, timeout=timeout)
    except (TimeoutError, OSError):  # todo: handle os error properly os err
        is_reachable = False
        delay = None
    else:
        is_reachable = True
        delay = result
    return ICMPURIAvailability.model_validate(dict(
        uri=uri,
        is_reachable=is_reachable,
        delay=int(delay * 1000) if delay is not None else None,
    ))


async def ping_http_uri(
        uri: str,
        timeout: int = 2,
        is_verbose: bool = True,
        is_detailed: bool = True,
) -> HTTPURIAvailability:
    async with httpx.AsyncClient() as session:
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        try:
            response = await session.get(uri, timeout=timeout)
        except httpx.TransportError:
            is_reachable = False
            status_code = None
        else:
            is_reachable = True
            status_code = response.status_code
        ended_at = loop.time()

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
        is_detailed: bool = Query(default=True),
):
    tasks = []
    results = []
    for i in payload.requests:
        i: DiscoverURIRequest
        task = await ping_uri.kiq(i, is_verbose=True, is_detailed=is_detailed)
        tasks.append((i, task))
    for request, task in tasks:
        result_ = await task.wait_result()
        results_ = list(result_.return_value)
        if is_verbose:
            results.append(DiscoverURIResponse(
                uri=request.uri,
                results=results_
            ))
        else:
            results.append(DiscoverURIShortResponse(
                uri=request.uri,
                is_icmp_reachable=bool(len([i for i in results_ if i["protocol"] == "icmp"])),
                is_http_reachable=bool(len([i for i in results_ if i["protocol"] == "tcp"])),
                available_urls={i["uri"] for i in results_ if
                                (i["protocol"] == "tcp"
                                 and ((i["status_code"] or 0) // 100 in (2, 3)))}
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
        is_detailed: bool = Query(default=False),
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
        is_detailed=is_detailed,
    )


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
