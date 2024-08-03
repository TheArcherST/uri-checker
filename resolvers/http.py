import asyncio
import ssl
from typing import (
    Annotated,
    Iterable,
)

import logging
import taskiq.events
from taskiq import (
    Context,
    TaskiqDepends, TaskiqState,
)

import httpx
from broker import broker
from config import config
from protocol import (
    HTTPResolverPayload,
    HTTPResolverResponse,
    HTTPMethodField, HTTPResult,
)


logger = logging.getLogger("HTTP resolver")


@broker.on_event(taskiq.events.TaskiqEvents.WORKER_STARTUP)
async def on_worker_startup(
        state: Annotated[TaskiqState, TaskiqDepends()]
):
    state.http_limits = httpx.Limits(
        max_connections=config.app.http.transport.max_connections,
        max_keepalive_connections=config.app.http.transport.max_keppalive_connections,
    )
    state.http_transport = httpx.AsyncHTTPTransport(
        limits=state.http_limits,
    )


@broker.task
async def http_resolver(
        context: Annotated[Context, TaskiqDepends()],
        payload: HTTPResolverPayload,
) -> HTTPResolverResponse:
    tasks = []

    if not config.app.http.global_transport:
        current_http_transport = httpx.HTTPTransport(
            limits=context.state.http_limits,
        )
    else:
        current_http_transport = context.state.http_transport

    for i in payload:
        tasks.append((i, asyncio.create_task(discover_uri(
            transport=current_http_transport,
            uri=i.uri,
            ips=i.dns.ips,
            method=config.app.http.method,
        ))))

    while tasks:
        await asyncio.sleep(0.1)
        c = 0
        for i in tasks.copy():
            if not i[1].done():
                continue
            tasks.remove(i)
            c += 1

            result: HTTPResult = i[1].result()
            logger.info(f"{result=}")
            i[0].http = result
        logger.info(f"{c} finished")
    logger.info("Return results!")
    return payload


async def discover_uri(
        transport: httpx.AsyncHTTPTransport,
        uri: str,
        ips: Iterable[str] | None,
        method: HTTPMethodField,
) -> HTTPResult:
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

        if ips is not None:
            ips = list(ips)

        response = None
        if config.app.http.use_manual_dns:
            if ips is None:
                logger.debug(f"IPS if none so skip HTTP for {uri}")
                return HTTPResult(
                    method=method,
                    status_code=None,
                    content_length=None,
                    content=None,
                    redirects=None,
                )

            for i in ips:
                logger.debug(f"Try ip {i} for {uri}")
                try:
                    response = await client.request(
                        method, httpx.URL(
                            scheme="https",
                            host=i,
                            path="/",
                        ),
                        headers={
                            "host": uri,
                        },
                        extensions=dict(
                            sni_hostname=uri,
                        ),
                    )
                except httpx.TransportError as e:
                    logger.warning(f"Transport error for {uri} - {e}")
                    if config.app.http.try_all_ips:
                        continue
                    break
                except ssl.SSLError as e:
                    # todo: any error markers?
                    logger.warning(f"SSL error for {uri} - {e}")
                    pass
                else:
                    break
            else:
                logger.warning(f"Not one ip from {ips} not work for {uri}")
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
            return HTTPResult(
                method=method,
                status_code=None,
                content_length=None,
                content=None,
                redirects=None,
            )
        content = await response.aread()
        return HTTPResult(
            method=method,
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
