import asyncio
from typing import Annotated
import logging

import aiodns
from idna import InvalidCodepoint
from taskiq import (
    TaskiqDepends,
    Context,
)

from broker import broker
from config import config
from protocol import (
    DNSResolverPayload,
    DNSResolverResponse, DNSResult, URICheckReport,
)

from resolvers.http import http_resolver


logger = logging.getLogger("DNS resolver")


@broker.task
async def dns_resolver(
        payload: DNSResolverPayload,
) -> DNSResolverResponse:
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

    finished_reports = []
    reports_pending_http = []
    tasks = []

    for i in payload:
        try:
            task = resolver.query(i.uri, "A")
        except InvalidCodepoint as e:
            i.dns = DNSResult(
                status=-1,
                ips=None,
            )
            finished_reports.append(i)
            logger.error(
                f"Domain name {i.uri} not handled because of "
                f" domain name is invalid: {e}"
            )
        else:
            tasks.append(task)
            task.report = i

    while tasks:
        await asyncio.sleep(0.1)
        c = 0
        for i in tasks.copy():
            if not i.done():
                continue
            tasks.remove(i)
            current_report: URICheckReport = i.report
            c += 1
            log_level = logging.DEBUG
            try:
                result = i.result()
            except aiodns.error.DNSError as e:
                status, message = e.args
                if status == 1:
                    # domain name not found
                    # result is an empty set
                    current_report.dns = DNSResult(
                        status=status,
                        ips=set(),
                    )
                    finished_reports.append(current_report)
                elif status == 4:
                    # domain name not exits
                    current_report.dns = DNSResult(
                        status=status,
                        ips=None,
                    )
                    finished_reports.append(current_report)
                elif status in (
                        12,  # Timeout while contacting DNS servers
                        11,  #
                        10,  # Misformatted DNS reply
                        8,   # Misformatted domain name
                ):
                    current_report.dns = DNSResult(
                        status=status,
                        ips=None,
                    )
                    finished_reports.append(current_report)
                    log_level = logging.WARNING
                else:
                    current_report.dns = DNSResult(
                        status=status,
                        ips=None,
                    )
                    finished_reports.append(current_report)
                    log_level = logging.ERROR

                logger.log(
                    log_level,
                    f"Domain name {current_report.uri} not handled because of DNS"
                    f" error `{status}`."
                )
            else:
                logger.info(f"[DNS] Result: {i!r} for domain {current_report.uri}")
                hosts = [j.host for j in result]
                current_report.dns = DNSResult(
                    status=0,
                    ips=hosts,
                )
                reports_pending_http.append(current_report)

        logger.info(f"{c} finished")

        if len(reports_pending_http) >= config.app.http.batch_size:
            logger.info(f"{len(reports_pending_http)} domains supplied to http pipeline")
            payload = reports_pending_http
            await http_resolver.kiq(
                payload=payload,
            )
            reports_pending_http = []
    else:
        logger.info(
            f"{len(reports_pending_http)} domains supplied to http pipeline")
        payload = reports_pending_http
        await http_resolver.kiq(
            payload=payload,
        )

    return finished_reports
