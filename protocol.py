from typing import Literal, Annotated

from pydantic import BaseModel, AfterValidator

type URIField = str
type HTTPMethodField = Literal["HEAD", "GET"]


class CheckURIsRequest(BaseModel):
    method: HTTPMethodField
    uris: list[str]


class DNSResult(BaseModel):
    status: int
    ips: list[str] | None
    detail: str | None = None


class HTTPResult(BaseModel):
    method: HTTPMethodField
    status_code: int | None
    content_length: int | None
    content: str | None = None
    redirects: list[tuple[int, str]] | None
    detail: str | None = None


class URICheckReport(BaseModel):
    uri: Annotated[URIField, AfterValidator(str.strip)]
    dns: DNSResult | None = None
    http: HTTPResult | None = None


type DNSResolverPayload = list[URICheckReport]
type DNSResolverResponse = list[URICheckReport]
type HTTPResolverPayload = list[URICheckReport]
type HTTPResolverResponse = list[URICheckReport]
