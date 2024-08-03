from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from protocol import HTTPMethodField


class RedisConfig(BaseModel):
    host: str
    port: int


class RabbitmqConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    vhost: str


class HTTPTransportConfig(BaseModel):
    max_connections: int = 200
    max_keppalive_connections: int = 100


class HTTPConfig(BaseModel):
    method: HTTPMethodField
    max_redirects: int
    batch_size: int
    reuse_session: bool
    global_transport: bool
    use_manual_dns: bool
    try_all_ips: bool
    transport: HTTPTransportConfig


class DNSConfig(BaseModel):
    batch_size: int
    timeout: float
    tries: int


class AppConfig(BaseModel):
    http: HTTPConfig
    dns: DNSConfig


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )

    redis: RedisConfig
    rabbitmq: RabbitmqConfig
    app: AppConfig


config = Config()
