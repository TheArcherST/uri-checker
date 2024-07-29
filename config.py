from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisConfig(BaseModel):
    host: str
    port: int


class RabbitmqConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    vhost: str


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
    )

    redis: RedisConfig
    rabbitmq: RabbitmqConfig


config = Config()
