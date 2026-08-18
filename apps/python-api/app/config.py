from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    prism_storage: str = "influxdb"
    http_addr: str = "0.0.0.0:8082"
    postgres_dsn: str = "postgres://prism:prism@timescaledb:5432/prism"
    clickhouse_url: str = "http://prism:prism@clickhouse:8123"
    clickhouse_db: str = "prism"
    influx_url: str = "http://influxdb:8086"
    influx_token: str = "prism-dev-token"
    influx_org: str = "prism"
    influx_bucket: str = "prism"
    vm_url: str = "http://victoriametrics:8428"
    nats_url: str = "nats://nats:4222"
    nats_subject: str = "prism.points"

    @property
    def storage(self) -> str:
        return self.prism_storage.lower()


settings = Settings()
SUPPORTED = ["timescaledb", "clickhouse", "influxdb", "victoriametrics"]
