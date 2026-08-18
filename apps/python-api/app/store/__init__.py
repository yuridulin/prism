from app.config import settings
from app.store.base import Store
from app.store.clickhouse import ClickHouseStore
from app.store.influx import InfluxStore
from app.store.timescale import TimescaleStore
from app.store.victoriametrics import VictoriaMetricsStore


def create_store() -> Store:
    name = settings.storage
    if name == "timescaledb":
        return TimescaleStore(settings.postgres_dsn)
    if name == "clickhouse":
        return ClickHouseStore(settings.clickhouse_url, settings.clickhouse_db)
    if name == "influxdb":
        return InfluxStore(
            settings.influx_url,
            settings.influx_token,
            settings.influx_org,
            settings.influx_bucket,
        )
    if name == "victoriametrics":
        return VictoriaMetricsStore(settings.vm_url)
    raise ValueError(f"unknown PRISM_STORAGE {name!r}")
