from app.config import settings
from app.store.base import Store
from app.store.clickhouse import ClickHouseStore
from app.store.influx import InfluxStore
from app.store.observe import ObservedStore
from app.store.questdb import QuestDBStore
from app.store.timescale import TimescaleStore
from app.store.victoriametrics import VictoriaMetricsStore


def create_store() -> Store:
    name = settings.storage
    if name == "timescaledb":
        inner: Store = TimescaleStore(settings.postgres_dsn)
    elif name == "clickhouse":
        inner = ClickHouseStore(settings.clickhouse_url, settings.clickhouse_db)
    elif name == "questdb":
        inner = QuestDBStore(settings.questdb_url, settings.questdb_ilp)
    elif name == "influxdb":
        inner = InfluxStore(settings.influx_url, settings.influx_token, settings.influx_org, settings.influx_bucket)
    elif name == "victoriametrics":
        inner = VictoriaMetricsStore(settings.vm_url)
    else:
        raise ValueError(f"unknown PRISM_STORAGE {name!r}")
    return ObservedStore(inner)
