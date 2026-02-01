"""Gateway config. Table services = compose service names (DNS)."""
from pydantic_settings import BaseSettings

TABLE_SERVICES = [
    "line_mst",
    "equip_mst",
    "sensor_mst",
    "worker_mst",
    "shift_cfg",
    "kpi_cfg",
    "alarm_cfg",
    "maint_cfg",
    "measurement",
    "status_his",
    "prod_his",
    "alarm_his",
    "maint_his",
    "shift_map",
    "kpi_sum",
]


class Settings(BaseSettings):
    """Override via env if table services run elsewhere."""
    TABLE_SERVICE_PORT: int = 8000

    def base_url(self, service: str) -> str:
        return f"http://{service}:{self.TABLE_SERVICE_PORT}"


settings = Settings()
