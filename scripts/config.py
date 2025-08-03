import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    host: str
    port: int
    user: str
    password: str
    database: str

@dataclass
class WeatherAPIConfig:
    """Weather API configuration"""
    base_url: str = "https://api.open-meteo.com/v1/forecast"
    latitude: float = 2.0167
    longitude: float = 117.3000
    timezone: str = "Asia/Jakarta"

@dataclass
class Config:
    """Main configuration class"""
    mysql: DatabaseConfig
    clickhouse: DatabaseConfig
    weather_api: WeatherAPIConfig
    csv_file_path: str
    log_level: str = "INFO"
    batch_size: int = 1000

def get_config() -> Config:
    """Get configuration from environment variables"""
    return Config(
        mysql=DatabaseConfig(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "root"),
            database=os.getenv("MYSQL_DATABASE", "coal_mining")
        ),
        clickhouse=DatabaseConfig(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_DATABASE", "coal_mining_dw")
        ),
        weather_api=WeatherAPIConfig(),
        csv_file_path=os.getenv("CSV_FILE_PATH", "/app/data/equipment_sensors.csv"),
        log_level=os.getenv("LOG_LEVEL", "INFO")
    )