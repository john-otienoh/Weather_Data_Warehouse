"""
ORM table definitions.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger, Boolean, Date, DateTime,
    Integer, Numeric, SmallInteger, String, Text
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class WeatherEvent(Base):
    """raw.weather_events — one row per city per ingest run (JSONB payload)."""

    __tablename__ = "weather_events"
    __table_args__ = {"schema": "bronze"}

    id:         Mapped[int]      = mapped_column(BigInteger, primary_key=True)
    city:       Mapped[str]      = mapped_column(String(100), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source:     Mapped[str]      = mapped_column(String(50), default="open-meteo")
    payload:    Mapped[dict]     = mapped_column(JSONB, nullable=False)

    def __repr__(self) -> str:
        return f"<WeatherEvent city={self.city!r} fetched_at={self.fetched_at}>"

class WeatherReading(Base):
    """silver.weather_readings — parsed hourly readings from Stage 1."""

    __tablename__ = "weather_readings"
    __table_args__ = {"schema": "silver"}

    id:                  Mapped[int]               = mapped_column(BigInteger, primary_key=True)
    city:                Mapped[str]               = mapped_column(String(100), nullable=False)
    recorded_at:         Mapped[datetime]          = mapped_column(DateTime(timezone=True), nullable=False)
    fetched_at:          Mapped[datetime]          = mapped_column(DateTime(timezone=True), nullable=False)
    temp_celsius:        Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    apparent_temp:       Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    humidity_pct:        Mapped[Optional[int]]     = mapped_column(SmallInteger)
    precipitation_mm:    Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 3))
    rain_mm:             Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 3))
    weather_code:        Mapped[Optional[int]]     = mapped_column(SmallInteger)
    pressure_hpa:        Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 2))
    cloud_cover_pct:     Mapped[Optional[int]]     = mapped_column(SmallInteger)
    wind_speed_mps:      Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    wind_direction_deg:  Mapped[Optional[int]]     = mapped_column(SmallInteger)

    def __repr__(self) -> str:
        return (
            f"<WeatherReading city={self.city!r} "
            f"recorded_at={self.recorded_at} "
            f"temp={self.temp_celsius}°C>"
        )

class DailySummary(Base):
    """gold.daily_summary — daily aggregates written by Stage 4."""

    __tablename__ = "daily_summary"
    __table_args__ = {"schema": "gold"}

    city:           Mapped[str]               = mapped_column(String(100), primary_key=True)
    summary_date:   Mapped[date]              = mapped_column(Date, primary_key=True)
    avg_temp:       Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    min_temp:       Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    max_temp:       Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    avg_humidity:   Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    avg_pressure:   Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 2))
    total_precip:   Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 3))
    total_rain:     Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 3))
    avg_wind_speed: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    dominant_code:  Mapped[Optional[int]]     = mapped_column(SmallInteger)
    created_at:     Mapped[datetime]          = mapped_column(DateTime(timezone=True))
    updated_at:     Mapped[datetime]          = mapped_column(DateTime(timezone=True))


class MonthlySummary(Base):
    """gold.monthly_summary — monthly aggregates written by Stage 4."""

    __tablename__ = "monthly_summary"
    __table_args__ = {"schema": "gold"}

    city:           Mapped[str]               = mapped_column(String(100), primary_key=True)
    year:           Mapped[int]               = mapped_column(SmallInteger, primary_key=True)
    month:          Mapped[int]               = mapped_column(SmallInteger, primary_key=True)
    avg_temp:       Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    min_temp:       Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    max_temp:       Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    total_precip:   Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 3))
    total_rain:     Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 3))
    rainy_days:     Mapped[Optional[int]]     = mapped_column(SmallInteger)
    avg_wind_speed: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    created_at:     Mapped[datetime]          = mapped_column(DateTime(timezone=True))
    updated_at:     Mapped[datetime]          = mapped_column(DateTime(timezone=True))


class TemperatureAnomaly(Base):
    """gold.temperature_anomalies — anomaly flags written by Stage 4."""

    __tablename__ = "temperature_anomalies"
    __table_args__ = {"schema": "gold"}

    id:           Mapped[int]               = mapped_column(BigInteger, primary_key=True)
    city:         Mapped[str]               = mapped_column(String(100), nullable=False)
    recorded_at:  Mapped[datetime]          = mapped_column(DateTime(timezone=True), nullable=False)
    temp_celsius: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    baseline_avg: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    deviation:    Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    is_anomaly:   Mapped[bool]              = mapped_column(Boolean, default=False)
    detected_at:  Mapped[datetime]          = mapped_column(DateTime(timezone=True))

