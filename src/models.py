from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


# Mapping from API camelCase keys to snake_case field names
_API_FIELD_MAP = {
    "figi": "figi",
    "compositeFIGI": "composite_figi",
    "shareClassFIGI": "share_class_figi",
    "name": "name",
    "ticker": "ticker",
    "exchCode": "exch_code",
    "securityType": "security_type",
    "securityType2": "security_type2",
    "marketSector": "market_sector",
    "securityDescription": "security_description",
}

_REVERSE_FIELD_MAP = {v: k for k, v in _API_FIELD_MAP.items()}


@dataclass
class FigiRecord:
    figi: str = ""
    composite_figi: str = ""
    share_class_figi: str = ""
    name: str = ""
    ticker: str = ""
    exch_code: str = ""
    security_type: str = ""
    security_type2: str = ""
    market_sector: str = ""
    security_description: str = ""

    @classmethod
    def from_api(cls, data: dict) -> FigiRecord:
        kwargs = {}
        for api_key, field_name in _API_FIELD_MAP.items():
            kwargs[field_name] = data.get(api_key) or ""
        return cls(**kwargs)

    def to_dict(self) -> dict:
        return {
            api_key: getattr(self, field_name)
            for field_name, api_key in _REVERSE_FIELD_MAP.items()
        }

    def to_csv_row(self) -> dict:
        return {
            "figi": self.figi,
            "composite_figi": self.composite_figi,
            "share_class_figi": self.share_class_figi,
            "name": self.name,
            "ticker": self.ticker,
            "exch_code": self.exch_code,
            "security_type": self.security_type,
            "security_type2": self.security_type2,
            "market_sector": self.market_sector,
            "security_description": self.security_description,
        }


@dataclass
class ExchangeSnapshot:
    exch_code: str
    total: int
    figis: list[str] = field(default_factory=list)  # composite FIGIs
    last_scanned: str = ""

    def to_dict(self) -> dict:
        return {
            "exch_code": self.exch_code,
            "total": self.total,
            "figis": self.figis,
            "last_scanned": self.last_scanned,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ExchangeSnapshot:
        return cls(
            exch_code=data["exch_code"],
            total=data["total"],
            figis=data.get("figis", []),
            last_scanned=data.get("last_scanned", ""),
        )


@dataclass
class FullSnapshot:
    timestamp: str = ""
    exchanges: dict[str, ExchangeSnapshot] = field(default_factory=dict)
    all_composite_figis: set[str] = field(default_factory=set)

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "exchanges": {
                code: snap.to_dict() for code, snap in self.exchanges.items()
            },
            "all_composite_figis": sorted(self.all_composite_figis),
        }

    @classmethod
    def from_dict(cls, data: dict) -> FullSnapshot:
        exchanges = {
            code: ExchangeSnapshot.from_dict(snap_data)
            for code, snap_data in data.get("exchanges", {}).items()
        }
        return cls(
            timestamp=data.get("timestamp", ""),
            exchanges=exchanges,
            all_composite_figis=set(data.get("all_composite_figis", [])),
        )

    @staticmethod
    def now_timestamp() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class DiffResult:
    new_records: list[FigiRecord] = field(default_factory=list)
    exchanges_checked: int = 0
    exchanges_changed: int = 0
    total_before: int = 0
    total_after: int = 0

    @property
    def new_count(self) -> int:
        return len(self.new_records)

    def summary(self) -> str:
        lines = [
            f"Exchanges checked:  {self.exchanges_checked}",
            f"Exchanges changed:  {self.exchanges_changed}",
            f"Total before:       {self.total_before}",
            f"Total after:        {self.total_after}",
            f"New composite FIGIs: {self.new_count}",
        ]
        return "\n".join(lines)
