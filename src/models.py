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
    """A single instrument record returned by the OpenFIGI filter API.

    All fields use snake_case internally; API wire format uses camelCase.
    The primary identity key is ``composite_figi`` (one per instrument per
    country, regardless of which exchange listing it appears on).
    """

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
        """Construct a FigiRecord from a raw API response dictionary.

        Maps camelCase API keys to snake_case field names using
        ``_API_FIELD_MAP``. Missing or falsy values are stored as empty
        strings.

        Args:
            data: A single instrument object from the OpenFIGI API response.

        Returns:
            A populated FigiRecord instance.
        """
        kwargs = {}
        for api_key, field_name in _API_FIELD_MAP.items():
            kwargs[field_name] = data.get(api_key) or ""
        return cls(**kwargs)

    def to_dict(self) -> dict:
        """Serialize this record to a camelCase dictionary suitable for JSON storage.

        Returns:
            A dict with OpenFIGI camelCase keys mapping to this record's field values.
        """
        return {
            api_key: getattr(self, field_name)
            for field_name, api_key in _REVERSE_FIELD_MAP.items()
        }

    def to_csv_row(self) -> dict:
        """Serialize this record to a flat snake_case dictionary for CSV export.

        Returns:
            A dict with snake_case keys suitable for use with csv.DictWriter.
        """
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
    """A point-in-time snapshot of all ETP composite FIGIs for one exchange.

    Attributes:
        exch_code: The OpenFIGI exchange code (e.g. ``"US"``, ``"LN"``).
        total: The total count reported by the API for this exchange/filter.
        figis: Deduplicated list of composite FIGIs seen on this exchange.
        last_scanned: ISO-8601 UTC timestamp of when this exchange was last fully paginated.
    """

    exch_code: str
    total: int
    figis: list[str] = field(default_factory=list)  # composite FIGIs
    last_scanned: str = ""

    def to_dict(self) -> dict:
        """Serialize this snapshot to a JSON-compatible dictionary.

        Returns:
            A dict with keys ``exch_code``, ``total``, ``figis``, and
            ``last_scanned``.
        """
        return {
            "exch_code": self.exch_code,
            "total": self.total,
            "figis": self.figis,
            "last_scanned": self.last_scanned,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ExchangeSnapshot:
        """Deserialize an ExchangeSnapshot from a stored dictionary.

        Args:
            data: A dict previously produced by ``to_dict()``.

        Returns:
            A populated ExchangeSnapshot instance.
        """
        return cls(
            exch_code=data["exch_code"],
            total=data["total"],
            figis=data.get("figis", []),
            last_scanned=data.get("last_scanned", ""),
        )


@dataclass
class FullSnapshot:
    """The complete persisted state of all scanned exchanges.

    Attributes:
        timestamp: ISO-8601 UTC timestamp of when the snapshot was last saved.
        exchanges: Mapping from exchange code to its ExchangeSnapshot.
        all_composite_figis: Flat set of every composite FIGI seen across all
            exchanges, used as the baseline for incremental diffs.
    """

    timestamp: str = ""
    exchanges: dict[str, ExchangeSnapshot] = field(default_factory=dict)
    all_composite_figis: set[str] = field(default_factory=set)

    def to_dict(self) -> dict:
        """Serialize the full snapshot to a JSON-compatible dictionary.

        The ``all_composite_figis`` set is sorted for deterministic output.

        Returns:
            A dict with keys ``timestamp``, ``exchanges``, and
            ``all_composite_figis``.
        """
        return {
            "timestamp": self.timestamp,
            "exchanges": {
                code: snap.to_dict() for code, snap in self.exchanges.items()
            },
            "all_composite_figis": sorted(self.all_composite_figis),
        }

    @classmethod
    def from_dict(cls, data: dict) -> FullSnapshot:
        """Deserialize a FullSnapshot from a stored dictionary.

        Args:
            data: A dict previously produced by ``to_dict()``.

        Returns:
            A populated FullSnapshot instance with exchange snapshots and
            the composite FIGI set restored.
        """
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
        """Return the current UTC time as an ISO-8601 string.

        Returns:
            A string in the form ``"YYYY-MM-DDTHH:MM:SSZ"``.
        """
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class DiffResult:
    """The result of an incremental diff run.

    Attributes:
        new_records: Full FigiRecord objects for composite FIGIs that did not
            exist in the previous snapshot.
        exchanges_checked: Number of exchanges whose current total was fetched.
        exchanges_changed: Number of exchanges where the total increased or
            which were newly seen.
        total_before: Size of ``all_composite_figis`` at the start of the diff.
        total_after: Size of ``all_composite_figis`` after merging new data.
    """

    new_records: list[FigiRecord] = field(default_factory=list)
    exchanges_checked: int = 0
    exchanges_changed: int = 0
    total_before: int = 0
    total_after: int = 0

    @property
    def new_count(self) -> int:
        """The number of newly discovered composite FIGIs.

        Returns:
            Length of ``new_records``.
        """
        return len(self.new_records)

    def summary(self) -> str:
        """Format a human-readable summary of this diff run.

        Returns:
            A multi-line string with counts for exchanges checked/changed,
            totals before/after, and the number of new composite FIGIs.
        """
        lines = [
            f"Exchanges checked:  {self.exchanges_checked}",
            f"Exchanges changed:  {self.exchanges_changed}",
            f"Total before:       {self.total_before}",
            f"Total after:        {self.total_after}",
            f"New composite FIGIs: {self.new_count}",
        ]
        return "\n".join(lines)
