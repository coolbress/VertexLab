from __future__ import annotations

from typing import Any, Literal, TypeAlias

from typing_extensions import NotRequired, Required, TypedDict

# JSONValue: JSON-safe union used in params/payloads
# Allows only primitives, lists, and dicts (validated recursively by RequestSpec._validate_params)
JSONValue = str | int | float | bool | None | dict[str, Any] | list[Any]


class PaginationParams(TypedDict):
    cursor_param: Required[str]
    meta_key: Required[str]
    max_pages: NotRequired[int]


class JobContext(TypedDict, total=False):
    pagination: PaginationParams
    dataset: str
    symbol: str
    trace_id: str
    request_id: int


class SymbolContext(TypedDict, total=False):
    dataset: Required[str]
    symbol: Required[str]


class PaginationJobContext(TypedDict, total=False):
    pagination: Required[PaginationParams]
    dataset: NotRequired[str]
    trace_id: str
    request_id: int


class PerSymbolJobContext(TypedDict, total=False):
    dataset: Required[str]
    symbol: Required[str]
    pagination: NotRequired[PaginationParams]
    trace_id: str
    request_id: int


# Dataset Literals for typing clarity (runtime remains flexible via str fields)
SharadarDataset: TypeAlias = Literal[
    "price",
    "tickers",
    "fundamental",
    "daily",
    "actions",
    "insider",
    "institutional",
    "sp500",
]

YFinanceDataset: TypeAlias = Literal[
    "info",
    "fast_info",
    "price",
    "dividends",
    "splits",
    "actions",
    "financials",
    "income_stmt",
    "balance_sheet",
    "cashflow",
    "earnings",
    "quarterly_financials",
    "quarterly_balance_sheet",
    "quarterly_cashflow",
    "quarterly_earnings",
    "major_holders",
    "institutional_holders",
    "mutualfund_holders",
    "insider_roster_holders",
    "insider_purchases",
    "recommendations",
    "calendar",
    "news",
]


class DLQStatusSpooled(TypedDict):
    status: Literal["spooled"]
    rescued: int
    remaining: int
    path: str
    error: None


class DLQStatusRescuedOnly(TypedDict):
    status: Literal["rescued_only"]
    rescued: int
    remaining: int
    path: None
    error: None


class DLQStatusNoop(TypedDict):
    status: Literal["noop"]
    rescued: int
    remaining: int
    path: None
    error: None


class DLQStatusSpoolFailed(TypedDict):
    status: Literal["spool_failed"]
    rescued: int
    remaining: int
    path: None
    error: Exception


class DLQStatusDisabled(TypedDict):
    status: Literal["disabled"]
    rescued: int
    remaining: int
    path: None
    error: None


DLQStatus = DLQStatusSpooled | DLQStatusRescuedOnly | DLQStatusNoop | DLQStatusSpoolFailed | DLQStatusDisabled
