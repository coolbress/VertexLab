from __future__ import annotations

from typing import Union, Any
from typing import Literal, TypeAlias
from typing_extensions import TypedDict, Required, NotRequired

# JSONValue: JSON-safe union used in params/payloads
# Allows only primitives, lists, and dicts (validated recursively by RequestSpec._validate_params)
JSONValue = Union[str, int, float, bool, None, dict[str, Any], list[Any]]

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
    "fundamental",
    "daily",
    "tickers",
    "actions",
    "insider",
    "institutional",
    "sp500",
]

YFinanceDataset: TypeAlias = Literal[
    "price",
    "financials",
    "quarterly_financials",
    "balance_sheet",
    "quarterly_balance_sheet",
    "cashflow",
    "quarterly_cashflow",
    "income_stmt",
    "earnings",
    "quarterly_earnings",
    "major_holders",
    "institutional_holders",
    "mutualfund_holders",
    "insider_roster_holders",
    "insider_purchases",
    "calendar",
    "news",
    "info",
    "dividends",
    "splits",
    "recommendations",
]
