from __future__ import annotations

from typing import Union, Mapping, Sequence
from typing import Literal, TypeAlias
from typing_extensions import TypedDict, Required, NotRequired

# JSON‐like value used in RequestSpec.params and other payloads (non-recursive for Pydantic)
JSONValue = Union[str, int, float, bool, None, Mapping[str, object], Sequence[object]]

class PaginationParams(TypedDict):
    cursor_param: Required[str]
    meta_key: Required[str]
    max_pages: NotRequired[int]

class JobContext(TypedDict, total=False):
    pagination: PaginationParams
    dataset: str
    symbol: str

class SymbolContext(TypedDict, total=False):
    dataset: Required[str]
    symbol: Required[str]

class PaginationJobContext(TypedDict, total=False):
    pagination: Required[PaginationParams]
    dataset: NotRequired[str]

class PerSymbolJobContext(TypedDict, total=False):
    dataset: Required[str]
    symbol: Required[str]
    pagination: NotRequired[PaginationParams]

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
    "fast_info",
    "dividends",
    "splits",
    "recommendations",
    "actions",
]
