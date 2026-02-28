from __future__ import annotations

import warnings
from typing import Callable, Any, TypeVar, Union

from vertex_forager.clients.base import HttpExecutor
from vertex_forager.core.config import RunResult
from vertex_forager.routers.base import BaseRouter
from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.writers.base import BaseWriter
from vertex_forager.core.types import JSONValue, SharadarDataset, YFinanceDataset

T = TypeVar("T", bound=Union[SharadarDataset, YFinanceDataset, str])

async def run_pipeline_for(
    *,
    client: Any,
    router: BaseRouter,
    dataset: T,
    symbols: list[str] | None,
    writer: BaseWriter,
    mapper: SchemaMapper,
    on_progress: Callable[..., None] | None = None,
    **kwargs: JSONValue,
) -> RunResult:
    """Execute the VertexForager pipeline using the provided client context.
    
    This function isolates pipeline orchestration from the client class to
    satisfy DIP and improve testability.
    """
    # Import VertexForager via base to allow test patching on vertex_forager.clients.base.VertexForager
    from vertex_forager.clients.base import VertexForager
    async with client._http_client():
        http = HttpExecutor(client=client)
        pipeline = VertexForager(
            router=router,
            http=http,
            writer=writer,
            mapper=mapper,
            config=client._config,
            controller=client.controller,
        )
        from vertex_forager.clients.validation import filter_reserved_kwargs
        reserved = {"router", "dataset", "symbols", "writer", "mapper", "on_progress"}
        run_kwargs = filter_reserved_kwargs(kwargs, reserved)
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=FutureWarning,
                module=r"pandas(\.|$)",
            )
            warnings.filterwarnings(
                "ignore",
                category=FutureWarning,
                module=r"yfinance(\.|$)",
            )
            client.last_run = await pipeline.run(
                dataset=dataset, symbols=symbols, on_progress=on_progress, **run_kwargs
            )
        return client.last_run
