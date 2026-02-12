from __future__ import annotations

from abc import ABC
from contextlib import asynccontextmanager
from typing import Any, Callable

import httpx

from vertex_forager.core.http import HttpExecutor, default_async_client
from vertex_forager.core.config import EngineConfig, RunResult
from vertex_forager.core.pipeline import VertexForager
from vertex_forager.core.controller import FlowController
from vertex_forager.routers.base import BaseRouter
from vertex_forager.schema.mapper import SchemaMapper
from vertex_forager.writers.base import BaseWriter


class BaseClient(ABC):
    """
    Vendor-agnostic base client abstraction for the Vertex Forager pipeline.

    This class serves as the foundation for all provider-specific clients (e.g., SharadarClient).
    It encapsulates the core infrastructure required to execute data collection tasks independently
    of the underlying data source.

    Key Responsibilities:
    1. **Session Management**: Manages the lifecycle of the underlying HTTP client (`httpx.AsyncClient`)
       via async context managers, ensuring efficient connection pooling and resource cleanup.
    2. **Pipeline Orchestration**: Instantiates and executes the `VertexForager` pipeline, wiring together
       components like Routers, Writers, and Mappers.
    3. **Flow Control**: Initializes the `FlowController` to enforce global rate limits and concurrency
       policies across all pipeline operations.
    4. **Configuration**: Centralizes engine configuration (`EngineConfig`) handling.

    Design Principles:
    - **Provider-Agnostic**: Contains NO vendor-specific logic. All vendor details must be injected
      via Routers and Mappers.
    - **Composition over Inheritance**: While this is a base class, it primarily delegates work to
      composed components (FlowController, VertexForager) rather than relying on deep inheritance chains.

    Usage:
        Subclasses must implement specific methods (e.g., `get_price_data`) that define *what* to fetch,
        delegating the *how* to `self._run()`.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        rate_limit: int,
        **kwargs: Any,
    ) -> None:
        """Initialize the base client infrastructure.

        Args:
            api_key: API key for the provider (optional, depends on provider).
            rate_limit: Maximum requests per minute (RPM) allowed for this client.
            **kwargs: Additional configuration parameters passed directly to `EngineConfig`.
        """
        self.api_key = api_key

        # Enforce rate_limit into EngineConfig
        config_params = kwargs.copy()
        config_params["requests_per_minute"] = rate_limit
        
        self._config = EngineConfig(**config_params)
        
        # Initialize FlowController for global rate limiting
        self.controller = FlowController(
            requests_per_minute=self._config.requests_per_minute,
            concurrency_limit=self._config.fetch_concurrency,
        )
        self.last_run: RunResult | None = None
        self._client: httpx.AsyncClient | None = None

    async def aclose(self) -> None:
        """Asynchronously close the underlying HTTP client to release resources."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "BaseClient":
        """Async context manager entry.

        Initializes the shared HTTP client.
        """
        if self._client is None:
            self._client = default_async_client()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Async context manager exit.

        Ensures the HTTP client is closed.
        """
        await self.aclose()

    async def _run(
        self,
        *,
        router: BaseRouter,
        dataset: str,
        symbols: list[str] | None,
        writer: BaseWriter,
        mapper: SchemaMapper,
        on_progress: Callable[..., None] | None = None,
        **kwargs: object,
    ) -> RunResult:
        """
        Run the VertexForager pipeline for the given router, dataset, and symbols.

        Args:
            router: Data router to fetch data from.
            dataset: Dataset name (e.g., "price_bars").
            symbols: List of symbols to fetch data for. If None, fetch all symbols.
            writer: Data writer to persist the processed data.
            mapper: Schema mapper to transform connector data to sink schema.
            on_progress: Optional callback function called on each completed request.
            **kwargs: Additional arguments passed to the pipeline run method.

        Returns:
            RunResult: Summary of the pipeline run, including success/failure status.
        """
        async with self._http_client() as http_client:
            http = HttpExecutor(client=http_client)
            pipeline = VertexForager(
                router=router,
                http=http,
                writer=writer,
                mapper=mapper,
                config=self._config,
                controller=self.controller,
            )
            
            run_kwargs = kwargs.copy()
            
            return await pipeline.run(
                dataset=dataset, 
                symbols=symbols, 
                on_progress=on_progress,
                **run_kwargs
            )

    @asynccontextmanager
    async def _http_client(self) -> httpx.AsyncClient:
        """Manage the HTTP client lifecycle.

        Yields the existing client if available, or creates a temporary one
        for the duration of the context.
        """
        if self._client is not None:
            yield self._client
            return

        client = default_async_client()
        try:
            yield client
        finally:
            await client.aclose()
