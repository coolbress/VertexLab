# API Reference

## Factories

::: vertex_forager.api.create_client

::: vertex_forager.api.create_router

## Client & Router

::: vertex_forager.api.BaseClient

::: vertex_forager.api.BaseRouter

## Pipeline Engine

::: vertex_forager.core.VertexForager

## Pipeline Results

::: vertex_forager.core.config.RunResult

::: vertex_forager.core.config.ParseResult

## Flow Control

::: vertex_forager.core.controller.FlowController

::: vertex_forager.core.controller.GradientConcurrencyLimiter

::: vertex_forager.core.controller.GCRARateLimiter

## Configuration

::: vertex_forager.core.config.EngineConfig

::: vertex_forager.core.config.RetryConfig

::: vertex_forager.core.config.RequestSpec

::: vertex_forager.core.config.FetchJob

::: vertex_forager.core.config.FramePacket

::: vertex_forager.core.config.HttpMethod

::: vertex_forager.core.config.RequestAuth

## HTTP

::: vertex_forager.core.http.HttpExecutor

## Writers

::: vertex_forager.writers.create_writer

::: vertex_forager.writers.base.BaseWriter

::: vertex_forager.writers.duckdb.DuckDBWriter

::: vertex_forager.writers.memory.InMemoryBufferWriter

::: vertex_forager.writers.base.WriteResult

## Exceptions

::: vertex_forager.exceptions.VertexForagerError

::: vertex_forager.exceptions.InputError

::: vertex_forager.exceptions.FetchError

::: vertex_forager.exceptions.TransformError

::: vertex_forager.exceptions.WriterError

::: vertex_forager.exceptions.ComputeError

::: vertex_forager.exceptions.ValidationError

::: vertex_forager.exceptions.PrimaryKeyMissingError

::: vertex_forager.exceptions.PrimaryKeyNullError

::: vertex_forager.exceptions.DLQSpoolError

## Utilities

::: vertex_forager.utils.as_dict

::: vertex_forager.utils.validate_tickers

::: vertex_forager.utils.env_bool

::: vertex_forager.utils.env_int

::: vertex_forager.utils.env_float

::: vertex_forager.utils.create_pbar_updater
