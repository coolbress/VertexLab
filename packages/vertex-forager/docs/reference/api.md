# API Reference

## Factories

Start here when you want the default entry points for creating clients and routers without importing provider-specific classes directly.

::: vertex_forager.api.create_client

::: vertex_forager.api.create_router

## Client & Router

These abstractions define the public provider-facing contract that concrete clients and routers implement.

::: vertex_forager.api.BaseClient

::: vertex_forager.api.BaseRouter

## Pipeline Engine

The pipeline engine coordinates fetch, parse, normalize, and write stages for a run.

::: vertex_forager.core.VertexForager

## Pipeline Results

These result models summarize parsed packets, table counts, and run-level outcomes.

::: vertex_forager.core.config.RunResult

::: vertex_forager.core.config.ParseResult

## Flow Control

Use these components when tuning throughput, concurrency, and request pacing.

::: vertex_forager.core.controller.FlowController

::: vertex_forager.core.controller.GradientConcurrencyLimiter

::: vertex_forager.core.controller.GCRARateLimiter

## Configuration

These types describe the request, retry, writer, and execution settings used throughout the pipeline.

::: vertex_forager.core.config.RetryConfig

::: vertex_forager.core.config.DownshiftConfig

::: vertex_forager.core.config.HTTPConfig

::: vertex_forager.core.config.AdvancedConfig

::: vertex_forager.core.config.RequestSpec

::: vertex_forager.core.config.FetchJob

::: vertex_forager.core.config.FramePacket

::: vertex_forager.core.config.HttpMethod

::: vertex_forager.core.config.RequestAuth

## HTTP

The HTTP executor handles transport concerns for provider requests and library fetch dispatch.

::: vertex_forager.core.http.HttpExecutor

## Retry

Retry helpers centralize backoff policy and retry execution behavior.

::: vertex_forager.core.retry.create_retry_controller

::: vertex_forager.core.retry.RetryExecutor

## Writers

Writer APIs control how normalized frames are persisted or collected in memory.

::: vertex_forager.writers.create_writer

::: vertex_forager.writers.base.BaseWriter

::: vertex_forager.writers.duckdb.DuckDBWriter

::: vertex_forager.writers.memory.InMemoryBufferWriter

::: vertex_forager.writers.base.WriteResult

## Lifecycle

Lifecycle helpers create and finalize the shared state for a pipeline run.

::: vertex_forager.core.lifecycle.initialize_run_state

::: vertex_forager.core.lifecycle.create_run_queues

::: vertex_forager.core.lifecycle.create_run_result

::: vertex_forager.core.lifecycle.RunFinalizer

## Exceptions

These are the main public exception types you should catch around client usage and integration code.

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

## Core Errors

Core errors cover pipeline-level failures that sit below the top-level public exceptions.

::: vertex_forager.core.errors.RunError

## Utilities

Utility helpers provide small convenience functions for env parsing, ticker validation, and progress updates.

::: vertex_forager.utils.as_dict

::: vertex_forager.utils.validate_tickers

::: vertex_forager.utils.env_bool

::: vertex_forager.utils.env_int

::: vertex_forager.utils.env_float

::: vertex_forager.utils.create_pbar_updater
