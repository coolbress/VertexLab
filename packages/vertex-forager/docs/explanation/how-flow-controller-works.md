# How Flow Controller Works

vertex-forager uses one flow controller to combine request pacing and concurrency control.

## Two mechanisms, one control surface

The controller combines:

- GCRA rate limiting for requests-per-minute control
- a gradient-based concurrency limiter for in-flight request control

This lets the pipeline limit both:

- how often new work starts
- how much work is already in progress

## GCRA rate limiting

The rate limiter is based on the Generic Cell Rate Algorithm.

Conceptually, each request is allowed only if its theoretical arrival time does not move too far ahead of the permitted schedule. That gives vertex-forager a stable RPM envelope without relying on sleep loops scattered throughout the code.

In `FlowController`, the GCRA burst allowance is intentionally tied to the concurrency limit rather than to the RPM value itself.

That means adaptive RPM changes update pacing, but they do not redefine the burst ceiling. The burst cap stays aligned with concurrency constraints.

## Gradient concurrency control

The concurrency limiter watches latency and uses a gradient algorithm to decide whether the system is healthy enough to support more parallel work.

If observed RTT grows or errors accumulate, the concurrency window can shrink. If the system recovers, the window can grow again.

## `effective_rpm` and `rpm_ceiling`

`rpm_ceiling` is the configured upper bound.

`effective_rpm` is the current live limit after adaptive downshifts or recovery. During unhealthy periods the controller can reduce `effective_rpm` below the ceiling. During recovery it moves back upward without exceeding the ceiling.

## What a downshift event means

A downshift means recent feedback crossed the configured error-rate threshold. The controller reduces pressure by lowering the effective request rate instead of waiting for more failures to accumulate.

Operationally, a downshift usually follows:

- repeated retryable HTTP failures
- throttle-like provider responses
- other transient transport problems

## How recovery happens

The controller keeps a healthy window. If feedback stays below the error threshold long enough, recovery logic increases the effective rate again by the configured recovery factor until the RPM ceiling is reached.

## Why this matters to users

Retry alone reacts after failure. The flow controller changes the shape of later traffic so the pipeline can stabilize instead of repeatedly hammering a degrading provider.

## Related pages

- [How retry and backoff work](how-retry-and-backoff-work.md)
- [How pipeline orchestrates](how-pipeline-orchestrates.md)
- [Configuration](../reference/config.md)
