# How Retry and Backoff Work

vertex-forager treats retry as part of the transport and provider control plane rather than as ad hoc exception handling around each request.

## What makes an error retryable

Retryability is decided from two broad signals:

- HTTP status codes in the configured `retry_status_codes`
- exception types and response conditions that the retry controller recognizes as transient

The retry layer also honors `Retry-After` when a provider sends it, so server guidance can override locally generated backoff timing.

## Backoff model

The retry controller builds an exponential backoff sequence with a lower bound from `base_backoff_s` and an upper cap from `max_backoff_s`.

The two public jitter modes change how that delay is randomized:

- `full_jitter`
  - spreads retries across the full interval
  - is better when many similar requests may fail together
- `equal`
  - keeps part of the delay deterministic and randomizes the remainder
  - is better when you want less variance between attempts

## How `max_attempts` interacts with timing

`max_attempts` is the total number of tries, not the number of retries after the first call.

That means the real retry envelope is determined by:

- the first attempt
- each later attempt up to `max_attempts`
- the capped exponential delay before each retry

Increasing `max_attempts` without reviewing `base_backoff_s` and `max_backoff_s` changes both resilience and total wall-clock time.

## Why retry is not the whole story

Retry operates inside a larger feedback loop:

- the `FlowController` limits how fast requests are issued
- retry reports success and failure outcomes back into that controller
- adaptive throttle can downshift the effective RPM after repeated errors

So retry is not only “try again later.” It also influences future pacing decisions.

## What does not retry freely

Retry is intentionally conservative for non-idempotent work. Requests that should not be replayed automatically are forced into a single-attempt policy to avoid duplicate side effects.

## Operational consequence

When a provider becomes unstable, vertex-forager does not only wait longer between attempts. It also feeds those outcomes into the controller so the pipeline can reduce pressure instead of brute-forcing repeated failure.

## Related pages

- [How flow controller works](how-flow-controller-works.md)
- [Configure retry](../how-to/configure-retry.md)
- [Configuration](../reference/config.md)
