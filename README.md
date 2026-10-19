# flezha_sdk

The Elixir SDK for Flezha. You write tasklets that handle one message at a
time and relays that declare how those tasklets are connected; a Flezha
controller places them, keeps their state durable, retries failures and moves
work when a host goes away.

**Support tier:**
[tier 1](https://github.com/flezha/flezha/blob/main/docs/public/publishing/maturity.md#sdk-elixir),
run by the maintainer in a live deployment, passes the full conformance run in CI and is
permanently supported.

## Install

```elixir
def deps do
  [{:flezha_sdk, "~> 0.1.0"}]
end
```

`flezha_sdk` 0.1.0 is not on Hex yet. Until it is, depend on it by path from a
checkout as [`examples/echo`](examples/echo/README.md#building-from-a-checkout)
shows.

## Supported Elixir versions

Elixir 1.17 or newer.

## Running

The SDK runs on `flz_core`, the runtime every Flezha SDK shares, through a
native library this package carries. Your application starts
`Flezha.SDK.child_spec/1` under its supervisor, and the runner connects to a
controller over a Unix domain socket (the default) or QUIC, wherever it runs:
its own BEAM, or the controller's. `examples/echo` is the smallest complete
one.

A controller that enforces authorization asks every runner for a credential:
issue one with `flz token issue runner@<ns> <ns> runner` and pass it as
`namespace_secret:`, or set `FLEZHA_NAMESPACE_SECRET`. Connection settings are
read from the OS environment, then from `config :flezha_sdk, ...`.


## Declaring a relay

One macro declares every relay. Its settings say how it runs and where its
work is kept, and each is optional:

```elixir
defmodule Checkout do
  use Flezha.SDK.Relay,
    factory_id: "checkout",
    run: {:steps, each: :per_input, on_failure: :keep_progress},
    durable: {:local, sleep_after: 60_000}

  def channels(c), do: c |> input("orders") |> into(Reserve) |> into(Charge)
end
```

`run` is `:flow` (the default, with `exactly: :effectively_once` for the
effectively-once tier) or `:steps`; `durable` is `false`, `:local` (the
default; a flow instance never sleeps, a steps instance after six hours idle)
or `:remote`. A setting written under
the wrong parent is refused when the class is registered, as
`topology.unknown_field`. `Flezha.SDK.Relay` documents the tree.

## The native library

The runtime is a native library, `Flezha.SDK.Native`, built over `flz_core`. It
is precompiled for Linux (x86_64, aarch64), macOS (x86_64, arm64) and Windows
(x86_64), downloaded when the package compiles and checked against the checksum
file the package carries. On any other host, or with `FLEZHA_SDK_BUILD_NIF=1`,
it is built from source, which needs a Rust toolchain. A checkout carries no
checksum file, so a build from one always compiles the library from source. A
node that never runs a runner sets `config :flezha_sdk, socket_nif: false` and
compiles no library.

## Local mode, testing and the relay API

- **Local mode**: `Flezha.SDK.Local.start/2` runs a relay's declared graph in
  your own process, with no controller. See
  [Local mode](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#local-mode).
- **Testing**: the SDK ships no tasklet test harness yet. See
  [Check a tasklet on its own](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#check-a-tasklet-on-its-own)
  for the SDKs that do.
- **The relay API**: a relay's entry channels, answering a caller, `call/3` to
  another relay, and `Flezha.SDK.send/5` and `Flezha.SDK.call/4` from
  application code. See
  [Sending and calling](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/sending_and_calling.md).

## Configuration

Every knob is read from its environment variable first, then from this
application's env, then its default: `Flezha.SDK.Knob` lists them.

## Developing this package

`scripts/ci.sh --core <dir|url>` runs the unit suite, local mode and a smoke run
of `test_app/` against the core release's fake controller. Everything else a
runner does with a real controller is proven in the core repository, which
carries this one as a submodule at `sdk/elixir`.

Apache-2.0.
