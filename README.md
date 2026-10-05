# flezha_sdk

The Elixir SDK for Flezha. You write tasklets that handle one message at a
time and relays that declare how those tasklets are connected; a Flezha
controller places them, keeps their state durable, retries failures and moves
work when a host goes away.

**Support tier:**
[tier 1](https://github.com/flezha/flezha/blob/main/docs/publishing/maturity.md#sdk-elixir),
run by the maintainer in a live deployment, passes the full conformance run in CI and is
permanently supported.

```elixir
def deps do
  [{:flezha_sdk, "~> 0.1.0"}]
end
```

Elixir 1.17 or later.

## Two ways to run

- **A runner of its own.** Your application starts `Flezha.SDK.child_spec/1`
  under its supervisor and connects to a controller over a Unix domain socket or
  QUIC. `examples/echo` is the smallest complete one.
- **Inside the controller's node.** The controller registers the in-node
  transport (`Flezha.SDK.Connect`) at boot, and a runner started there with
  `transport: :native` reaches it without a socket.

`Flezha.SDK.Local` runs a relay's declared graph in your own process with no
controller at all, holding state in memory only.

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
the wrong parent does not compile. `Flezha.SDK.Relay` documents the tree.

## Answering a caller, calling a relay

A called relay answers with its outputs: the first output an input causes on an
output answers the call that input came from, and still reaches the output's
plugins and listeners. A channel can also end in `call/3`, which sends each
value to another relay and hands the outcome back on a new entry channel,
`<channel>.response`:

```elixir
def channels(c) do
  turns = c |> input("in") |> into(Turn)
  turns |> key_by(& &1.session) |> call(factory: "agent", deadline_ms: 60_000) |> into(Turn)
end
```

`call/3` takes a target instance (`call(stream, "pricing", deadline_ms: 300)`)
or `factory:` (with `create: false` to reach only a running instance), a
required `deadline_ms:`, and optional `channel:`, `answer_channel:` (the
callee's channel whose output answers), `serialize:` and `decode:`.
Each outcome is a `%Flezha.SDK.Response{}`: `request` (the bytes sent),
`result` (`{:ok, answer}` or `{:error, %Flezha.SDK.Error{}}`, a missed deadline
being `call.timeout`), `target` (the callee's instance id) and `created`.

## The socket client

`:uds` and `:quic` dial through a small native library, `Flezha.SDK.Native`. It
is precompiled for Linux (x86_64, aarch64), macOS (x86_64, arm64) and Windows
(x86_64), downloaded when the package compiles and checked against the checksum
file the package carries. On any other host, or with `FLEZHA_SDK_BUILD_NIF=1`,
it is built from source, which needs a Rust toolchain. A node that never dials a
socket sets `config :flezha_sdk, socket_nif: false` and compiles no library.

## Configuration

Every knob is read from its environment variable first, then from this
application's env, then its default: `Flezha.SDK.Knob` lists them.

## Developing this package

`scripts/ci.sh --core <dir|url>` runs the unit suite, local mode and a smoke run
of `test_app/` against the core release's fake controller. Everything else a
runner does with a real controller is proven in the core repository, which
carries this one as a submodule at `sdk/elixir`.

Apache-2.0.
