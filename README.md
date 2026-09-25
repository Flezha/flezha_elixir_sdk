# flezha_sdk

The Elixir SDK for Flezha. You write tasklets that handle one message at a
time and operators that declare how those tasklets are connected; a Flezha
controller places them, keeps their state durable, retries failures and moves
work when a host goes away.

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

`Flezha.SDK.Local` runs an operator's declared graph in your own process with no
controller at all, holding state in memory only.

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
