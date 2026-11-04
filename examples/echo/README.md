# Echo: the Elixir hello world

The Elixir hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `mix.exs` names one released version of `flezha_sdk`, the
way your own project would.

It is a runner of its own, a separate BEAM node that connects to a controller
over its socket. Code running inside the controller's own node is the other way
to run the SDK, described in
the [Elixir SDK guide](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/elixir_sdk.md).

Three files, one for each of the [building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md):

| File | Part | What it does |
|---|---|---|
| `lib/echo/tasklet.ex` | the tasklet | the processing stage: prefixes each message |
| `lib/echo/relay.ex` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `lib/echo/application.ex` | the runner | this node: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([Up and running](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md)).
From this folder:

```sh
mix deps.get
mix run --no-halt
```

Then, from another terminal:

```sh
flz relay start echo --id my_echo
flz relay send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It runs until the node stops: Ctrl-C twice, or a SIGTERM. The namespace is the
first argument and defaults to `default`: `mix run --no-halt -- my_app`.

Unless `/var/lib/flezha-runner/id` is writable, it uses a fresh runner identity
for each boot. Set `FLEZHA_RUNNER_ID_PATH` to a writable file to keep one
identity across restarts.

## Building from a checkout

To run the example against the package in your checkout instead of the released
one, point the dependency in `mix.exs` at it:

```elixir
defp deps do
  [{:flezha_sdk, path: "../.."}]
end
```

A copy of this folder outside the checkout names the package's absolute path
instead. A checkout has no checksum file to download the package's native library
against, so the first compile builds it with a Rust toolchain and takes a few minutes.

## Where to go next

[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what each of the three parts is and why the split is the way it
is.
