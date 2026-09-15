# Echo: the Elixir hello world

The Elixir hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `mix.exs` names one released version of `flezha`, the way
your own project would.

It is a runner of its own, a separate BEAM node that connects to a controller
over its socket. The `flezha` package is the controller and the SDK in one, so
`config/config.exs` switches the controller half off. Code running inside the
controller's own node is the other way to run the SDK, described in
[`docs/sdk/elixir_sdk.md`](../../../docs/sdk/elixir_sdk.md).

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `lib/echo/procedure.ex` | the procedure | the processing stage: prefixes each message |
| `lib/echo/operative.ex` | the operative | the graph: `in` feeds the procedure, output lands on `out` |
| `lib/echo/application.ex` | the runner | this node: offers the operative and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../docs/guides/controller_deployment.md)).
From this folder:

```sh
mix deps.get
mix run --no-halt
```

Then, from another terminal:

```sh
flz operative start my_echo echo
flz operative send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It runs until the node stops: Ctrl-C twice, or a SIGTERM. The namespace is the
first argument and defaults to `default`: `mix run --no-halt -- my_app`.

On start it warns that it cannot persist a runner id at
`/var/lib/flezha-runner/id` unless that path is writable, and uses a fresh
identity for the boot. Set `FLEZHA_RUNNER_ID_PATH` to a writable file to keep one
identity across restarts.

## Building from a checkout

The package is not on Hex yet. Until it is, point the dependency in `mix.exs` at
your checkout:

```elixir
defp deps do
  [{:flezha, path: "../../.."}]
end
```

A copy of this folder outside the checkout names the checkout's absolute path
instead. The first `mix deps.get` then fetches the controller's own dependencies,
and the first compile builds its native library with a Rust toolchain, so it
takes a few minutes.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is.
