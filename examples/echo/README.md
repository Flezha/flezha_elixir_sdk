# Echo: the Elixir hello world

The Elixir hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `mix.exs` names one released version of `flezha_sdk`, the
way your own project would.

It is a runner of its own, a separate BEAM node that connects to a controller
over its socket. Code running inside the controller's own node is the other way
to run the SDK, described in
[`docs/internal/sdk/elixir_sdk.md`](../../../../docs/internal/sdk/elixir_sdk.md).

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `lib/echo/tasklet.ex` | the tasklet | the processing stage: prefixes each message |
| `lib/echo/relay.ex` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `lib/echo/application.ex` | the runner | this node: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).
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

On start it warns that it cannot persist a runner id at
`/var/lib/flezha-runner/id` unless that path is writable, and uses a fresh
identity for the boot. Set `FLEZHA_RUNNER_ID_PATH` to a writable file to keep one
identity across restarts.

## Building from a checkout

The package is not on Hex yet. Until it is, point the dependency in `mix.exs` at
the package in your checkout:

```elixir
defp deps do
  [{:flezha_sdk, path: "../.."}]
end
```

A copy of this folder outside the checkout names the package's absolute path
instead. With no release to download it from, the first compile builds the
package's native library with a Rust toolchain, so it takes a few minutes.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is.
