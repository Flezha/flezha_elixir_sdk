# Starter: a relay that counts visits per page

A small, complete Flezha project in Elixir, to grow into your own: one relay,
one tasklet that keeps state, and a test. `mix.exs` names the SDK by its
released version, the way your own project does. It is a runner node of its
own, connecting to a controller over its socket.

| File | What it is |
|---|---|
| `lib/starter/count.ex` | the tasklet `count`: keeps a page's total in state, changes it only through the `counted` change, and answers the new count |
| `lib/starter/relay.ex` | the relay `starter`: the entry channel `hit`, described and keyed by page, into `count`, out on `counts` |
| `lib/starter/application.ex` | the runner: serves the relay until the node stops |
| `test/starter_test.exs` | the test: four hits through the relay in local mode, with no controller |

## Run it

You need a controller running and reachable, as
[Start the controller](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md#start-the-controller)
starts one. From this folder, fetch the SDK, run the test and start the runner:

```sh
mix deps.get
mix test
mix run --no-halt
```

Then, from another terminal, start the relay and call it:

```sh
flz relay start starter --id starter
flz relay call starter --channel hit --arg page=home
```

```
{"page":"home","total":1}
```

Each call for the same page answers the next total. `flz` is the release's
`bin/flz`. The runner keeps running until the node stops: Ctrl-C twice, or a
SIGTERM. The namespace is its first argument and defaults to `default`:
`mix run --no-halt -- my_app`.

## Make it yours

Nothing inside the files is renamed for you. Change the `app` and the module
prefix `Starter`. `starter` is the name `flz relay start` takes. `count` is the
name the tasklet's state is saved under, so choose it before the first run.

## Building from a checkout

To run the project against a Flezha checkout instead of the released package,
point the dependency in `mix.exs` at it, with its absolute path:

```elixir
deps: [{:flezha_sdk, path: "/path/to/flezha/sdk/elixir"}],
```

A checkout has no checksum file to download the SDK's socket client against, so
the first compile builds it with a Rust toolchain and takes a few minutes.
