# Agentdesk: the Elixir project of the agentdesk tutorial

The chat sessions the [agentdesk tutorial](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/agentdesk.md)
builds, as a standalone project. It depends on `flezha_sdk` and Jason alone, the way your
own project would: `mix.exs` names its released version
([Building from a checkout](#building-from-a-checkout) runs it against this checkout instead).

Every conversation is a session of its own: an instance of the steps relay `agent`,
which runs one turn per message and keeps the conversation as its state. A turn is
answered by a demo model that lives in this project. It is a stand-in, not a model:
it answers every message with the same sentence, one token at a time, and reports how
many tokens it used. `lib/agentdesk/demo_model.ex` is where a real model's client would
be called.

In front of the sessions is `gateway`, a flow relay that carries the HTTP
plugin: a POST is a message for a user's session, answered 202 at once, a GET streams
the session's answer as it is written, and each user has a daily token budget.

| File | What is in it |
|---|---|
| `lib/agentdesk/application.ex` | the runner: this node joins the `agents` namespace and registers the `agent` and `gateway` factories |
| `lib/agentdesk/agent_relay.ex` | a session, a steps relay: one turn per message, and a failed turn fails alone |
| `lib/agentdesk/reply.ex` | answers a message with the demo model, sends the answer as it grows, and keeps the conversation |
| `lib/agentdesk/demo_model.ex` | the demo model, and the `Answer` it gives |
| `lib/agentdesk/gateway_relay.ex` | the HTTP side, a flow relay: it carries the HTTP plugin, answers every request, and calls the session a message is for |
| `lib/agentdesk/request.ex` | one HTTP request, as the plugin hands it to the gateway, and the two routes the gateway knows |
| `lib/agentdesk/watch.ex` | answers a request for a session's events with its stream, and any other request with 404 |
| `lib/agentdesk/budget.ex` | keeps the tokens each user's answers used today, and admits a message while they are under the day's budget |
| `lib/agentdesk/admit.ex` | answers a message 202 when it goes on to the session, and 429 when its user is over the budget |
| `Dockerfile` | the runner's image: the release `mix release` builds, on Debian 13 |
| `controller/Dockerfile` | the controller's image: Flezha's own, with the plugin packages in `controller/plugins/` |
| `compose.yaml` | one controller and one runner, in Docker Compose, with the gateway's HTTP port published on this machine only |
| `cluster.yaml` | a second controller with its runner, and a PostgreSQL database both controllers copy their state to; read on top of `compose.yaml` |
| `k8s/agentdesk.yaml` | the same images on Kubernetes: one pod with the controller and the runner, a volume, and a service |

Every tasklet is a module that writes `use Flezha.SDK.Tasklet, name: "..."` and
`run(input, state, ctx)`, as in the tradedesk. What a tasklet remembers is its `state`
block, and each change to it is one `defchange`. The values the tasklets hand each other
are plain maps; an answer is an `Answer` struct, and the gateway's input is a `Request`,
which the entry channel decodes the plugin's envelope into.

## Run it

You need Elixir 1.17 or later with Erlang/OTP, and Docker with Docker Compose.

```sh
mix deps.get
MIX_ENV=prod mix release
docker compose up -d --build --wait
alias flz='docker compose exec controller bin/flz'
```

The runner's image copies the release as Mix left it, with the Erlang runtime it
carries, so a release built against a C library newer than Debian 13's may not start in
it. The alias runs `flz` inside the controller's container. The runner joins `agents` on
its own; the controller lists it:

```sh
flz runner list --namespace agents
```

## Talk to a session

Start a session, then send it a turn and wait for its answer:

```sh
flz relay start agent --id anna --namespace agents
flz relay call anna --namespace agents --text 'Hello'
```

```
{"text":"Message 1: you said \"Hello\". I am a demo model.","tokens":10}
```

## Talk to it over HTTP

With the HTTP plugin's package in `controller/plugins/` ([Add your own plugin](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/add_your_own_plugin.md) builds it), start
the gateway, enable the plugin on it, and send a message from this machine:

```sh
flz relay start gateway --id gateway --namespace agents
flz plugin enable web http_server --relay_id=gateway --namespace agents --hook=on_terminate --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http
curl -s -d 'Hello from the web' localhost:8080/sessions/anna/messages
curl -sN localhost:8080/sessions/anna/events
```

The example runs with authorization off, and the plugin authenticates nobody: it is a
demonstration, not a setup to expose.

`docker compose down -v` stops both containers and removes the sessions with their volumes.

## Run it as a cluster, or on Kubernetes

With `RELEASE_COOKIE` and `DURABILITY_PASSWORD` set in the shell,
`COMPOSE_FILE=compose.yaml:cluster.yaml docker compose up -d --build --wait` adds a
second controller; [Run a cluster](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/run_a_cluster.md)
walks it. [Run on Kubernetes](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/run_on_kubernetes.md)
builds the images and deploys `k8s/agentdesk.yaml` on k3s.

## Building from a checkout

Docker pulls the controller's image `flezha/flezha:0.1.0` from Docker Hub; the
commands below build it from the checkout instead.
To run the project against
the package in your checkout instead of the released one, point the dependency in
`mix.exs` at it:

```elixir
defp deps do
  [{:flezha_sdk, path: "../.."}, {:jason, "~> 1.4"}]
end
```

A copy of this folder outside the checkout names the package's absolute path instead.
A checkout has no checksum file to download the package's socket client against, so the
first compile builds it with a Rust toolchain and takes a few minutes. From the repository root, build
the controller's image:

```sh
docker build -f deploy/docker/prod/Dockerfile.controller -t flezha/flezha:0.1.0 .
```

## Where to go next

The tradedesk in [`../tradedesk`](../tradedesk/README.md) is the tradedesk tutorial's project, and
the [calls contract](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/calls.md) is what `flz relay call`
follows.
