# Tradedesk: the Elixir project for guides Part 2

The toy trader the [Part 2 guides](../../../../docs/guides/guides.md) build, as a
standalone project. It depends on `flezha_sdk` and Jason alone, the way your own
project would; until `flezha_sdk` is released, `mix.exs` names it by a path into
this checkout ([Building from a checkout](#building-from-a-checkout)).

It holds the desk's two operators and the runner that serves them, a separate
BEAM node that connects to a controller over its socket. The guides write them
one tasklet at a time.

| File | What is in it |
|---|---|
| `lib/tradedesk/application.ex` | the runner: this node joins the `trading` namespace and registers both factories |
| `lib/tradedesk/trade_watcher.ex` | the ETL stream, an unbounded operator: the exchange's trade messages in, a cell per symbol, buy signals out |
| `lib/tradedesk/track_prices.ex` | keeps each symbol's last ten prices, and compares a new price with their average |
| `lib/tradedesk/buy_above_average.ex` | starts an order when a price is 2 % or more above its average |
| `lib/tradedesk/order_executor.ex` | the workflow, a bounded operator: two steps in order, then two more on the same placed order. The order an instance is started with is its `OrderTicket` |
| `lib/tradedesk/reserve_funds.ex` | step one: puts the money aside, and releases it again if the order is undone |
| `lib/tradedesk/place_order.ex` | step two: places the paper order, and cancels it if the order is undone |
| `lib/tradedesk/confirm_fill.ex` | waits ten seconds for the exchange, confirms the fill and ends the order with its result, and refuses an order above the exchange's limit |
| `lib/tradedesk/record_order.ex` | books the placed order, and has nothing to undo |

Every tasklet is a module that writes `use Flezha.SDK.Tasklet, name: "..."`
and `run(input, state, ctx)`, which answers `{:ok, state}`. What a tasklet
remembers is its `state` block, and each change to it is one `defchange`. The
values the tasklets hand each other are plain maps, and the order is an
`OrderTicket` struct.

## Run it

You need a controller running, the `trading` namespace created and a key for it
([`docs/guides/tradedesk/authorization.md`](../../../../docs/guides/tradedesk/authorization.md)
and
[`namespaces.md`](../../../../docs/guides/tradedesk/namespaces.md)). From this
folder:

```sh
mix deps.get
FLEZHA_NAMESPACE_SECRET=<trading key> mix run --no-halt
```

It runs until the node stops: Ctrl-C twice, or a SIGTERM. The namespace is the
first argument and defaults to `trading`: `mix run --no-halt -- my_desk`.

From another terminal, the controller lists it:

```sh
flz runner list --namespace trading
```

## Start an order by hand

One instance of `order_executor` is one order. `--text` carries the order on the
`order` channel:

```sh
flz operator start order_executor --id order_by_hand --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
```

About ten seconds later, once `confirm_fill` has its answer from the exchange,
the runner's terminal shows the order being booked, and the instance ends. The
ids come from `stable_id/1`, so yours differ:

```
[book] EUR/USD 10 as ord-663e51f721fc6553d09b24a47c4e1383
```

The booking comes after the wait: an Elixir order runs `confirm_fill` and
`record_order` one after the other, so the book waits for the fill.
`flz operator info` shows how the order ended:

```sh
flz operator info order_by_hand --namespace trading
```

```
...
Operator: order_by_hand
  Kind: bounded
  Phase: finished
  Status: ok
  Finished At (unix s): 1790611042
```

## Watch it undo itself

The paper exchange fills at most 100 at a time, so `confirm_fill` refuses a
larger order. Start one the same way, with a quantity above the limit:

```sh
flz operator start order_executor --id order_too_big --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 500}' --channel order
```

`confirm_fill` refuses it after ten seconds, is tried three times, ten seconds
each, and then the order ends in failure. The book is written once the last
try is spent, and then every step that had finished is undone, in reverse: the
order is cancelled, then the money is released. `record_order` has no undo, so
the booking stays. The tries are logged at the `debug` level, which a
`mix run` in the `dev` environment shows:

```
17:57:34.082 [debug] [Flezha.SDK] order_too_big/node/single attempt 1/3 failed: the exchange fills at most 100 a time, and this is 500; retrying

17:57:44.150 [debug] [Flezha.SDK] order_too_big/node/single attempt 2/3 failed: the exchange fills at most 100 a time, and this is 500; retrying
[book] EUR/USD 500 as ord-e015c2df9402f4625d9cef4def51f243
[undo] cancelled ord-e015c2df9402f4625d9cef4def51f243

17:57:54.287 [debug] [Flezha.SDK] order_too_big compensating task 1
[undo] released res-e0d09a3a6a33661df98dd0a4768bb45a
```

```sh
flz operator info order_too_big --namespace trading
```

```
...
Operator: order_too_big
  Kind: bounded
  Phase: finished
  Status: error
  Error: confirm_fill: the exchange fills at most 100 a time, and this is 500
  Finished At (unix s): 1790611074
```

## Building from a checkout

The package is not on Hex yet. Until it is, the dependency in `mix.exs` points
at the package in your checkout:

```elixir
defp deps do
  [{:flezha_sdk, path: "../.."}, {:jason, "~> 1.4"}]
end
```

A copy of this folder outside the checkout names the package's absolute path
instead. With no release to download it from, the first compile builds the
package's socket client with a Rust toolchain, so it takes a few minutes.

## Where to go next

The echo in [`../echo`](../echo/README.md) is the Part 1 project, and
[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what a tasklet, an operator and a runner each are.
