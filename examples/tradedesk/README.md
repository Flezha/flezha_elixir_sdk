# Tradedesk: the Elixir project of the tradedesk tutorial

The toy trader the [tradedesk tutorial](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/tradedesk.md) builds, as a
standalone project. It depends on `flezha_sdk` and Jason alone, the way your own
project would: `mix.exs` names its released version
([Building from a checkout](#building-from-a-checkout) runs it against this checkout instead).

It holds the desk's two relays and the runner that serves them, a separate
BEAM node that connects to a controller over its socket. The tutorial writes them
one tasklet at a time.

| File | What is in it |
|---|---|
| `lib/tradedesk/application.ex` | the runner: this node joins the `trading` namespace and registers both factories |
| `lib/tradedesk/trade_watcher.ex` | the ETL stream, a flow relay: the exchange's trade messages in, a cell per symbol, buy signals out |
| `lib/tradedesk/track_prices.ex` | keeps each symbol's last ten prices, and compares a new price with their average |
| `lib/tradedesk/buy_above_average.ex` | starts an order when a price is 2 % or more above its average |
| `lib/tradedesk/order_executor.ex` | the workflow, a steps relay: two steps in order, then two more on the same placed order. The order an instance is started with is its `OrderTicket` |
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
([Open the desk](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/open_the_desk.md) does both). From this
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
flz relay start order_executor --id order_by_hand --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
```

The runner's terminal shows the order being booked at once, while
`confirm_fill` waits. The ids come from `stable_id/1`, so yours differ:

```
[book] EUR/USD 10 as ord-663e51f721fc6553d09b24a47c4e1383
```

About ten seconds later, once `confirm_fill` has its answer from the exchange,
the instance ends. `flz relay info` shows how the order ended:

```sh
flz relay info order_by_hand --namespace trading
```

```
...
Relay: order_by_hand
  Run: steps
  Phase: finished
  Status: ok
  Finished At (unix s): 1790611042
```

## Watch it undo itself

The paper exchange fills at most 100 at a time, so `confirm_fill` refuses a
larger order. Start one the same way, with a quantity above the limit:

```sh
flz relay start order_executor --id order_too_big --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 500}' --channel order
```

`confirm_fill` refuses it after ten seconds, is tried three times, ten seconds
each, and then the order ends in failure. The booking prints at once, beside
the tries, and once the last try is spent every step that had finished is
undone, in reverse: the order is cancelled, then the money is released. `record_order` has no undo, so
the booking stays. The runner prints:

```
[book] EUR/USD 136 as ord-8d77f1d5e001f0e1d0ba82e0d52d23c2
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 47 ms): the exchange fills at most 100 a time, and this is 136
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 112 ms): the exchange fills at most 100 a time, and this is 136
[undo] cancelled ord-8d77f1d5e001f0e1d0ba82e0d52d23c2
[undo] released res-0074b24e42ff82a541af69e1e369de53
```

```sh
flz relay info order_too_big --namespace trading
```

```
...
Relay: order_too_big
  Run: steps
  Phase: finished
  Status: error
  Error: confirm_fill: the exchange fills at most 100 a time, and this is 500
  Finished At (unix s): 1790611074
```

## Building from a checkout

To run the project against the package in your checkout instead of the released
one, point the dependency in `mix.exs` at it:

```elixir
defp deps do
  [{:flezha_sdk, path: "../.."}, {:jason, "~> 1.4"}]
end
```

A copy of this folder outside the checkout names the package's absolute path
instead. A checkout has no checksum file to download the package's native library
against, so the first compile builds it with a Rust toolchain and takes a few minutes.

## Where to go next

The echo in [`../echo`](../echo/README.md) is the project of the getting-started pages, and
[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what a tasklet, a relay and a runner each are.
