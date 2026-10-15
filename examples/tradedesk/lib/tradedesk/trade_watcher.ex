defmodule Tradedesk.TradeWatcher do
  @moduledoc """
  The desk's stream: trades in, each price compared with its symbol's recent
  average, and an order started when a price jumps far enough above it.

  A flow relay runs for as long as the desk does. The exchange sends
  other messages beside its trades and delivers several trades in one message,
  so the wiring keeps the trade messages, splits each into its trades before
  anything else sees them, drops what the desk does not follow, and routes the
  rest to a cell per symbol.
  """
  use Flezha.SDK.Relay, factory_id: "trade_watcher"

  @doc """
  Four transforms on the wiring, then two tasklets, and one place output
  leaves the desk.

  The entry channel passes bytes, so it decodes each message from JSON. The
  first `filter` keeps the exchange's trade messages: the same connection also
  carries the confirmation of every subscription, which has no trades to
  split. `flat_map` turns one message into its trades, each routed on its own.
  The second `filter` keeps the market orders. `key_by` names the cell each
  trade belongs to, which gives every symbol its own state and its own order
  of arrival; it is the last link before `into`, because it is evaluated on
  whatever the transforms above it produced. The four run at the routing seam
  rather than as tasklets of their own, which is why they must stay cheap and
  free of side effects.

  Only the first stage is keyed: a plain `into` leaves its consumer
  single-cell, which is right here, since the second stage keeps nothing per
  symbol.
  """
  @impl true
  def channels(c) do
    c
    # JSON from the exchange
    |> input("trades")
    # trade messages only
    |> filter(&(&1["channel"] == "trade"))
    # a message into its trades
    |> flat_map(& &1["data"])
    # market orders only
    |> filter(&(&1["ord_type"] == "market"))
    # one cell per symbol
    |> key_by(& &1["symbol"])
    |> into(Tradedesk.TrackPrices)
    # fed by the tasklet above
    |> into(Tradedesk.BuyAboveAverage)
    # out to the file plugin
    |> output("signals")
  end
end
