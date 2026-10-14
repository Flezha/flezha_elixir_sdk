defmodule Tradedesk.Application do
  @moduledoc """
  Starts the desk's runner: the process that connects to the controller and
  serves tradedesk's relays.

  The namespace is the first argument and defaults to `trading`:
  `mix run --no-halt -- my_desk`. A runner serves one namespace, and under an
  enforced controller it proves it with that namespace's key, read from
  `FLEZHA_NAMESPACE_SECRET`.

  The guides write the desk's relays one at a time, and each is listed in
  `relays:`. Listing one starts nothing: it offers the controller relays
  of that type, which are started from it later. The list takes both, the flow
  relay that tracks prices and the steps relay that places one order.
  """
  use Application

  @impl true
  def start(_type, _args) do
    namespace = List.first(System.argv(), "trading")

    children = [
      {Flezha.SDK,
       namespace: namespace,
       relays: [Tradedesk.TradeWatcher, Tradedesk.OrderExecutor],
       transport: :uds}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
