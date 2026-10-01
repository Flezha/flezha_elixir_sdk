defmodule Flezha.SDK.CallEnd do
  @moduledoc """
  The `call(...)` end's share of `Flezha.SDK.Topology`'s check, re-expressing
  `flz_core`'s: a call closes only its own chain, as an edge does, and like a
  `response()` it is a plugin end beside which no sink, response or other call
  may stand; its deadline is positive, a factory call that may not create its
  callee is keyed, and its Response channel feeds a tasklet.
  """

  @issue_details %{
    CallDeadlineZero: [:channel],
    CallOutsidePerInputLoop: [:channel],
    CallResponseUnread: [:channel, :response],
    CallWithoutKey: [:channel],
    SinkOnEntry: [:channel]
  }

  @doc "The detail keys of a call issue's positional fields; `nil` for another issue."
  @spec issue_details(atom()) :: [atom()] | nil
  def issue_details(tag), do: @issue_details[tag]

  @doc """
  Close a chain of the channel `state` derives on `wire`, a response or a
  call end; `key?` is whether the chain keys its values before it.
  """
  @spec terminate(map(), map(), boolean()) :: map()
  def terminate(state, %{call: call} = wire, key?) do
    found =
      for {true, issue} <- [
            {call != nil and call.deadline_ms == 0, {:CallDeadlineZero, state.name}},
            {call != nil and call.factory? and not call.create and not key?,
             {:CallWithoutKey, state.name}},
            {state.sinks != [], {:ChannelEndsTwice, state.name}}
          ],
          do: issue

    state = %{state | issues: Enum.reverse(found) ++ state.issues}
    if state.sinks == [], do: %{state | sinks: [wire]}, else: state
  end

  @doc "A call end whose Response channel no tasklet reads, per derived channel."
  @spec unread([map()]) :: [tuple()]
  def unread(channels) do
    read =
      for %{source: {:entry, name}, consumers: [_ | _]} <- channels, into: MapSet.new(), do: name

    for channel <- channels,
        %{call: %{response: response}} <- channel.sinks,
        not MapSet.member?(read, response),
        do: {:CallResponseUnread, channel.name, response}
  end

  @doc "A call end on a class whose loop is not `:per_input`, which never reads its Response."
  @spec outside_per_input_loop(map(), atom()) :: [tuple()]
  def outside_per_input_loop(_routes, :per_input), do: []

  def outside_per_input_loop(routes, _each) do
    for {_source, routes} <- routes,
        %{call: %{}, channel: channel} <- routes,
        do: {:CallOutsidePerInputLoop, channel}
  end
end
