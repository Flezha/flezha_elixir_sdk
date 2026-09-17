defmodule Flezha.SDK.RouteLatest do
  @moduledoc """
  The single-entity procedure `Flezha.SDK.Channels.route_latest/4` expands to:
  every key seen on the traffic stream receives each distinct document once, on
  that key's own next traffic item, as a `{key, document}` pair.

  A dynamically keyed procedure cannot receive a broadcast, and fanning a
  document out over a remembered key set would emit keys a partitioned member
  may no longer own. Delivering on the key's own traffic emits only keys that
  traffic just proved this member owns, at the price of one tick of latency per
  key. A key never seen in traffic receives nothing.

  The document is opaque: retained and compared with `==`, never parsed. One
  equal to the retained document is inert; a different one replaces it and
  bumps the version, so every key is owed a delivery again. The state
  `%{document, version, delivered}` rides the change journal like any
  procedure's.
  """

  @behaviour Flezha.SDK.Procedure

  alias Flezha.SDK.Ctx

  @impl true
  def name, do: "route_latest"

  # Version 0 means nothing is retained. A key's absent entry reads as 0, so it
  # is owed nothing until the first document and is behind that one.
  @impl true
  def init_state(_key), do: %{document: nil, version: 0, delivered: %{}}

  @impl true
  def apply_change(state, {:retain, document}),
    do: %{state | document: document, version: state.version + 1}

  def apply_change(state, {:delivered, key, version}),
    do: %{state | delivered: Map.put(state.delivered, key, version)}

  @impl true
  def run({:document, document}, %{version: version, document: retained} = state, ctx) do
    if version > 0 and retained == document,
      do: {:ok, state},
      else: {:ok, Ctx.apply(ctx, state, {:retain, document})}
  end

  def run({:traffic, key}, %{version: version} = state, ctx) do
    if Map.get(state.delivered, key, 0) >= version do
      {:ok, state}
    else
      # Emit first: a failed emit rides the retry ladder, and a delivery
      # recorded before it would make the retry silent.
      with :ok <- Ctx.emit(ctx, {key, state.document}),
           do: {:ok, Ctx.apply(ctx, state, {:delivered, key, version})}
    end
  end
end
