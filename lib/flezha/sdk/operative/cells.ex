defmodule Flezha.SDK.Operative.Cells do
  @moduledoc """
  An operative instance's Cells, held in the state of the process that owns
  them — the stream `Flezha.SDK.Operative` or the workflow
  `Flezha.SDK.Workflow.Driver` — as `%{node_ref => %{key => pid}}`.

  Single Cells and the owned Static ones are minted at start; a Dynamic key's
  Cell is minted on the first value addressed to it, up to its node's
  `max_cells/0`, and a new key past the cap reaches nothing. Under a declared
  `cell_idle_ttl_ms/0` one `:reap` tick sweeps them in the owner, so no push
  can be routed while it asks each Cell whether it is idle — the drain gate —
  and an idle one evicts itself destructively before the next push can mint
  its successor. Evicted keys are remembered, bounded, so a live read can say
  "evicted" rather than "never existed". The set also holds the owner's
  plugin outputs no bound stream would take, replayed after the Cells rebind.

  One option is the kind's: a stream Cell parks what its ladder exhausts,
  a workflow Cell reports it to the driver, and has no timers until they fire
  inside a workflow.
  """

  require Logger

  alias Flezha.SDK.{Cell, Counters, Hold, KeyGroup, Knob, Park, Tasklet, Topology}

  # Evicted keys remembered; beyond it a key reads as never existed.
  @evicted_trace_cap 1_024
  # Evictions one sweep performs at most: the owner is held while it runs,
  # so a burst of expiries drains over a few ticks rather than stalling it.
  @evict_budget 256

  @enforce_keys [:operative_id, :topology, :kind]
  defstruct [
    :operative_id,
    :topology,
    :kind,
    :range,
    :counters,
    :hold_cap,
    :runner,
    :secondaries,
    :slots,
    :credit,
    :held,
    cells: %{},
    dynamic: %{},
    evicted: nil
  ]

  @type t :: %__MODULE__{}

  @doc """
  The Cell set of the calling process, its Single and owned Static Cells
  minted. `opts` carries `:operative_id`, `:topology`, `:kind` (`:stream` |
  `:workflow`), `:counters`, `:runner` and optionally `:range`, `:hold_cap`,
  `:secondaries` and `:credit`.
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    topology = Keyword.fetch!(opts, :topology)
    counters = Keyword.fetch!(opts, :counters)
    hold_cap = Keyword.get(opts, :hold_cap) || Hold.cap()

    prewarm(%__MODULE__{
      operative_id: Keyword.fetch!(opts, :operative_id),
      topology: topology,
      kind: Keyword.fetch!(opts, :kind),
      range: Keyword.get(opts, :range) || KeyGroup.decode(0, 0, 0, 0),
      counters: counters,
      hold_cap: hold_cap,
      runner: Keyword.get(opts, :runner),
      secondaries: Keyword.get(opts, :secondaries, []),
      # The slot survives the Cell: what a persistent Cell last shipped,
      # for the replacement of one killed untrappably.
      slots: :ets.new(:flezha_sdk_slots, [:public, :set]),
      credit: Keyword.get(opts, :credit),
      dynamic: dynamic_knobs(topology),
      evicted: Hold.new(cap: @evicted_trace_cap),
      held: Hold.new(cap: hold_cap, counters: counters)
    })
  end

  @doc "Every live Cell's pid."
  @spec pids(t()) :: [pid()]
  def pids(%__MODULE__{cells: cells}),
    do: for({_ref, keyed} <- cells, {_key, pid} <- keyed, do: pid)

  @doc """
  Push `item` into every Cell `value` reaches on `node_ref` by its node's
  mode — the one Cell of a Single node, the owned keys `cells_for/1` names on
  a Static one, the Cell of `key` on a Dynamic one, minted on first sight —
  answering those Cells' keys. A key the node's cap refuses reaches nothing;
  a Dynamic key this member does not own is `{:misroute, reason}`.
  """
  @spec deliver(t(), reference(), term(), String.t() | nil, term()) ::
          {[String.t() | nil] | {:misroute, term()}, t()}
  def deliver(%__MODULE__{} = c, node_ref, value, key, item) do
    case targets(c, node_ref, value, key) do
      {{:misroute, _reason} = misroute, c} -> {misroute, c}
      {keys, c} -> {keys, Enum.reduce(keys, c, &push(&2, node_ref, &1, item))}
    end
  end

  defp targets(%__MODULE__{topology: topo} = c, node_ref, value, key) do
    case Map.fetch!(topo.node_mode, node_ref) do
      :single ->
        cells_of(c, node_ref, [nil])

      :static ->
        keys =
          topo.node_modules[node_ref]
          |> Tasklet.cells_for(value)
          |> Enum.filter(&KeyGroup.owns?(c.range, &1))

        cells_of(c, node_ref, keys)

      :dynamic ->
        case KeyGroup.admit(c.range, key || "") do
          :ok -> cells_of(c, node_ref, [key])
          {:error, reason} -> {{:misroute, reason}, c}
        end
    end
  end

  defp cells_of(c, node_ref, keys) do
    Enum.reduce(keys, {[], c}, fn key, {found, c} ->
      case ensure(c, node_ref, key) do
        {:capped, c} -> {found, c}
        {_pid, c} -> {found ++ [key], c}
      end
    end)
  end

  # A `:drop_nothing` push is a `GenServer.call`, so a Cell that died between
  # the last input and this one would take the owner down unless the exit is
  # caught; the retry starts a fresh Cell, which is what the `:DOWN` clause
  # would have arranged, so "the key is routable again on its next input" is
  # true by construction rather than by ordering luck.
  defp push(c, node_ref, key, item, retry? \\ true) do
    case ensure(c, node_ref, key) do
      {:capped, c} -> c
      {pid, c} -> enqueue(c, pid, node_ref, key, item, retry?)
    end
  end

  defp enqueue(c, pid, node_ref, key, item, retry?) do
    case knob(c.topology, node_ref, :on_full, :drop_nothing) do
      :drop_nothing -> Cell.enqueue_block(pid, item)
      other -> Cell.enqueue_drop(pid, item, other)
    end

    c
  catch
    :exit, _reason when retry? -> c |> forget(node_ref, key) |> push(node_ref, key, item, false)
  end

  @doc """
  The Cell of `tasklet` (its `name/0`) at `key` for a lookup. `materialise`
  mints a Dynamic key that has no Cell yet — what a restore of state the
  controller holds for it needs — unless the node is at its cap.
  """
  @spec lookup(t(), String.t(), String.t() | nil, boolean()) ::
          {{:ok, pid()}
           | {:error, :tasklet_not_found | :cell_not_found | :cell_evicted | :cell_capped}, t()}
  def lookup(%__MODULE__{topology: topo} = c, tasklet, key, materialise?) do
    with {:ok, node_ref} <- Topology.node_of(topo, tasklet),
         {:ok, slot_key} <- slot_key(c, node_ref, key) do
      case {get_in(c.cells, [node_ref, slot_key]), materialise?} do
        {nil, false} -> {{:error, missing(c, node_ref, slot_key)}, c}
        {nil, true} -> materialise(c, node_ref, slot_key)
        {pid, _} -> {{:ok, pid}, c}
      end
    else
      {:error, _} = error -> {error, c}
    end
  end

  defp materialise(c, node_ref, key) do
    case ensure(c, node_ref, key) do
      {:capped, c} -> {{:error, :cell_capped}, c}
      {pid, c} -> {{:ok, pid}, c}
    end
  end

  # Evicted and never-existed serve the same absence; the diagnosis differs.
  defp missing(%__MODULE__{topology: topo} = c, node_ref, key) do
    if topo.node_mode[node_ref] == :dynamic and Hold.member?(c.evicted, key),
      do: :cell_evicted,
      else: :cell_not_found
  end

  @doc """
  Every node as `{tasklet_name, mode, [{key, cell_pid}]}`, the live-read
  walk's input.
  """
  @spec listing(t()) :: [{String.t(), :single | :static | :dynamic, [{term(), pid()}]}]
  def listing(%__MODULE__{topology: topo} = c) do
    Enum.map(topo.order, fn node_ref ->
      pids = c.cells |> Map.get(node_ref, %{}) |> Enum.sort()
      {topo.node_modules[node_ref].name(), topo.node_mode[node_ref], pids}
    end)
  end

  @doc """
  A Cell died: forget it, logging a crash, and mint a Single or Static one
  again at once, as `new/1` minted it — what its slot holds, armed timers
  above all, must not wait for an input. A Dynamic one stays lazy, as its
  keys are.
  """
  @spec down(t(), pid(), term()) :: t()
  def down(%__MODULE__{} = c, pid, reason) do
    dead = for {ref, keyed} <- c.cells, {key, ^pid} <- keyed, do: {ref, key}

    Enum.reduce(dead, c, fn {node_ref, key}, acc ->
      if reason not in [:normal, :shutdown] do
        Logger.warning(
          "[Flezha.SDK] cell crashed: operative=#{acc.operative_id} " <>
            "node=#{inspect(node_ref)} key=#{inspect(key)} reason=#{inspect(reason)}"
        )
      end

      acc |> forget(node_ref, key) |> remint(node_ref, key, reason)
    end)
  end

  defp remint(%__MODULE__{topology: topo} = c, node_ref, key, reason) do
    if reason in [:normal, :shutdown] or topo.node_mode[node_ref] == :dynamic,
      do: c,
      else: c |> ensure(node_ref, key) |> elem(1)
  end

  @doc """
  Bind every live Cell, and every Cell minted from now on, to `secondaries`,
  then replay the outputs no stream would take. Each Cell rebinds and replays
  its held records BEFORE an output of the same dispatch is replayed, which
  is why a Cell's rebind is a call: a send only orders the mailbox, and the
  guarantee is about the order two processes write to the wire.
  """
  @spec rebind(t(), list()) :: t()
  def rebind(%__MODULE__{} = c, secondaries) do
    Enum.each(pids(c), &Cell.rebind(&1, secondaries))
    %{c | secondaries: secondaries, held: c.held |> Hold.bind(secondaries) |> Hold.replay()}
  end

  @doc """
  Ship one plugin output on a bound stream. An output no stream will take is
  held for the next bound one rather than lost: the caller cannot learn of a
  send-side loss, so nothing here may discard a frame in silence.
  """
  @spec emit_output(t(), map(), binary()) :: t()
  def emit_output(%__MODULE__{} = c, meta, bytes) do
    hold = Hold.bind(c.held, c.secondaries)
    {_verdict, held} = Hold.write({:output, c.operative_id}, hold, {bytes, meta})
    %{c | held: held}
  end

  @doc """
  Arm the next `:reap` tick to the calling process, at the shortest TTL's
  cadence; a node declaring no TTL arms nothing.
  """
  @spec schedule_reap(t()) :: t()
  def schedule_reap(%__MODULE__{dynamic: dynamic} = c) do
    case for {_ref, %{ttl_ms: ttl}} <- dynamic, ttl != nil, do: ttl do
      [] -> c
      ttls -> Process.send_after(self(), :reap, reap_interval(Enum.min(ttls)))
    end

    c
  end

  defp reap_interval(ttl_ms),
    do:
      Knob.get(
        :reap_interval_ms,
        ttl_ms |> div(4) |> min(Knob.get(:sdk_max_reap_interval_ms)) |> max(1)
      )

  @doc """
  One sweep: every Dynamic Cell idle past its node's TTL is evicted, except
  those whose wire identity is in `keep`.
  """
  @spec reap(t(), MapSet.t(binary())) :: t()
  def reap(%__MODULE__{} = c, keep) do
    {c, _left} =
      Enum.reduce(c.dynamic, {c, @evict_budget}, fn
        {node_ref, %{ttl_ms: ttl}}, {acc, left} when ttl != nil ->
          reap_node(acc, node_ref, ttl, left, keep)

        _no_ttl, acc ->
          acc
      end)

    c
  end

  # Two calls per Cell: a bounded probe that changes nothing, then — for one
  # proved idle — the eviction, synchronous, so its delete is on the wire
  # before any push can mint a successor on the same stream.
  defp reap_node(c, node_ref, ttl_ms, budget, keep) do
    c.cells
    |> Map.get(node_ref, %{})
    |> Enum.reduce({c, budget}, fn
      _cell, {acc, 0} ->
        {acc, 0}

      {key, pid}, {acc, left} ->
        if not MapSet.member?(keep, wire_identity(acc, node_ref, key)) and
             Cell.idle_for?(pid, ttl_ms, Knob.get(:sdk_idle_probe_ms)) and
             Cell.evict(pid) == :evicted,
           do: {note_evicted(forget(acc, node_ref, key), key), left - 1},
           else: {acc, left}
    end)
  end

  @doc "The wire identity of the Cell of `key` on `node_ref`."
  @spec wire_identity(t(), reference(), String.t() | nil) :: binary()
  def wire_identity(%__MODULE__{topology: topo} = c, node_ref, key),
    do: Park.identity(c.operative_id, topo.node_modules[node_ref].name(), key)

  defp note_evicted(%__MODULE__{evicted: evicted} = c, key) do
    dynamic_event(c, :evicted)
    %{c | evicted: Hold.push(evicted, key)}
  end

  defp dynamic_event(%__MODULE__{operative_id: oid}, event),
    do:
      :telemetry.execute([:flezha, :sdk, :dynamic], %{count: 1}, %{
        operative_id: oid,
        event: event
      })

  # --- minting ---

  # Eagerly create Single (one) and owned Static cells; Dynamic ones are lazy.
  defp prewarm(%__MODULE__{topology: topo} = c) do
    Enum.reduce(topo.node_mode, c, fn
      {node_ref, :single}, acc ->
        acc |> ensure(node_ref, nil) |> elem(1)

      {node_ref, :static}, acc ->
        topo.node_modules[node_ref]
        |> Tasklet.cell_keys()
        |> Enum.filter(&KeyGroup.owns?(acc.range, &1))
        |> Enum.reduce(acc, fn key, a -> a |> ensure(node_ref, key) |> elem(1) end)

      {_node_ref, :dynamic}, acc ->
        acc
    end)
  end

  defp ensure(c, node_ref, key) do
    keyed = Map.get(c.cells, node_ref, %{})

    case Map.get(keyed, key) do
      nil -> mint(c, node_ref, key, keyed)
      pid -> {pid, c}
    end
  end

  # A dynamic node at its cap mints nothing for a new key: the shared core's
  # best-effort cap — counted, never reported back to the controller.
  defp mint(%__MODULE__{dynamic: dynamic} = c, node_ref, key, keyed) do
    case Map.get(dynamic, node_ref) do
      %{cap: cap} when map_size(keyed) >= cap ->
        dynamic_event(c, :capped)
        {:capped, c}

      knobs ->
        {:ok, pid} = start_cell(c, node_ref, key)
        Process.monitor(pid)
        if knobs, do: dynamic_event(c, :created)
        {pid, %{c | cells: Map.put(c.cells, node_ref, Map.put(keyed, key, pid))}}
    end
  end

  @doc """
  Drop a dead Cell and release its in-flight slot. Idempotent with `down/3`,
  which does the same for the down message that follows.
  """
  @spec forget(t(), reference(), String.t() | nil) :: t()
  def forget(%__MODULE__{} = c, node_ref, key) do
    Counters.dispatch_finished(c.counters, Cell.identity(c.operative_id, node_ref, key))
    %{c | cells: Map.update(c.cells, node_ref, %{}, &Map.delete(&1, key))}
  end

  defp start_cell(%__MODULE__{topology: topo} = c, node_ref, key) do
    module = topo.node_modules[node_ref]
    ladder = Topology.retry_ladder(topo, node_ref)

    Cell.start(
      operative: self(),
      operative_id: c.operative_id,
      node_ref: node_ref,
      node_name: topo.node_name[node_ref],
      # The channel this Cell consumes from: what a park reports, matching
      # the reference SDK.
      input_channel: inbound_channel(topo, node_ref),
      module: module,
      cell_key: key,
      capacity: Tasklet.mailbox_capacity(module),
      policy: knob(topo, node_ref, :on_full, :drop_nothing),
      retry_max: ladder.retry_max,
      retry_backoff_ms: ladder.retry_backoff_ms,
      # A workflow answers an exhausted ladder by unwinding, not by shedding
      # the input to the dead-letter queue.
      park_on_exhaustion: c.kind == :stream,
      counters: c.counters,
      hold_cap: c.hold_cap,
      secondaries: c.secondaries,
      slots: c.slots,
      credit: c.credit,
      persistence_enabled: topo.persistence_enabled?,
      tier: tier_facts(topo, node_ref),
      ctx_opts: [runner: c.runner] ++ timers(c.kind) ++ ctx_opts(topo, node_ref, key)
    )
  end

  # Timers do not fire inside a workflow yet: every timer call answers
  # `{:error, :timers_unavailable}` and arms nothing.
  defp timers(:workflow), do: [timers: :unavailable]
  defp timers(:stream), do: []

  # What a Cell of this node needs to know about the tier: whether it is on,
  # the checkpoint policy, the sink routes it captures its own outputs through
  # — the router delivers a tier class's emits to nodes only — and whether it
  # has an inter-tasklet edge at all.
  defp tier_facts(topo, node_ref) do
    routes = Map.get(topo.routes, {:node, node_ref}, [])

    %{
      on?: topo.effectively_once?,
      checkpoint: topo.checkpoint,
      sink_routes: for(%{to: {:sink, _}} = r <- routes, do: r),
      edges?: Enum.any?(routes, &match?(%{to: {:node, _}}, &1)),
      correction: topo.correction
    }
  end

  # The facts the ctx refuses on: this node's channel name, whether an output
  # reaches a keyed sink, and whether such a sink leaves a keyless Cell no
  # key to fall back on.
  defp ctx_opts(topo, node_ref, key) do
    keyed =
      for %{to: {:sink, _}, keyed: true} = r <- Map.get(topo.routes, {:node, node_ref}, []), do: r

    [
      channel: topo.node_channel[node_ref],
      keyed_sink?: keyed != [],
      key_required?: key == nil and Enum.any?(keyed, &is_nil(&1.key_by))
    ]
  end

  # The key a Cell is registered under for its node's mode: none for a
  # Single node, a declared and owned one for a Static node, any owned key for
  # a Dynamic node.
  defp slot_key(%__MODULE__{topology: topo} = c, node_ref, key) do
    case Map.fetch!(topo.node_mode, node_ref) do
      :single when key in [nil, ""] ->
        {:ok, nil}

      :static ->
        if key in Tasklet.cell_keys(topo.node_modules[node_ref]) and
             KeyGroup.owns?(c.range, key),
           do: {:ok, key},
           else: {:error, :cell_not_found}

      :dynamic when is_binary(key) ->
        if KeyGroup.owns?(c.range, key), do: {:ok, key}, else: {:error, :cell_not_found}

      _ ->
        {:error, :cell_not_found}
    end
  end

  # The dynamic nodes' knobs, read once: the cap, and the TTL when declared.
  defp dynamic_knobs(topo) do
    for {node_ref, :dynamic} <- topo.node_mode, into: %{} do
      module = topo.node_modules[node_ref]

      {node_ref, %{cap: Tasklet.max_cells(module), ttl_ms: Tasklet.cell_idle_ttl_ms(module)}}
    end
  end

  # The channel a node consumes from — an entry channel by its own name, an
  # inter-tasklet edge by the producing node's output name. A static topology
  # fact: a node several channels feed reports the first route found.
  defp inbound_channel(topo, node_ref) do
    Enum.find_value(topo.routes, fn {source, routes} ->
      if Enum.any?(routes, &(&1.to == {:node, node_ref})) do
        case source do
          {:entry, channel} -> channel
          {:node, producer} -> topo.node_name[producer] || inspect(producer)
        end
      end
    end)
  end

  defp knob(topo, node_ref, key, default),
    do: get_in(topo.node_knobs, [node_ref, key]) || default
end
