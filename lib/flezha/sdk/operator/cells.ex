defmodule Flezha.SDK.Operator.Cells do
  @moduledoc """
  An operator instance's Cells, held in the state of the process that owns
  them — the unbounded `Flezha.SDK.Operator` or the bounded operator
  `Flezha.SDK.Bounded.Driver` — as `%{node_ref => %{key => pid}}`, with each
  live Cell's `Flezha.SDK.Cell.Room`, which a push takes its slot from.

  Single Cells and the owned Static ones are minted at start; a Dynamic key's
  Cell is minted on the first value addressed to it, up to its node's
  `max_cells/0`, and a new key past the cap reaches nothing. Under a declared
  `idle_ttl_seconds/0` one `:reap` tick sweeps them in the owner, so no push
  can be routed while it asks each Cell whether it is idle — the drain gate —
  and an idle one evicts itself destructively before the next push can mint
  its successor. Evicted keys are remembered, bounded, so a live read can say
  "evicted" rather than "never existed". The set also holds the owner's
  plugin outputs no bound stream would take, replayed after the Cells rebind.

  One option is the kind's: an unbounded Cell parks what its ladder exhausts,
  a bounded operator Cell reports it to the driver, and has no timers until they fire
  inside a bounded operator.
  """

  require Logger

  alias Flezha.SDK.{
    Cell,
    Counters,
    Hold,
    KeyGroup,
    Knob,
    Operator,
    Park,
    Prefix,
    Tasklet,
    Topology
  }

  # Evicted keys remembered; beyond it a key reads as never existed.
  @evicted_trace_cap 1_024
  # Evictions one sweep performs at most: the owner is held while it runs,
  # so a burst of expiries drains over a few ticks rather than stalling it.
  @evict_budget 256

  @enforce_keys [:operator_id, :topology, :kind]
  defstruct [
    :operator_id,
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
    :prefix,
    cells: %{},
    rooms: %{},
    dynamic: %{},
    evicted: nil
  ]

  @type t :: %__MODULE__{}

  @doc """
  The Cell set of the calling process, its Single and owned Static Cells
  minted. `opts` carries `:operator_id`, `:topology`, `:kind` (`:unbounded` |
  `:bounded`), `:counters`, `:runner` and optionally `:range`, `:hold_cap`,
  `:secondaries`, `:credit` and `:prefix`, the instance's processed-prefix
  ledger.
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    topology = Keyword.fetch!(opts, :topology)
    counters = Keyword.fetch!(opts, :counters)
    hold_cap = Keyword.get(opts, :hold_cap) || Hold.cap()

    prewarm(%__MODULE__{
      operator_id: Keyword.fetch!(opts, :operator_id),
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
      prefix: Keyword.get(opts, :prefix),
      dynamic: dynamic_knobs(topology),
      evicted: Hold.new(cap: @evicted_trace_cap),
      held: Hold.new(cap: hold_cap, counters: counters)
    })
  end

  @doc "The live Cell of `node_ref` under `key`, or `nil`."
  @spec pid(t(), reference(), String.t() | nil) :: pid() | nil
  def pid(%__MODULE__{cells: cells}, node_ref, key),
    do: cells |> Map.get(node_ref, %{}) |> Map.get(key)

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
        case admit(c, node_ref, key) do
          :ok -> cells_of(c, node_ref, [key])
          {:error, reason} -> {{:misroute, reason}, c}
        end
    end
  end

  @doc """
  Whether this member may mint a Cell of `node_ref` under `key`: a Dynamic
  node's key must hash into the owned range; a Single or Static node's Cells
  are declared, and an unowned declared one simply stays idle.
  """
  @spec admit(t(), reference(), String.t() | nil) :: :ok | {:error, String.t()}
  def admit(%__MODULE__{topology: topo, range: range}, node_ref, key) do
    case Map.fetch!(topo.node_mode, node_ref) do
      :dynamic -> KeyGroup.admit(range, key || "")
      _declared -> :ok
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

  # A push to a Cell that died between the last input and this one exits,
  # and would take the owner down unless the exit is caught; the retry starts
  # a fresh Cell, which is what the `:DOWN` clause
  # would have arranged, so "the key is routable again on its next input" is
  # true by construction rather than by ordering luck. Unless the instance
  # still holds unfinished input: then the owner ends, as `down/3` would.
  defp push(c, node_ref, key, item, retry? \\ true) do
    case ensure(c, node_ref, key) do
      {:capped, c} -> c
      {pid, c} -> enqueue(c, pid, node_ref, key, item, retry?)
    end
  end

  # An input with an offset holds the instance's processed prefix below it
  # from here until the Cell releases it.
  defp enqueue(c, pid, node_ref, key, item, retry?) do
    offset = held_offset(c, item)
    if offset, do: Prefix.hold(c.prefix, offset)

    try do
      case knob(c.topology, node_ref, :on_full, :drop_nothing) do
        :drop_nothing -> Cell.push(pid, Map.fetch!(c.rooms, pid), item)
        other -> Cell.enqueue_drop(pid, item, other)
      end

      c
    catch
      :exit, reason ->
        Prefix.release(c.prefix, offset)
        crashed(c, node_ref, key, reason)

        if retry?,
          do: c |> forget(node_ref, key) |> push(node_ref, key, item, false),
          else: exit(reason)
    end
  end

  defp held_offset(%__MODULE__{prefix: nil}, _item), do: nil
  defp held_offset(%__MODULE__{}, %Cell.Input{offset: offset}), do: offset
  defp held_offset(%__MODULE__{}, _item), do: nil

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

  A crash while the instance holds unfinished input exits the calling owner
  with `{:cell_exited, reason}` instead of minting anything.
  """
  @spec down(t(), pid(), term()) :: t()
  def down(%__MODULE__{} = c, pid, reason) do
    dead = for {ref, keyed} <- c.cells, {key, ^pid} <- keyed, do: {ref, key}

    Enum.reduce(dead, c, fn {node_ref, key}, acc ->
      if not Operator.clean_exit?(reason), do: crashed(acc, node_ref, key, reason)
      acc |> forget(node_ref, key) |> remint(node_ref, key, reason)
    end)
  end

  # Logged, so a key crashing on every input is visible. A held offset is an
  # input some Cell took and has not finished, and only the input log can
  # deliver it again: a replacement minted here could take a later input and
  # write a floor past it. So the owner ends instead, and the controller
  # rebinds the instance and re-forwards the log above its floor. Only an
  # instance with durable input keeps a ledger.
  defp crashed(%__MODULE__{} = c, node_ref, key, reason) do
    Logger.warning(
      "[Flezha.SDK] cell crashed: operator=#{c.operator_id} " <>
        "node=#{inspect(node_ref)} key=#{inspect(key)} reason=#{inspect(reason)}"
    )

    if c.prefix && Prefix.pending?(c.prefix), do: exit({:cell_exited, reason})
    :ok
  end

  defp remint(%__MODULE__{topology: topo} = c, node_ref, key, reason) do
    if Operator.clean_exit?(reason) or topo.node_mode[node_ref] == :dynamic,
      do: c,
      else: c |> ensure(node_ref, key) |> elem(1)
  end

  @doc """
  Kill every live Cell and wait until each is gone, `sdk_shutdown_ack_ms` in
  all at most.
  """
  @spec kill_all(t()) :: :ok
  def kill_all(%__MODULE__{} = c) do
    deadline = System.monotonic_time(:millisecond) + Knob.get(:sdk_shutdown_ack_ms)

    c
    |> pids()
    |> Enum.map(fn pid ->
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      {ref, pid}
    end)
    |> Enum.each(fn {ref, pid} ->
      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      after
        max(deadline - System.monotonic_time(:millisecond), 0) -> :ok
      end
    end)
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
    {_verdict, held} = Hold.write({:output, c.operator_id}, hold, {bytes, meta})
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
    do: Park.identity(c.operator_id, topo.node_modules[node_ref].name(), key)

  defp note_evicted(%__MODULE__{evicted: evicted} = c, key) do
    dynamic_event(c, :evicted)
    %{c | evicted: Hold.push(evicted, key)}
  end

  defp dynamic_event(%__MODULE__{operator_id: oid}, event),
    do:
      :telemetry.execute([:flezha, :sdk, :dynamic], %{count: 1}, %{
        operator_id: oid,
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
        room = Cell.Room.new(Tasklet.mailbox_capacity(c.topology.node_modules[node_ref]))
        {:ok, pid} = start_cell(c, node_ref, key, room)
        Process.monitor(pid)
        if knobs, do: dynamic_event(c, :created)
        cells = Map.put(c.cells, node_ref, Map.put(keyed, key, pid))
        {pid, %{c | cells: cells, rooms: Map.put(c.rooms, pid, room)}}
    end
  end

  @doc """
  Drop a dead Cell and release its in-flight slot. Idempotent with `down/3`,
  which does the same for the down message that follows.
  """
  @spec forget(t(), reference(), String.t() | nil) :: t()
  def forget(%__MODULE__{} = c, node_ref, key) do
    Counters.dispatch_finished(c.counters, Cell.identity(c.operator_id, node_ref, key))

    %{
      c
      | cells: Map.update(c.cells, node_ref, %{}, &Map.delete(&1, key)),
        rooms: Map.delete(c.rooms, pid(c, node_ref, key))
    }
  end

  defp start_cell(%__MODULE__{topology: topo} = c, node_ref, key, room) do
    module = topo.node_modules[node_ref]
    ladder = Topology.retry_ladder(topo, node_ref)

    Cell.start(
      operator: self(),
      operator_id: c.operator_id,
      node_ref: node_ref,
      node_name: topo.node_name[node_ref],
      # The channel this Cell consumes from: what a park reports, matching
      # the reference SDK.
      input_channel: inbound_channel(topo, node_ref),
      module: module,
      cell_key: key,
      room: room,
      policy: knob(topo, node_ref, :on_full, :drop_nothing),
      retry_max: ladder.retry_max,
      retry_backoff_ms: ladder.retry_backoff_ms,
      # A bounded operator answers an exhausted ladder by unwinding, not by shedding
      # the input to the dead-letter queue, and decides when a due firing runs.
      park_on_exhaustion: c.kind == :unbounded,
      gate_firings: c.kind == :bounded,
      counters: c.counters,
      hold_cap: c.hold_cap,
      secondaries: c.secondaries,
      slots: c.slots,
      credit: c.credit,
      persistence_enabled: topo.persistence_enabled?,
      tier: tier_facts(c, topo, node_ref),
      ctx_opts: [runner: c.runner] ++ ctx_opts(topo, node_ref, key)
    )
  end

  # What a Cell of this node needs to know about its records: whether it
  # writes effect records (the tier, or a durable bounded operator, whose stage commit
  # names them), the checkpoint policy, the sink routes it captures its own
  # outputs through (the router delivers such a class's emits to nodes only),
  # whether a record waits for an inter-tasklet edge (a bounded operator's edge value
  # waits on its driver instead) and whether it is a bounded stage's Cell.
  defp tier_facts(%__MODULE__{kind: kind} = c, topo, node_ref) do
    routes = Map.get(topo.routes, {:node, node_ref}, [])
    stage? = kind == :bounded

    %{
      on?: topo.effectively_once? or (stage? and topo.persistence_enabled?),
      checkpoint: topo.checkpoint,
      sink_routes: for(%{to: {:sink, _}} = r <- routes, do: r),
      edge_routes:
        for(
          %{to: {:node, consumer}} = r <- routes,
          do: %{
            channel: topo.node_seq[node_ref],
            consumer: topo.node_seq[consumer],
            transform: r.transform
          }
        ),
      edges?: not stage? and Enum.any?(routes, &match?(%{to: {:node, _}}, &1)),
      stage?: stage?,
      correction: topo.correction,
      prefix: c.prefix
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

      {node_ref, %{cap: Tasklet.max_cells(module), ttl_ms: Tasklet.idle_ttl_ms(module)}}
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
