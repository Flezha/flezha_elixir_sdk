# The row interpreter for `test/fixtures/reference_topologies.txt`.
#
# The palette procedures below are the fixture's roles bound to this SDK. They
# exist only for the conformance run and are deliberately outside
# `ElixirRunner.Factories.all/0` — a runner never advertises them.

defmodule ElixirRunner.Conformance.PlainProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  def name, do: "plain"
  def init_state(_key), do: nil
  def run(_input, state, _ctx), do: {:ok, state}
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("plain")
end

defmodule ElixirRunner.Conformance.SecondProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  def name, do: "second"
  def init_state(_key), do: nil
  def run(_input, state, _ctx), do: {:ok, state}
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("second")
end

defmodule ElixirRunner.Conformance.TwinProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  # A distinct module answering the SAME name as `plain`: that IS the collision.
  def name, do: "plain"
  def init_state(_key), do: nil
  def run(_input, state, _ctx), do: {:ok, state}
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("twin")
end

defmodule ElixirRunner.Conformance.StaticProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  # The role is "the static declaration missing its other half". In this SDK
  # `entities_for/1` defaults to the whole keyset, so keys without a router are
  # complete and a router without keys is the incomplete half — the mirror of
  # the reference's, and the shape `EntitiesForMissing` names here.
  def name, do: "static"
  def init_state(_key), do: nil
  def run(_input, state, _ctx), do: {:ok, state}
  def entities_for(_input), do: ["a"]
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("static")
end

defmodule ElixirRunner.Conformance.RoutedProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  def name, do: "routed"
  def init_state(_key), do: nil
  def run(_input, state, _ctx), do: {:ok, state}
  def entity_keys, do: ["a", "b"]
  def entities_for(_input), do: ["a"]
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("routed")
end

defmodule ElixirRunner.Conformance.DynamicProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  # Dynamic mode is DERIVED here: a keyless procedure a keyed edge reaches.
  def name, do: "dynamic"
  def init_state(_key), do: nil
  def run(_input, state, _ctx), do: {:ok, state}
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("dynamic")
end

defmodule ElixirRunner.Conformance.NochangeProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  # A logical clock over a procedure that journals nothing: no `apply_change/2`.
  def name, do: "nochange"
  def init_state(_key), do: nil
  def run(_input, state, _ctx), do: {:ok, state}
  def clock, do: :logical
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("nochange")
end

defmodule ElixirRunner.Conformance.EventTimeProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure

  def name, do: "eventtime"
  def init_state(_key), do: nil
  def run(_input, state, _ctx), do: {:ok, state}
  def clock, do: :event_time
  def event_time_of(_input), do: 0
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("eventtime")
end

defmodule ElixirRunner.Conformance.Row do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative

  # One class module for every interpreted row: the steps it declares are the
  # row the interpreter is driving, so the class kind and the class flags a row
  # reads off its module both come from the same context.
  def factory_id, do: ElixirRunner.Conformance.context().topology
  def channels(c), do: ElixirRunner.Conformance.declare(c)
  def sync_durability, do: ElixirRunner.Conformance.context().sync_durability
end

defmodule ElixirRunner.Conformance do
  @moduledoc """
  The Elixir arm of the refusal table: every row of
  `test/fixtures/reference_topologies.txt` driven through
  `Flezha.SDK.Channels` and judged by `Flezha.SDK.Topology.build/2`.

  Two entry points, both in `ElixirRunner.Application`:

    * `--conformance-report <path>` writes the report `docs/sdk/conformance.md`
      § "The refusal table" specifies and exits 0 without connecting;
    * `FLEZHA_REFUSAL_FIXTURE=<topology id>` registers ONLY that row's
      topology at boot, so a refused one exits non-zero naming its refusal.

  One mapping this SDK's surface forces: `<proc>.mailbox(n)` is a procedure
  declaration here, not a builder knob, so the palette reads the row's capacity
  off `mailbox_capacity/1` rather than the handle carrying it.

  `class.persistent` maps to nothing: persistence is on unless a class opts out
  with `persistence_enabled/0`.
  """

  alias ElixirRunner.Conformance.Row
  alias Flezha.SDK.{Channels, Topology}

  @runtime "elixir"
  @fixture "test/fixtures/reference_topologies.txt"
  @manifest "test/apps/elixir_runner/conformance.json"

  # Authorable in no builder — the fixture header says why for each.
  @unauthorable ~w(detached raw_flags raw_class_flags)

  # The two rows the fixture cites rather than re-spells: this app's own
  # fixtures, whose derived lines are the seven-column table at the top.
  @cited %{
    "hybrid_channel" => {ElixirRunner.HybridChannelOp, :channels},
    "commit_block_chain" => {ElixirRunner.Workflows.TierCommitBlockWorkflow, :procedures}
  }

  @default_mailbox_capacity 1024
  @empty_context %{topology: "none", steps: [], mailbox: %{}, sync_durability: false}

  # --- the two entry points ---

  @doc "Drive every fixture row and write the conformance report to `path`."
  @spec write_report(String.t()) :: :ok
  def write_report(path) do
    surfaces = surfaces()
    rows = Enum.map(rows(), &report_row(&1, surfaces))
    File.write!(path, Jason.encode!(%{"runtime" => @runtime, "rows" => rows}))
  end

  @doc """
  The class set `FLEZHA_REFUSAL_FIXTURE` selects: only that row's topology, as
  `{streams, workflows}`. A refused row never reaches a connected runner — its
  refusal names are printed and the VM exits non-zero, which is what the drill
  reads.
  """
  @spec boot_classes(String.t()) :: {[module()], [module()]}
  def boot_classes(topology) do
    row = Enum.find(rows(), &(&1.topology == topology))

    cond do
      row == nil ->
        die("no fixture row is called #{topology}")

      skip_reason(row, surfaces()) != nil ->
        die("row #{topology} needs #{row.needs}, which this builder does not carry")

      true ->
        register(row)
    end
  end

  defp register(row) do
    {module, declare} = class_of(row)
    put_context(row)

    case Topology.build(module, declare) do
      {:ok, _topology} when declare == :channels -> {[module], []}
      {:ok, _topology} -> {[], [module]}
      {:error, {:invalid_topology, issues}} -> die(rejection(row.topology, issues))
    end
  end

  defp rejection(topology, issues),
    do: "topology rejected [#{Enum.join(names(issues), ", ")}] declaring #{topology}"

  defp die(message) do
    IO.puts(:stderr, "[elixir_runner] #{message}")
    System.halt(1)
  end

  # --- one row ---

  defp report_row(row, surfaces) do
    case skip_reason(row, surfaces) do
      nil -> outcome(row)
      needs -> %{"topology" => row.topology, "outcome" => "skipped", "needs" => needs}
    end
  end

  # A row is skipped only for a surface the fixture header names and this
  # builder does not carry, so every skip is checkable against the manifest.
  defp skip_reason(%{needs: "-"}, _surfaces), do: nil

  defp skip_reason(row, surfaces) do
    if row.needs in @unauthorable or row.needs not in surfaces, do: row.needs
  end

  defp outcome(row) do
    {module, declare} = class_of(row)
    put_context(row)

    case Topology.build(module, declare) do
      {:ok, topology} ->
        %{
          "topology" => row.topology,
          "outcome" => "accepted",
          "derived" => derived(topology, entry_order(module, declare))
        }

      {:error, {:invalid_topology, issues}} ->
        %{"topology" => row.topology, "outcome" => "refused", "names" => names(issues)}
    end
  end

  defp class_of(row), do: Map.get(@cited, row.topology, {Row, :channels})

  # The refusal names, read off the verdict as data: a refusal is a tagged
  # tuple whose tag IS the reference's name.
  defp names(issues), do: Enum.map(issues, &(&1 |> elem(0) |> Atom.to_string()))

  # --- the derived declaration ---

  # One entry per channel in declaration order: the entry channels the row
  # opened, then every producing channel something terminates on. A producer's
  # output channel nothing closes on is no channel at all, which is what the
  # fixture's seven-column table states.
  defp derived(topology, entry_order) do
    nodes = Enum.sort_by(topology.node_seq, fn {_ref, seq} -> seq end)

    entries = for name <- entry_order, do: {{:entry, name}, name, nil}
    outputs = for {ref, seq} <- nodes, do: {{:node, ref}, topology.node_channel[ref], seq}

    (entries ++ outputs)
    |> Enum.map(&channel(&1, topology))
    |> Enum.reject(&is_nil/1)
    |> Enum.with_index()
    |> Enum.map(fn {channel, index} -> Map.put(channel, "index", index) end)
  end

  defp channel({source, name, producer}, topology) do
    routes = Map.get(topology.routes, source, [])
    sinks = for %{to: {:sink, plugin}} <- routes, do: plugin
    consumers = for %{to: {:node, ref}} <- routes, do: Map.fetch!(topology.node_seq, ref)

    cond do
      producer == nil -> line(name, "entry", nil, consumers, sinks)
      sinks == [] and consumers == [] -> nil
      consumers == [] -> line(name, "plugin", producer, consumers, sinks)
      true -> line(name, "inter_proc", producer, consumers, sinks)
    end
  end

  defp line(name, kind, producer, consumers, sinks) do
    %{
      "name" => name,
      "kind" => kind,
      "producer" => producer,
      "consumers" => consumers,
      "sink" => List.first(sinks)
    }
  end

  # The entry channels in declaration order. The compiled topology keeps them
  # in a map, so the record the builder wrote is what states their order.
  defp entry_order(module, declare) do
    c = Channels.new()
    _ = apply(module, declare, [c])
    c |> Channels.take() |> Map.fetch!(:entry_order) |> Enum.uniq()
  end

  # --- the step vocabulary, mapped to this SDK's builder ---

  @doc false
  # The row's steps, declared on the builder token `channels/1` was handed.
  def declare(c) do
    Enum.reduce(context().steps, %{token: c, handles: %{}}, &step/2)
  end

  defp step("entry(" <> _ = step, acc) do
    name = argument(step)
    %{acc | handles: Map.put(acc.handles, name, Channels.input(acc.token, name))}
  end

  defp step(step, acc) do
    [head | links] = String.split(step, ".")
    dotted(head, links, acc)
  end

  defp dotted("class", [flag], acc) do
    {name, value} = call(flag)
    class_flag(name, value, acc)
  end

  # A procedure knob, read off the palette's own declaration rather than here.
  defp dotted(_head, ["mailbox(" <> _], acc), do: acc

  defp dotted(head, links, acc), do: chain(head, links, acc)

  # One fluent sequence off a handle, its links left to right. Every step opens
  # a fresh chain, because the stored handle carries none.
  defp chain(head, links, acc) do
    {_stream, acc} = Enum.reduce(links, {Map.fetch!(acc.handles, head), acc}, &link/2)
    acc
  end

  defp link(link, {stream, acc}) do
    case call(link) do
      {"map", _} -> {Channels.map(stream, & &1), acc}
      {"key", _} -> {Channels.key_by(stream, & &1), acc}
      {"keyed", _} -> {Channels.keyed(stream), acc}
      {"named", name} -> {Channels.named(stream, name), acc}
      {"sink", name} -> {Channels.sink(stream, name), acc}
      {"retry_max", n} -> {Channels.retry_max(stream, integer(n)), acc}
      {"on_full", policy} -> {Channels.on_full(stream, overflow(policy)), acc}
      {"into", role} -> into(stream, role, acc)
      {other, _} -> raise "the fixture spells a link this interpreter does not map: #{other}"
    end
  end

  # `into` answers the consumer's own output channel, addressed by its role.
  defp into(stream, role, acc) do
    out = Channels.into(stream, procedure(role))
    {out, %{acc | handles: Map.put(acc.handles, role, out)}}
  end

  defp class_flag("persistent", _value, acc), do: acc
  defp class_flag("sync_durability", _value, acc), do: acc

  defp class_flag("effectively_once", _value, acc),
    do: %{acc | token: Channels.effectively_once(acc.token)}

  defp class_flag("checkpoint_ack", _value, acc),
    do: %{acc | token: Channels.checkpoint_ack(acc.token)}

  defp class_flag("checkpoint_every", value, acc),
    do: %{acc | token: Channels.checkpoint_every(acc.token, integer(value))}

  defp class_flag("outputs_at_checkpoint", value, acc),
    do: %{acc | token: Channels.outputs_at_checkpoint(acc.token, integer(value))}

  defp class_flag("correction_window", value, acc),
    do: %{acc | token: Channels.correction_window(acc.token, integer(value))}

  defp class_flag(other, _value, _acc),
    do: raise("the fixture spells a class flag this interpreter does not map: #{other}")

  defp procedure("plain"), do: ElixirRunner.Conformance.PlainProc
  defp procedure("second"), do: ElixirRunner.Conformance.SecondProc
  defp procedure("twin"), do: ElixirRunner.Conformance.TwinProc
  defp procedure("static"), do: ElixirRunner.Conformance.StaticProc
  defp procedure("routed"), do: ElixirRunner.Conformance.RoutedProc
  defp procedure("dynamic"), do: ElixirRunner.Conformance.DynamicProc
  defp procedure("nochange"), do: ElixirRunner.Conformance.NochangeProc
  defp procedure("eventtime"), do: ElixirRunner.Conformance.EventTimeProc

  defp procedure(role),
    do: raise("the fixture names a palette role that is not one: #{role}")

  defp overflow("drop_nothing"), do: :drop_nothing
  defp overflow("drop_newest"), do: :drop_newest
  defp overflow("drop_oldest"), do: :drop_oldest

  # --- the row's context, read by the palette and by the class module ---

  @doc false
  def context, do: Application.get_env(:elixir_runner, :conformance_row, @empty_context)

  @doc false
  # The per-Entity mailbox capacity the row declares for `role`.
  def mailbox_capacity(role),
    do: Map.get(context().mailbox, role, @default_mailbox_capacity)

  # The context is application environment rather than the process dictionary
  # because the boot path builds the topology in the SDK's own process.
  defp put_context(row) do
    steps =
      row.steps |> String.split(";") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))

    Application.put_env(:elixir_runner, :conformance_row, %{
      topology: row.topology,
      steps: steps,
      mailbox: mailboxes(steps),
      sync_durability: "class.sync_durability" in steps
    })
  end

  defp mailboxes(steps) do
    for step <- steps,
        [role, "mailbox(" <> _ = link] <- [String.split(step, ".")],
        into: %{},
        do: {role, integer(argument(link))}
  end

  # --- the fixture ---

  defp rows do
    root = repo_root()

    root
    |> Path.join(@fixture)
    |> File.read!()
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "#")))
    |> Enum.map(&columns/1)
    |> Enum.filter(&(length(&1) == 4))
    |> Enum.map(fn [refusal, topology, steps, needs] ->
      %{refusal: refusal, topology: topology, steps: steps, needs: needs}
    end)
  end

  defp columns(line), do: line |> String.split("|") |> Enum.map(&String.trim/1)

  # The surfaces this builder carries, stated once — in the manifest the seam
  # test reads, so a skip and its justification are the same statement.
  defp surfaces do
    repo_root()
    |> Path.join(@manifest)
    |> File.read!()
    |> Jason.decode!()
    |> Map.fetch!("topology_surfaces")
  end

  # The app runs in its own cwd, and the fixture path is the repository's.
  defp repo_root do
    cwd = File.cwd!()

    root =
      cwd
      |> Stream.iterate(&Path.dirname/1)
      |> Enum.take(8)
      |> Enum.find(&File.exists?(Path.join(&1, @fixture)))

    root || raise "#{@fixture} is the contract and is not readable above #{cwd}"
  end

  # `verb(arg)` -> `{"verb", "arg"}`; a bare word -> `{"word", ""}`.
  defp call(text) do
    case String.split(text, "(", parts: 2) do
      [verb, rest] -> {verb, rest |> String.trim_trailing(")") |> String.trim()}
      [word] -> {word, ""}
    end
  end

  defp argument(text), do: text |> call() |> elem(1)

  defp integer(text), do: String.to_integer(text)
end
