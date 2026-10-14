# The row interpreter for `test/fixtures/reference_topologies.txt`.
#
# The palette tasklets below are the fixture's roles bound to this SDK. They
# exist only for the conformance run and are deliberately outside
# `ElixirRunner.Factories.all/0` — a runner never advertises them.

defmodule ElixirRunner.Conformance.PlainTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "plain"

  @impl true
  def run(_input, state, _ctx), do: {:ok, state}
  @impl true
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("plain")
end

defmodule ElixirRunner.Conformance.SecondTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "second"

  @impl true
  def run(_input, state, _ctx), do: {:ok, state}
  @impl true
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("second")
end

defmodule ElixirRunner.Conformance.TwinTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "plain"

  # A distinct module answering the SAME name as `plain`: that IS the collision.
  @impl true
  def run(_input, state, _ctx), do: {:ok, state}
  @impl true
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("twin")
end

defmodule ElixirRunner.Conformance.StaticTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "static"

  # The role is "the static declaration missing its other half". In this SDK
  # `cells_for/1` defaults to the whole keyset, so keys without a router are
  # complete and a router without keys is the incomplete half — the mirror of
  # the reference's, and the shape `topology.cells_for_missing` names here.
  @impl true
  def run(_input, state, _ctx), do: {:ok, state}
  @impl true
  def cells_for(_input), do: ["a"]
  @impl true
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("static")
end

defmodule ElixirRunner.Conformance.RoutedTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "routed", cell_keys: ["a", "b"]

  @impl true
  def run(_input, state, _ctx), do: {:ok, state}
  @impl true
  def cells_for(_input), do: ["a"]
  @impl true
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("routed")
end

defmodule ElixirRunner.Conformance.DynamicTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "dynamic"

  # Dynamic mode is DERIVED here: a keyless tasklet a keyed edge reaches.
  @impl true
  def run(_input, state, _ctx), do: {:ok, state}
  @impl true
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("dynamic")
end

defmodule ElixirRunner.Conformance.NochangeTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "nochange", logical_clock: true

  # A logical clock over a tasklet that journals nothing: no `apply_change/2`.
  @impl true
  def run(_input, state, _ctx), do: {:ok, state}
  @impl true
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("nochange")
end

defmodule ElixirRunner.Conformance.EventTimeTask do
  @moduledoc false
  use Flezha.SDK.Tasklet, name: "eventtime"

  @impl true
  def run(_input, state, _ctx), do: {:ok, state}
  @impl true
  def event_time(_input), do: 0
  @impl true
  def mailbox_capacity, do: ElixirRunner.Conformance.mailbox_capacity("eventtime")
end

defmodule ElixirRunner.Conformance.Row do
  @moduledoc false
  @behaviour Flezha.SDK.Relay

  # The class every interpreted row registers: the steps it declares are the
  # row the interpreter is driving, so the settings tree a row writes comes
  # from the same context.
  def factory_id, do: ElixirRunner.Conformance.context().topology
  def channels(c), do: ElixirRunner.Conformance.declare(c)
  def run, do: ElixirRunner.Conformance.context().run
  def durable, do: ElixirRunner.Conformance.context().durable
end

# The refusal table's `shorthand_duplicate_id` pair: a flow short form and a
# short form that runs in steps under one factory id, which is what proves the
# two runs share one namespace. Nothing but that row registers them.

defmodule ElixirRunner.ShorthandDupFlow do
  @moduledoc false
  use Flezha.SDK.Relay, factory_id: "plain", output: "out"

  def run(input, state, ctx) do
    emit(ctx, input)
    {:ok, state}
  end
end

defmodule ElixirRunner.ShorthandDupSteps do
  @moduledoc false
  use Flezha.SDK.Relay, factory_id: "plain", output: "twin_sink", run: :steps

  def run(input, state, ctx) do
    emit(ctx, input)
    {:ok, state}
  end
end

defmodule ElixirRunner.Conformance do
  @moduledoc """
  The Elixir arm of the refusal table: every row of
  `test/fixtures/reference_topologies.txt` driven through
  `Flezha.SDK.Channels` and judged by `Flezha.SDK.register_factory/2` on one
  class, whose settings tree is the row's `class.run(...)` and
  `class.durable(...)`.

  Two entry points, both in `ElixirRunner.Application`:

    * `--conformance-report <path>` writes the report `docs/sdk/conformance.md`
      § "The refusal table" specifies and exits 0 without connecting;
    * `FLEZHA_REFUSAL_FIXTURE=<topology id>` registers ONLY that row's
      topology at boot, so a refused one exits non-zero naming its refusal.

  One mapping this SDK's surface forces: `<task>.mailbox(n)` is a tasklet
  declaration here, not a builder knob, so the palette reads the row's capacity
  off `mailbox_capacity/1` rather than the handle carrying it.

  `class.run(<tree>)` and `class.durable(<tree>)` carry the tree as JSON; the
  row class's `run/0` and `durable/0` answer it in this SDK's spelling (a
  branch name is an atom, a branch with members `{name, keyword}`), so a
  misplaced member reaches the SDK's own registration check.
  """

  alias ElixirRunner.Conformance.Row
  alias Flezha.SDK.Channels

  @runtime "elixir"
  @fixture "test/fixtures/reference_topologies.txt"
  @manifest "test/apps/elixir_runner/conformance.json"

  # Authorable in no builder — the fixture header says why for each.
  @unauthorable ~w(detached)

  # The two rows the fixture cites rather than re-spells: this app's own
  # fixtures, whose derived lines are the seven-column table at the top.
  @cited %{
    "hybrid_channel" => ElixirRunner.HybridChannelOp,
    "commit_block_chain" => ElixirRunner.Steps.TierCommitBlockSteps
  }

  @default_mailbox_capacity 1024
  @empty_context %{
    topology: "none",
    steps: [],
    mailbox: %{},
    run: nil,
    durable: nil
  }

  # --- the two entry points ---

  @doc "Drive every fixture row and write the conformance report to `path`."
  @spec write_report(String.t()) :: :ok
  def write_report(path) do
    surfaces = surfaces()
    rows = Enum.map(rows(), &report_row(&1, surfaces))
    File.write!(path, Jason.encode!(%{"runtime" => @runtime, "rows" => rows}))
  end

  @doc """
  The class list `FLEZHA_REFUSAL_FIXTURE` selects: only that row's topology. A
  refused row never reaches a connected runner — its refusal names are printed
  and the VM exits non-zero, which is what the drill reads.
  """
  @spec boot_classes(String.t()) :: [module()]
  def boot_classes(topology) do
    row = Enum.find(rows(), &(&1.topology == topology))

    cond do
      row == nil ->
        die("no fixture row is called #{topology}")

      skip_reason(row, surfaces()) != nil ->
        die("row #{topology} needs #{row.needs}, which this builder does not carry")

      shorthand_steps(row) != [] ->
        boot_shorthand(row)

      true ->
        register(row)
    end
  end

  defp register(row) do
    module = class_of(row)
    put_context(row)

    case registered(module) do
      {:ok, _derived} -> [module]
      {:error, issues} -> die(rejection(row.topology, issues))
    end
  end

  # The row's class as the core judges it, settings tree and all; nothing
  # connects.
  defp registered(module), do: Flezha.SDK.Declaration.check(module)

  defp rejection(topology, refusal),
    do: "topology rejected [#{Enum.join(names(refusal), ", ")}] declaring #{topology}"

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
    case shorthand_steps(row) do
      [] -> built(row)
      declarations -> judge_shorthand(row, declarations)
    end
  end

  defp built(row) do
    module = class_of(row)
    put_context(row)

    case registered(module) do
      {:ok, derived} ->
        %{"topology" => row.topology, "outcome" => "accepted", "derived" => channels_of(derived)}

      {:error, refusal} ->
        %{"topology" => row.topology, "outcome" => "refused", "names" => names(refusal)}
    end
  end

  defp class_of(row), do: @cited[row.topology] || Row

  # --- the §5 short form ---
  #
  # Elixir refuses a malformed short form where `use Flezha.SDK.Tasklet`
  # expands, so a module that earns one cannot be compiled and the row is
  # answered by citing the test that compiles it from a string and asserts the
  # `CompileError`. The one runtime refusal, a duplicate factory id, boots for
  # real.

  @macro_test "sdk/elixir/test/flezha/sdk/authoring_macros_test.exs"

  # The `shorthand(<id>|-, <sink>|-, <input>|-)` steps of a row.
  defp shorthand_steps(row) do
    row.steps
    |> String.split(";")
    |> Enum.map(&String.trim/1)
    |> Enum.filter(&String.starts_with?(&1, "shorthand("))
    |> Enum.map(&parse_shorthand/1)
  end

  # Only what a refusal is judged on: `.topology` marks a declaration that also
  # carries a topology body. The `.noout` row needs `shorthand_typed_output`,
  # which this builder does not carry, so it is skipped before it gets here.
  defp parse_shorthand(step) do
    body = String.replace_suffix(step, ".topology", "")

    unless body |> argument() |> String.split(",") |> length() == 3 do
      die("a shorthand step declares three fields: read `#{step}`")
    end

    %{topology?: body != step}
  end

  defp refusal_of(%{topology?: true}), do: "topology.short_form_with_topology"

  defp judge_shorthand(row, [_one]) do
    %{
      "topology" => row.topology,
      "outcome" => "cited",
      "names" => [refusal_of(hd(shorthand_steps(row)))],
      "citation" => @macro_test
    }
  end

  defp judge_shorthand(row, _pair) do
    case drive_duplicate() do
      :ok -> %{"topology" => row.topology, "outcome" => "accepted"}
      {:error, name} -> %{"topology" => row.topology, "outcome" => "refused", "names" => [name]}
    end
  end

  # The duplicate row's pair, registered on a handle that never connects.
  defp drive_duplicate do
    handle =
      "conformance_shorthand"
      |> Flezha.SDK.create_runner()
      |> Flezha.SDK.register_factory(ElixirRunner.ShorthandDupFlow)
      |> Flezha.SDK.register_factory(ElixirRunner.ShorthandDupSteps)

    case handle do
      %Flezha.SDK{} -> :ok
      {:error, %Flezha.SDK.Error{code: code}} -> {:error, code}
    end
  end

  defp boot_shorthand(row) do
    case shorthand_steps(row) do
      [_one] ->
        die(
          "#{row.topology}: #{refusal_of(hd(shorthand_steps(row)))} is a compile error here; " <>
            "pinned by #{@macro_test}"
        )

      _pair ->
        case drive_duplicate() do
          :ok -> die("#{row.topology}: the runner accepted a second relay under one id")
          {:error, name} -> die("#{row.topology}: #{name}")
        end
    end
  end

  # The refusal codes, read off the verdict as data: every issue it lists.
  defp names(%Flezha.SDK.Error{details: %{"issues" => issues}}),
    do: Enum.map(issues, & &1["code"])

  defp names(%Flezha.SDK.Error{code: code}), do: [code]

  # --- the derived declaration ---

  # One entry per channel in declaration order: an entry channel where the row
  # opened it among the producing channels, and every producing channel
  # something terminates on. A producer's output channel nothing closes on is
  # no channel at all, which is what the fixture's seven-column table states.
  # The core spells a consumer `{"tasklet": n, "chain": m}`; the report carries
  # the flat list of tasklet indices, which is what the table pins.
  defp channels_of(derived) do
    for channel <- derived["channels"] do
      %{
        "index" => channel["index"],
        "name" => channel["name"],
        "kind" => channel["kind"],
        "producer" => channel["producer"],
        "consumers" => Enum.map(channel["consumers"], & &1["tasklet"]),
        "sink" => channel["sink"]
      }
    end
  end

  # --- the step vocabulary, mapped to this SDK's builder ---

  @doc false
  # The row's steps, declared on the builder token `channels/1` was handed.
  def declare(c) do
    Enum.reduce(context().steps, %{token: c, handles: %{}}, &step/2)
  end

  # `<x>.schema(<json>)` is an `input` option here, so it is read with the entry.
  defp step("entry(" <> _ = step, acc) do
    name = argument(step)
    schema = Enum.find_value(context().steps, &schema_of(&1, name))
    input = Channels.input(acc.token, name, schema: schema)
    %{acc | handles: Map.put(acc.handles, name, input)}
  end

  # `merge(<task>, <x>…)`: one identity mapping per listed source, a repeated
  # one included, closed on the tasklet, whose output its role addresses.
  defp step("merge(" <> _ = step, acc) do
    [role | sources] = step |> argument() |> String.split(",") |> Enum.map(&String.trim/1)

    out =
      sources
      |> Enum.reduce(Channels.merge(acc.token), fn source, merge ->
        Channels.from(merge, Map.fetch!(acc.handles, source), & &1)
      end)
      |> Channels.into(tasklet(role))

    %{acc | handles: Map.put(acc.handles, role, out)}
  end

  # `branch(<x>, <n>)`: the arms wire nothing, since the row states only how many.
  defp step("branch(" <> _ = step, acc) do
    [source, arms] = step |> argument() |> String.split(",") |> Enum.map(&String.trim/1)
    _ = Channels.branch(Map.fetch!(acc.handles, source), List.duplicate(& &1, integer(arms)))
    acc
  end

  defp step(step, acc) do
    [head | links] = String.split(step, ".")
    dotted(head, links, acc)
  end

  # The class's settings, read off the context by the row class.
  defp dotted("class", [_setting], acc), do: acc

  # A tasklet knob, read off the palette's own declaration rather than here,
  # and an entry's schema, read with its `entry` step.
  defp dotted(_head, ["mailbox(" <> _], acc), do: acc
  defp dotted(_head, ["schema(" <> _], acc), do: acc

  defp dotted(head, links, acc), do: chain(head, links, acc)

  # One fluent sequence off a handle, its links left to right. Every step opens
  # a fresh chain, because the stored handle carries none.
  defp chain(head, links, acc) do
    {_stream, acc} =
      Enum.reduce(links, {Map.fetch!(acc.handles, head), acc}, &link(&1, &2, head))

    acc
  end

  defp link(link, {stream, acc}, head) do
    case call(link) do
      {"into", role} -> into(stream, role, acc)
      {"call" <> _ = verb, argument} -> {call_step(verb, argument, stream, head), acc}
      {verb, argument} -> {stream_link(verb, argument, stream), acc}
    end
  end

  # `<x>.call…(<arg>)`: a 500 ms call whose Response channel feeds the role
  # that produces x, unless the step leaves it unread.
  defp call_step(verb, argument, stream, role) do
    response =
      case verb do
        "call_factory" ->
          Channels.call(stream, factory: argument, deadline_ms: 500)

        "call_factory_nocreate" ->
          Channels.call(stream, factory: argument, deadline_ms: 500, create: false)

        "call_deadline" ->
          Channels.call(stream, "pricing", deadline_ms: integer(argument))

        call when call in ["call", "call_unread"] ->
          Channels.call(stream, argument, deadline_ms: 500)
      end

    if verb != "call_unread", do: Channels.into(response, tasklet(role))
    stream
  end

  # `into` answers the consumer's own output channel, addressed by its role.
  defp into(stream, role, acc) do
    out = Channels.into(stream, tasklet(role))
    {out, %{acc | handles: Map.put(acc.handles, role, out)}}
  end

  defp stream_link("map", _, stream), do: Channels.map(stream, & &1)
  defp stream_link("filter", _, stream), do: Channels.filter(stream, fn _value -> true end)
  defp stream_link("flat_map", _, stream), do: Channels.flat_map(stream, &[&1])
  defp stream_link("key", _, stream), do: Channels.key_by(stream, & &1)
  defp stream_link("keyed", _, stream), do: Channels.keyed(stream)
  defp stream_link("named", name, stream), do: Channels.named(stream, name)
  defp stream_link("output", "", stream), do: Channels.output(stream)
  defp stream_link("output", name, stream), do: Channels.output(stream, name)
  defp stream_link("retry_max", n, stream), do: Channels.retry_max(stream, integer(n))
  defp stream_link("on_full", policy, stream), do: Channels.on_full(stream, overflow(policy))

  defp stream_link(other, _, _stream),
    do: raise("the fixture spells a link this interpreter does not map: #{other}")

  defp tasklet("plain"), do: ElixirRunner.Conformance.PlainTask
  defp tasklet("second"), do: ElixirRunner.Conformance.SecondTask
  defp tasklet("twin"), do: ElixirRunner.Conformance.TwinTask
  defp tasklet("static"), do: ElixirRunner.Conformance.StaticTask
  defp tasklet("routed"), do: ElixirRunner.Conformance.RoutedTask
  defp tasklet("dynamic"), do: ElixirRunner.Conformance.DynamicTask
  defp tasklet("nochange"), do: ElixirRunner.Conformance.NochangeTask
  defp tasklet("eventtime"), do: ElixirRunner.Conformance.EventTimeTask

  defp tasklet(role),
    do: raise("the fixture names a palette role that is not one: #{role}")

  defp overflow("drop_nothing"), do: :drop_nothing
  defp overflow("drop_newest"), do: :drop_newest
  defp overflow("drop_oldest"), do: :drop_oldest

  # --- the row's context, read by the palette and by the class module ---

  @doc false
  def context, do: Application.get_env(:elixir_runner, :conformance_row, @empty_context)

  @doc false
  # The per-Cell mailbox capacity the row declares for `role`.
  def mailbox_capacity(role),
    do: Map.get(context().mailbox, role, @default_mailbox_capacity)

  # The context is application environment rather than the process dictionary
  # because the boot path builds the topology in the SDK's own process.
  defp put_context(row) do
    steps = steps(row)

    Application.put_env(:elixir_runner, :conformance_row, %{
      topology: row.topology,
      steps: steps,
      mailbox: mailboxes(steps),
      run: setting(steps, "run"),
      durable: setting(steps, "durable")
    })
  end

  # `class.<setting>(<json>)` as this SDK spells the tree, `nil` when absent.
  defp setting(steps, name) do
    case Enum.find(steps, &String.starts_with?(&1, "class.#{name}(")) do
      nil -> nil
      step -> step |> argument() |> Jason.decode!() |> tree()
    end
  end

  defp tree(name) when is_binary(name), do: String.to_atom(name)

  defp tree(%{} = branch) when map_size(branch) == 1 do
    [{name, members}] = Map.to_list(branch)
    members = for {key, value} <- members || %{}, do: {String.to_atom(key), tree(value)}
    {if(name == "false", do: false, else: String.to_atom(name)), members}
  end

  defp tree(value), do: value

  defp steps(row),
    do: row.steps |> String.split(";") |> Enum.map(&String.trim/1) |> Enum.reject(&(&1 == ""))

  defp schema_of(step, name) do
    case String.split(step, ".", parts: 2) do
      [^name, "schema(" <> _ = link] -> link |> argument() |> Jason.decode!()
      _other -> nil
    end
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
