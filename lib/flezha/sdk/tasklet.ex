defmodule Flezha.SDK.Tasklet do
  @moduledoc """
  A tasklet: one processing stage of a relay's graph, run once per
  input and declared with `use`.

      defmodule Echo do
        use Flezha.SDK.Tasklet, name: "echo"

        @impl true
        def run(input, state, ctx) do
          emit(ctx, "echo: " <> input)
          {:ok, state}
        end
      end

  `use` declares this behaviour, defines `name/0` and the zero-arity callbacks
  its options name, imports the ctx functions, and makes available a `state`
  block, which writes `init_state/1` and `state_from_json/1`, and `defchange`,
  the one way to change state (`Flezha.SDK.State`); see `__using__/1`. The
  author writes `run/3` and any other callback that takes arguments, each
  under `@impl true`.

  Callbacks. Mandatory: `name/0`, `init_state/1` (the cell key or nil; `use`
  answers `nil` for a module that declares no state), `run/3` (input, state,
  ctx) returning
  `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`.
  Optional, with defaults: `cell_keys/0` (static keyset, `[]`; a keyset is
  refused as `topology.cells_for_missing` without `cells_for/1`, which names
  the keys an input routes to),
  `mailbox_capacity/0` (1024, env `FLEZHA_MAILBOX_CAPACITY`, range 1..=1_048_576),
  `max_cells/0` (100_000 — the cap on a dynamic node's live Cells; at it
  a new key's input is dropped and counted), `idle_ttl_seconds/0` (nil — a
  dynamic Cell idle past that many whole seconds is evicted destructively by
  its Relay's sweep; nil never evicts). Dynamic typing collapses the Rust
  type tokens — Input/Output/State are plain terms.

  Persistence: a tasklet that exports `apply_change/2` is changeful — its
  state is rebuilt by folding recorded changes. In the `use` form each
  `defchange` generates one fold clause and its recorder; without one the
  tasklet is the reference's `NoChange`: its state is a plain term nothing
  persists. `compact_after_bytes/0` (8 MiB) is the per-Cell WAL
  size at which the controller solicits a snapshot; `state_from_json/1` turns
  a JSON object read back from the controller (a migration, `init_values`)
  into this tasklet's state term, the object itself by default.

  Schema: `schema_version/0` (1) is stamped on every record and resolved per
  record on restore against `min_readable_schema_version/0` (the version
  itself); a record between the floor and the version is handed to
  `migrate_state/2` (a snapshot) or `migrate_change/2` (a change) with the
  stamped version and the DECODED term — this SDK's body codec is the
  language's own — answering `{:ok, term}` at the running version or
  `{:error, reason}`. Both default to refusing, so a floor declared without
  its migration fails at the first old record.

  Time: the clock `Flezha.SDK.Ctx.now/1` serves follows from what the tasklet
  writes — event time when it defines `event_time/1` (what that answers for
  the input), the logical clock (the Cell's durable change seq) when
  `logical_clock/0` answers `true`, the epoch millisecond at dispatch entry
  otherwise. A logical clock on a changeless tasklet is refused at
  registration as `topology.unservable_clock`, and both clocks at once as `topology.two_clocks`,
  which the `use` form refuses at compile time instead. Timers:
  `on_timer/4` (the scheduled timestamp, the kind, the state, a ctx) answers
  what `run/3` answers and is what a timer registered through the ctx fires
  into; `max_timers_per_cell/0` (10_000) bounds a Cell's pending set, and an
  item whose registrations pass it fails, not retried, when the core settles it.

  Saga: `compensate/3` (the outputs the Cell released, the state, a ctx) is
  the undo a steps relay runs, in reverse completion order, when it
  unwinds after partial progress. A tasklet declaring none has nothing to
  undo. A crash-resumed unwind reconstructs completed stages with EMPTY
  outputs — a tasklet's collected output was never durable — so a hook that
  records an undo must do so unconditionally rather than from what it is
  handed.

  ## The reference form

  A module may instead write `@behaviour Flezha.SDK.Tasklet` and every
  callback by hand, `name/0` included, recording a change through its own
  `apply_change/2` and `Flezha.SDK.Ctx.apply/3`. It registers and runs as a
  `use` module does; the `use` form refuses a hand-written `apply_change/2`.
  """

  alias Flezha.SDK.Ctx
  alias Flezha.SDK.Knob

  @type state :: term()
  @type run_result ::
          {:ok, state()} | {:error, term()} | {:error, {:non_retryable, term()}}

  @callback name() :: String.t()
  @callback init_state(cell_key :: String.t() | nil) :: state()
  @callback run(input :: term(), state(), Ctx.t()) :: run_result()
  @callback cell_keys() :: [String.t()]
  @callback cells_for(input :: term()) :: [String.t()]
  @callback mailbox_capacity() :: pos_integer()
  @callback max_cells() :: pos_integer()
  @callback idle_ttl_seconds() :: pos_integer() | nil
  @callback apply_change(state(), change :: term()) :: state()
  @callback compact_after_bytes() :: pos_integer()
  @callback state_from_json(map()) :: state()
  @callback schema_version() :: pos_integer()
  @callback min_readable_schema_version() :: pos_integer()
  @callback migrate_state(from_version :: pos_integer(), old_state :: term()) ::
              {:ok, state()} | {:error, term()}
  @callback migrate_change(from_version :: pos_integer(), old_change :: term()) ::
              {:ok, term()} | {:error, term()}
  @callback logical_clock() :: boolean()
  @callback event_time(input :: term()) :: non_neg_integer()
  @callback on_timer(
              fired_at :: non_neg_integer(),
              kind :: :event_time | :processing_time,
              state(),
              Ctx.t()
            ) :: run_result()
  @callback max_timers_per_cell() :: pos_integer()
  @callback compensate(outputs :: [term()], state(), Ctx.t()) :: run_result()

  @optional_callbacks compensate: 3,
                      cell_keys: 0,
                      cells_for: 1,
                      mailbox_capacity: 0,
                      max_cells: 0,
                      idle_ttl_seconds: 0,
                      apply_change: 2,
                      compact_after_bytes: 0,
                      state_from_json: 1,
                      schema_version: 0,
                      min_readable_schema_version: 0,
                      migrate_state: 2,
                      migrate_change: 2,
                      logical_clock: 0,
                      event_time: 1,
                      on_timer: 4,
                      max_timers_per_cell: 0

  @default_max_cells 100_000
  # In lockstep with `flz_core::persistence::DEFAULT_COMPACT_AFTER_BYTES` and
  # the controller's `:default_compact_after_bytes`.
  @default_compact_after_bytes 8 * 1024 * 1024
  @default_schema_version 1

  # The scalar settings `use` takes, each named after the callback it defines.
  # An option not written defines nothing, so every default stays resolved by
  # `function_exported?` exactly as it is for a hand-written module — the
  # mailbox knob's included.
  @use_options [
    :cell_keys,
    :mailbox_capacity,
    :max_cells,
    :idle_ttl_seconds,
    :compact_after_bytes,
    :schema_version,
    :min_readable_schema_version,
    :logical_clock,
    :max_timers_per_cell
  ]

  # The values two of those options take; any other is refused where it is written.
  @use_values [idle_ttl_seconds: :pos_integer, logical_clock: :boolean]

  # Options and callbacks that no longer exist, each with what replaced it, so
  # a leftover is refused by name rather than silently ignored.
  @retired_options [
    cell_idle_ttl_ms: "`idle_ttl_seconds:`, in whole seconds",
    clock: "`logical_clock: true`, or an `event_time/1` def for event time"
  ]
  @retired_callbacks [
    {:cell_idle_ttl_ms, 0, "`idle_ttl_seconds/0`, in whole seconds"},
    {:clock, 0, "`logical_clock/0` answering true, or `event_time/1` for event time"},
    {:event_time_of, 1, "`event_time/1`"}
  ]

  # The §5 short form's options, taken by the relay macro beside the
  # tasklet's own. A tasklet refuses them.
  @short_form_options [:output, :input, :codec]
  # Everything a short form takes beyond `factory_id` and the class settings:
  # its own options and its tasklet's.
  @short_form_only @short_form_options ++ @use_options

  # `Flezha.SDK.Ctx`'s authoring surface, imported by `use`. `apply/3` is
  # deliberately absent: importing it would shadow `Kernel.apply/3` in every
  # tasklet body. `defchange`'s recorders call it by its full name.
  @ctx_imports [
    emit: 2,
    emit_with_meta: 3,
    emit_keyed: 3,
    emit_keyed_with_meta: 4,
    part: 2,
    now: 1,
    timer_after: 2,
    timer_at_event_time: 2,
    cancel_processing_time_timer: 2,
    cancel_event_time_timer: 2,
    cell_key: 1,
    spawn_steps: 3,
    spawn_steps: 4,
    effect: 3,
    effect: 4,
    complete: 2,
    checkpoint: 1,
    stable_id: 1
  ]

  @doc """
  Write a tasklet as a declaration rather than as nine one-line callbacks.

      use Flezha.SDK.Tasklet, name: "position", max_cells: 50_000

      @impl true
      def run(_input, state, _ctx), do: {:ok, state}

  `name` is required. Every other option is one of the zero-arity callbacks
  above, under its own name, and **only the options written define anything**:
  a module that does not write `mailbox_capacity` exports none, so
  `FLEZHA_MAILBOX_CAPACITY` still decides its mailbox. Callbacks that take
  arguments — `run/3`, `cells_for/1`, `event_time/1`, `on_timer/4`,
  `compensate/3`, `migrate_state/2`, `migrate_change/2` — stay `def`s the
  author writes. An unknown option is a `CompileError` naming it.
  `idle_ttl_seconds` takes a positive integer and `logical_clock` a boolean,
  any other value being a `CompileError`; defining `event_time/1` beside
  `logical_clock: true` is one too, naming both.

  `state do field … end` and `defchange` are available here too; see
  `Flezha.SDK.State`. A module with neither a `state` block nor its own
  `init_state/1` gets `def init_state(_), do: nil`, so a stateless tasklet
  writes only `run/3`. `apply_change/2` is not written by hand here:
  `defchange` generates it.

  `output` and `input` describe a relay, not a tasklet, and are a
  `CompileError` naming the relay macro: the §5 short form is written
  with `use Flezha.SDK.Relay`.

  ## Nesting

  A tasklet nested inside a relay module inherits that module's imports
  lexically, so `use` writes `import Flezha.SDK.Channels, only: []` to take
  `input/2` and `output/2` back — a nested tasklet defining its own `input/0`
  would otherwise clash. A nested tasklet must be written **before** the
  `channels/1` that names it, which is how Elixir aliases resolve.
  """
  defmacro __using__(opts) do
    known = @use_options ++ @short_form_options ++ [:name]

    opts =
      __validate__(
        opts,
        known,
        __MODULE__,
        {:name, "the tasklet's node identity"},
        @use_values
      )

    for {key, _value} <- opts, key in @short_form_options do
      raise CompileError,
        description:
          "`use Flezha.SDK.Tasklet` declares a tasklet; `#{key}:` belongs to a relay: " <>
            "write the short form as `use Flezha.SDK.Relay`"
    end

    callbacks = __callbacks__(opts, @use_options, __MODULE__)
    ctx_imports = @ctx_imports

    quote do
      @behaviour Flezha.SDK.Tasklet
      use Flezha.SDK.State

      import Flezha.SDK.Ctx, only: unquote(ctx_imports)
      # A nested tasklet inherits its relay module's imports; the builder
      # is not part of a tasklet's surface and `input/2` would clash with a
      # tasklet's own `input/0`.
      import Flezha.SDK.Channels, only: []

      @impl true
      def name, do: unquote(Keyword.fetch!(opts, :name))

      unquote_splicing(callbacks)

      @flezha_logical_clock unquote(Keyword.get(opts, :logical_clock, false))
      @before_compile Flezha.SDK.Tasklet
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    refuse_misdeclared(env.module, Module.get_attribute(env.module, :flezha_logical_clock))

    # A stateless tasklet writes only `run/3`: the block's `init_state/1` and
    # the author's own both win over this one. The block's is generated by
    # `Flezha.SDK.State`'s own `@before_compile`, which may not have run yet, so
    # the block is read from the attribute rather than from what is defined.
    has_own? =
      Module.defines?(env.module, {:init_state, 1}) or
        Module.get_attribute(env.module, :flezha_state_block, false)

    unless has_own? do
      quote do
        @impl true
        def init_state(_cell_key), do: nil
      end
    end
  end

  # The option checks all three `use` macros make: a keyword list, a value
  # inside what `values` allows for its option, no retired or unknown option,
  # and the one option that macro requires. `required` is `{key, what_it_is}`,
  # so the refusal says what the author is being asked for.
  @doc false
  def __validate__(opts, known, macro, {required, what}, values \\ []) do
    unless Keyword.keyword?(opts) do
      raise CompileError,
        description: "`use #{inspect(macro)}` takes a keyword list, read #{inspect(opts)}"
    end

    for {key, allowed} <- values,
        {:ok, value} <- [Keyword.fetch(opts, key)],
        not allowed?(allowed, value) do
      raise CompileError,
        description:
          "`use #{inspect(macro)}` has no #{key} #{Macro.to_string(value)}; it takes " <>
            describe(allowed)
    end

    for {key, _value} <- opts, instead = @retired_options[key] do
      refuse("`use #{inspect(macro)}` has no option #{inspect(key)} any more; write #{instead}")
    end

    for {key, _value} <- opts, key not in known do
      raise CompileError,
        description:
          "`use #{inspect(macro)}` has no option #{inspect(key)}; it takes " <>
            Enum.map_join(Enum.sort(known), ", ", &inspect/1)
    end

    unless Keyword.has_key?(opts, required) do
      raise CompileError,
        description: "`use #{inspect(macro)}` requires `#{required}:`, #{what}"
    end

    opts
  end

  defp allowed?(:pos_integer, value), do: is_integer(value) and value > 0
  defp allowed?(:boolean, value), do: is_boolean(value)
  defp allowed?(values, value), do: value in values

  defp describe(:pos_integer), do: "a positive integer"
  defp describe(:boolean), do: "true or false"
  defp describe(values), do: Enum.map_join(values, ", ", &inspect/1)

  # What the module the author wrote declares beside its options. A callback
  # that no longer exists, written with `def` or `defp`, would be ignored
  # without a word. Writing `event_time/1` is what declares event time, so a
  # module that also declares the logical clock has declared two.
  defp refuse_misdeclared(module, logical_clock?) do
    for {fun, arity, instead} <- @retired_callbacks, Module.defines?(module, {fun, arity}) do
      refuse(
        "#{inspect(module)} defines `#{fun}/#{arity}`, which is no longer a tasklet " <>
          "callback; write #{instead}"
      )
    end

    if logical_clock? == true and Module.defines?(module, {:event_time, 1}) do
      refuse(
        "#{inspect(module)} defines `event_time/1`, which declares the event-time clock, " <>
          "and `logical_clock: true`; a tasklet runs on one clock, so write one of them"
      )
    end
  end

  # One zero-arity callback per option written, and nothing for an option that
  # is not. `behaviour` is what each `@impl` names.
  @doc false
  def __callbacks__(opts, known, behaviour) do
    for {key, value} <- opts, key in known do
      quote do
        @impl unquote(behaviour)
        def unquote(key)(), do: unquote(Macro.escape(value))
      end
    end
  end

  # Everything `use` on a relay module emits: the behaviour, the builder
  # import, the required `factory_id`, one callback per option written, and
  # what a short form's `run/3` body is written with (the ctx imports and the
  # `state` block). A short form's own options, and its tasklet's, are kept
  # for `__before_compile_relay__/1`.
  @doc false
  def __relay_using__(opts, use_options, behaviour) do
    opts =
      __validate__(
        opts,
        [:factory_id | @short_form_only ++ use_options],
        behaviour,
        {:factory_id, "the id the controller places by"},
        @use_values
      )

    callbacks = __callbacks__(opts, use_options, behaviour)
    factory_id = Keyword.fetch!(opts, :factory_id)
    short_form = Keyword.take(opts, @short_form_only)
    ctx_imports = @ctx_imports

    quote do
      @behaviour unquote(behaviour)
      use Flezha.SDK.State
      import Flezha.SDK.Channels
      import Flezha.SDK.Ctx, only: unquote(ctx_imports)

      @impl unquote(behaviour)
      def factory_id, do: unquote(factory_id)

      unquote_splicing(callbacks)

      @flezha_relay {unquote(factory_id), unquote(short_form)}
      # After `Flezha.SDK.State`'s hook, so a `state` block's callbacks exist.
      @before_compile {Flezha.SDK.Tasklet, :__before_compile_relay__}
    end
  end

  # A relay module is a short form exactly when it defines `run/3` and no
  # `channels/1`.
  @doc false
  defmacro __before_compile_relay__(env) do
    {factory_id, short_form} = Module.get_attribute(env.module, :flezha_relay)
    run? = Module.defines?(env.module, {:run, 3})
    topology? = Module.defines?(env.module, {:channels, 1})

    cond do
      run? and topology? ->
        refuse(
          "topology.short_form_with_topology: #{inspect(env.module)} defines `run/3`, the short form's " <>
            "tasklet body, and `channels/1`; the short form generates its topology, " <>
            "so write one or the other"
        )

      run? ->
        refuse_misdeclared(env.module, Keyword.get(short_form, :logical_clock))
        short_form(env.module, factory_id, short_form)

      topology? and short_form != [] ->
        refuse(
          "#{inspect(env.module)} writes `channels/1`; `#{elem(hd(short_form), 0)}:` is a " <>
            "short form's option, which a full form states in `channels/1` and its tasklets instead"
        )

      topology? ->
        nil

      true ->
        refuse(
          "#{inspect(env.module)} declares no body: write `run/3` for a single-tasklet " <>
            "relay, or `channels/1`"
        )
    end
  end

  defp refuse(description), do: raise(CompileError, description: description)

  # The short form's expansion: a nested tasklet module named after the
  # factory id, taking the tasklet options written and delegating every
  # tasklet callback the declaring module defines (a `state` block's
  # `init_state/1` is overridable, which `Module.defines?/2` does not report),
  # and the topology body wiring the entry channel into it and, with an `output`,
  # it to that plugin channel.
  defp short_form(module, factory_id, options) do
    tasklet = Module.concat(module, Tasklet)
    entry = Keyword.get(options, :input, "input")
    tasklet_options = Macro.escape([name: factory_id] ++ Keyword.take(options, @use_options))

    delegated =
      for {fun, arity} <- __MODULE__.behaviour_info(:callbacks),
          fun != :name,
          Module.defines?(module, {fun, arity}) or
            {fun, arity} in Module.overridables_in(module) do
        args = Macro.generate_arguments(arity, __MODULE__)

        quote do
          @impl true
          defdelegate unquote(fun)(unquote_splicing(args)), to: unquote(module)
        end
      end

    wired =
      quote do
        c
        |> Flezha.SDK.Channels.input(unquote(entry), unquote(Keyword.take(options, [:codec])))
        |> Flezha.SDK.Channels.into(unquote(tasklet))
      end

    topology =
      case Keyword.fetch(options, :output) do
        {:ok, output} -> quote(do: Flezha.SDK.Channels.output(unquote(wired), unquote(output)))
        :error -> wired
      end

    quote do
      defmodule unquote(tasklet) do
        @moduledoc false
        # Its `apply_change/2` delegates to the author's, which `defchange` wrote.
        @flezha_delegates_changes true
        use Flezha.SDK.Tasklet, unquote(tasklet_options)

        unquote_splicing(delegated)
      end

      @impl true
      def channels(c), do: unquote(topology)
    end
  end

  @doc """
  Whether this tasklet declares a saga undo hook. A steps relay that unwinds
  runs `compensate/3` on every Cell whose tasklet completed forward; a
  tasklet declaring none simply has nothing to undo.
  """
  @spec compensates?(module()) :: boolean()
  def compensates?(module), do: exports?(module, :compensate, 3)

  @doc "Static cell keyset (`[]` unless the tasklet overrides `cell_keys/0`)."
  def cell_keys(module) do
    if exports?(module, :cell_keys, 0), do: module.cell_keys(), else: []
  end

  @doc """
  Per-Cell mailbox capacity as declared: the overridden callback, else the env,
  else 1024. The core refuses a capacity outside its range by name.
  """
  def mailbox_capacity(module) do
    if exports?(module, :mailbox_capacity, 0),
      do: module.mailbox_capacity(),
      else: Knob.get(:mailbox_capacity)
  end

  @doc "Cap on live dynamic cells (default 100_000)."
  def max_cells(module) do
    if exports?(module, :max_cells, 0),
      do: module.max_cells(),
      else: @default_max_cells
  end

  @doc """
  The idle window after which a dynamic Cell is evicted, in milliseconds: the
  declared `idle_ttl_seconds/0` times 1 000; nil (the default) never evicts.
  """
  @spec idle_ttl_ms(module()) :: pos_integer() | nil
  def idle_ttl_ms(module) do
    case exports?(module, :idle_ttl_seconds, 0) and module.idle_ttl_seconds() do
      seconds when is_integer(seconds) and seconds > 0 ->
        seconds * 1_000

      absent when absent in [false, nil] ->
        nil

      other ->
        raise ArgumentError,
              "#{inspect(module)}.idle_ttl_seconds/0 answered #{inspect(other)}; " <>
                "it answers a positive whole number of seconds, or nil"
    end
  end

  @doc "Whether the tasklet journals — it exports `apply_change/2`."
  @spec changeful?(module()) :: boolean()
  def changeful?(module), do: exports?(module, :apply_change, 2)

  @doc "The schema version stamped on the tasklet's records (1 unless declared)."
  @spec schema_version(module()) :: pos_integer()
  def schema_version(module) do
    if exports?(module, :schema_version, 0),
      do: module.schema_version(),
      else: @default_schema_version
  end

  @doc "The oldest schema version the tasklet reads (its own unless declared)."
  @spec min_readable_schema_version(module()) :: pos_integer()
  def min_readable_schema_version(module) do
    if exports?(module, :min_readable_schema_version, 0),
      do: module.min_readable_schema_version(),
      else: schema_version(module)
  end

  @doc "A snapshot stamped `from` as the running version's state; `:undeclared` without a migration."
  @spec migrate_state(module(), pos_integer(), term()) ::
          {:ok, state()} | {:error, term()} | :undeclared
  def migrate_state(module, from, old) do
    if exports?(module, :migrate_state, 2), do: module.migrate_state(from, old), else: :undeclared
  end

  @doc "A change stamped `from` as the running version's change; `:undeclared` without a migration."
  @spec migrate_change(module(), pos_integer(), term()) ::
          {:ok, term()} | {:error, term()} | :undeclared
  def migrate_change(module, from, old) do
    if exports?(module, :migrate_change, 2),
      do: module.migrate_change(from, old),
      else: :undeclared
  end

  @doc """
  The clock the tasklet runs on, from what it writes: `:event_time` when it
  exports `event_time/1`, `:logical` when `logical_clock/0` answers `true`,
  `:wall` otherwise.
  """
  @spec clock(module()) :: :wall | :logical | :event_time
  def clock(module) do
    cond do
      exports?(module, :event_time, 1) -> :event_time
      logical_clock?(module) -> :logical
      true -> :wall
    end
  end

  @doc "Whether the tasklet declares the logical clock — `logical_clock/0` answers `true`."
  @spec logical_clock?(module()) :: boolean()
  def logical_clock?(module),
    do: exports?(module, :logical_clock, 0) and module.logical_clock() == true

  @doc "The retired callbacks a module exports, each with what replaced it."
  @spec retired_callbacks(module()) :: [{String.t(), String.t()}]
  def retired_callbacks(module) do
    for {fun, arity, instead} <- @retired_callbacks,
        exports?(module, fun, arity),
        do: {"#{fun}/#{arity}", instead}
  end

  @doc "Whether the tasklet handles a firing — it exports `on_timer/4`."
  @spec on_timer?(module()) :: boolean()
  def on_timer?(module), do: exports?(module, :on_timer, 4)

  @doc "Cap on a Cell's pending timers, or nil for the core's 10_000."
  @spec max_timers_per_cell(module()) :: pos_integer() | nil
  def max_timers_per_cell(module),
    do: if(exports?(module, :max_timers_per_cell, 0), do: module.max_timers_per_cell())

  @doc "Per-Cell WAL size at which the controller solicits a snapshot (8 MiB unless overridden)."
  @spec compact_after_bytes(module()) :: pos_integer()
  def compact_after_bytes(module) do
    if exports?(module, :compact_after_bytes, 0),
      do: module.compact_after_bytes(),
      else: @default_compact_after_bytes
  end

  @doc "The compaction threshold a class need not declare."
  @spec default_compact_after_bytes() :: pos_integer()
  def default_compact_after_bytes, do: @default_compact_after_bytes

  @doc "A JSON object read back from the controller as this tasklet's state term."
  @spec state_from_json(module(), map()) :: state()
  def state_from_json(module, json) do
    if exports?(module, :state_from_json, 1), do: module.state_from_json(json), else: json
  end

  @doc false
  def exports?(module, fun, arity),
    do: Code.ensure_loaded?(module) and function_exported?(module, fun, arity)
end
