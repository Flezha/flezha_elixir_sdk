defmodule Flezha.SDK.Procedure do
  @moduledoc """
  Behaviour an Elixir procedure implements. Mandatory: `name/0`, `init_state/1`
  (the entity key or nil), `run/3` (input, state, ctx) returning
  `{:ok, new_state} | {:error, reason} | {:error, {:non_retryable, reason}}`.
  Optional, with defaults: `entity_keys/0` (static keyset, `[]`), `entities_for/1`,
  `mailbox_capacity/0` (1024, env `FLEZHA_MAILBOX_CAPACITY`, range 1..=1_048_576),
  `max_entities/0` (100_000 — the cap on a dynamic node's live Entities; at it
  a new key's input is dropped and counted), `entity_idle_ttl_ms/0` (nil — a
  dynamic Entity idle past it is evicted destructively by its Operative's
  sweep; nil never evicts). Dynamic typing collapses the Rust type tokens —
  Input/Output/State are plain terms.

  Persistence: a procedure that exports `apply_change/2` is changeful — its
  state is rebuilt by folding recorded changes, and `Flezha.SDK.Ctx.apply/3` is
  the one path that records one (the twin of the Rust `StateHandle::apply`).
  Without it the procedure is the reference's `NoChange`: its state is a plain
  term nothing persists. `compact_after_bytes/0` (8 MiB) is the per-Entity WAL
  size at which the controller solicits a snapshot; `state_from_json/1` turns
  a JSON object read back from the controller (a migration, `init_values`)
  into this procedure's state term, the object itself by default.

  Schema: `schema_version/0` (1) is stamped on every record and resolved per
  record on restore against `min_readable_schema_version/0` (the version
  itself); a record between the floor and the version is handed to
  `migrate_state/2` (a snapshot) or `migrate_change/2` (a change) with the
  stamped version and the DECODED term — this SDK's body codec is the
  language's own — answering `{:ok, term}` at the running version or
  `{:error, reason}`. Both default to refusing, so a floor declared without
  its migration fails at the first old record.

  Time: `clock/0` (`:wall`) names what `Flezha.SDK.Ctx.now/1` serves — the
  epoch millisecond at dispatch entry, `:logical` (the Entity's durable change
  seq) or `:event_time` (what `event_time_of/1` extracts from the input; a
  `:logical` clock on a procedure without `apply_change/2` and an `:event_time`
  clock without its extractor are refused at registration). Timers:
  `on_timer/4` (the scheduled timestamp, the kind, the state, a ctx) answers
  what `run/3` answers and is what a timer registered through the ctx fires
  into; `max_timers_per_entity/0` (10_000) bounds an Entity's pending set.

  Saga: `compensate/3` (the outputs the Entity released, the state, a ctx) is
  the undo a workflow operative runs, in reverse completion order, when it
  unwinds after partial progress. A procedure declaring none has nothing to
  undo. A crash-resumed unwind reconstructs completed stages with EMPTY
  outputs — a procedure's collected output was never durable — so a hook that
  records an undo must do so unconditionally rather than from what it is
  handed.
  """

  alias Flezha.SDK.Ctx
  alias Flezha.SDK.Knob

  @type state :: term()
  @type run_result ::
          {:ok, state()} | {:error, term()} | {:error, {:non_retryable, term()}}

  @callback name() :: String.t()
  @callback init_state(entity_key :: String.t() | nil) :: state()
  @callback run(input :: term(), state(), Ctx.t()) :: run_result()
  @callback entity_keys() :: [String.t()]
  @callback entities_for(input :: term()) :: [String.t()]
  @callback mailbox_capacity() :: pos_integer()
  @callback max_entities() :: pos_integer()
  @callback entity_idle_ttl_ms() :: pos_integer() | nil
  @callback apply_change(state(), change :: term()) :: state()
  @callback compact_after_bytes() :: pos_integer()
  @callback state_from_json(map()) :: state()
  @callback schema_version() :: pos_integer()
  @callback min_readable_schema_version() :: pos_integer()
  @callback migrate_state(from_version :: pos_integer(), old_state :: term()) ::
              {:ok, state()} | {:error, term()}
  @callback migrate_change(from_version :: pos_integer(), old_change :: term()) ::
              {:ok, term()} | {:error, term()}
  @callback clock() :: :wall | :logical | :event_time
  @callback event_time_of(input :: term()) :: non_neg_integer()
  @callback on_timer(
              fired_at :: non_neg_integer(),
              kind :: :event_time | :processing_time,
              state(),
              Ctx.t()
            ) :: run_result()
  @callback max_timers_per_entity() :: pos_integer()
  @callback compensate(outputs :: [term()], state(), Ctx.t()) :: run_result()

  @optional_callbacks compensate: 3,
                      entity_keys: 0,
                      entities_for: 1,
                      mailbox_capacity: 0,
                      max_entities: 0,
                      entity_idle_ttl_ms: 0,
                      apply_change: 2,
                      compact_after_bytes: 0,
                      state_from_json: 1,
                      schema_version: 0,
                      min_readable_schema_version: 0,
                      migrate_state: 2,
                      migrate_change: 2,
                      clock: 0,
                      event_time_of: 1,
                      on_timer: 4,
                      max_timers_per_entity: 0

  @default_mailbox_capacity 1024
  @max_mailbox_capacity 1_048_576
  @default_max_entities 100_000
  # In lockstep with `flz_core::persistence::DEFAULT_COMPACT_AFTER_BYTES` and
  # the controller's `:default_compact_after_bytes`.
  @default_compact_after_bytes 8 * 1024 * 1024
  @default_schema_version 1
  # In lockstep with the reference's `DEFAULT_MAX_TIMERS_PER_ENTITY`.
  @default_max_timers_per_entity 10_000

  # The scalar settings `use` takes, each named after the callback it defines.
  # An option not written defines nothing, so every default stays resolved by
  # `function_exported?` exactly as it is for a hand-written module — the
  # mailbox knob's included.
  @use_options [
    :entity_keys,
    :mailbox_capacity,
    :max_entities,
    :entity_idle_ttl_ms,
    :compact_after_bytes,
    :schema_version,
    :min_readable_schema_version,
    :clock,
    :max_timers_per_entity
  ]

  # The §5 short form's options. `sink` is what makes one; `kind` and `input`
  # describe the operative it generates.
  @short_form_options [:kind, :sink, :input]

  # `Flezha.SDK.Ctx`'s authoring surface, imported by `use`. `apply/3` is
  # deliberately absent: importing it would shadow `Kernel.apply/3` in every
  # procedure body. A `state` block's recorders call it by its full name.
  @ctx_imports [
    emit: 2,
    emit_with_meta: 3,
    emit_keyed: 3,
    emit_keyed_with_meta: 4,
    now: 1,
    timer_after: 2,
    timer_at_event_time: 2,
    cancel_processing_time_timer: 2,
    cancel_event_time_timer: 2,
    entity_key: 1,
    spawn_workflow: 3,
    spawn_workflow: 4,
    checkpoint: 1,
    stable_id: 1
  ]

  @doc """
  Write a procedure as a declaration rather than as nine one-line callbacks.

      use Flezha.SDK.Procedure, name: "position", max_entities: 50_000

      def run(input, state, ctx), do: {:ok, state}

  `name` is required. Every other option is one of the zero-arity callbacks
  above, under its own name, and **only the options written define anything**:
  a module that does not write `mailbox_capacity` exports none, so
  `FLEZHA_MAILBOX_CAPACITY` still decides its mailbox. Callbacks that take
  arguments — `run/3`, `entities_for/1`, `event_time_of/1`, `on_timer/4`,
  `compensate/3`, `migrate_state/2`, `migrate_change/2` — stay `def`s the
  author writes. An unknown option is a `CompileError` naming it.

  `state do field … end` and `defchange` are available here too; see
  `Flezha.SDK.State`. A module with neither a `state` block nor its own
  `init_state/1` gets `def init_state(_), do: nil`, so a stateless procedure
  writes only `run/3`.

  ## The §5 short form

  With `sink`, this one procedure is a whole operative of the written `kind`:

      use Flezha.SDK.Procedure, name: "upcase", kind: :stream, sink: "out"

  The module then also implements `Flezha.SDK.StreamOperative` (or
  `Flezha.SDK.WorkflowOperative` for `kind: :workflow`), wiring an entry
  channel named `"input"` — or the `input` option's value — into itself and
  itself to the `sink` plugin channel, with `factory_id/0` equal to `name` and
  every other operative callback left undefined so its default applies. It is
  registered as any operative is: listed under `operatives:` in the child spec,
  or passed to `Flezha.SDK.register_factory/2`.

  Three refusals are `CompileError`s whose message begins with the contract's
  name for them: `SinkWithoutId` for a `sink` with no `name`, `SinkWithoutKind`
  for a `sink` with no `kind`, and `ShorthandKeyWithoutSink` for a `kind` or
  `input` with no `sink`. Elixir cannot see a procedure's output type, so it
  owes no `SinkWithoutOutput` and its manifest does not list
  `shorthand_typed_output`.

  A workflow short form started by a spawn does not see its payload yet; that
  is `KB-381`, not a property of the short form.

  ## Nesting

  A procedure nested inside an operative module inherits that module's imports
  lexically, so `use` writes `import Flezha.SDK.Channels, only: []` to take
  `input/2` and `sink/2` back — a nested procedure defining its own `input/0`
  would otherwise clash. A nested procedure must be written **before** the
  `channels/1` that names it, which is how Elixir aliases resolve.
  """
  defmacro __using__(opts) do
    known = @use_options ++ @short_form_options ++ [:name]

    opts =
      __validate__(opts, known, __MODULE__, {:name, "the procedure's node identity"})

    callbacks = __callbacks__(opts, @use_options, __MODULE__)
    short_form = short_form(opts)
    ctx_imports = @ctx_imports

    quote do
      @behaviour Flezha.SDK.Procedure
      use Flezha.SDK.State

      import Flezha.SDK.Ctx, only: unquote(ctx_imports)
      # A nested procedure inherits its operative module's imports; the builder
      # is not part of a procedure's surface and `input/2` would clash with a
      # procedure's own `input/0`.
      import Flezha.SDK.Channels, only: []

      @impl true
      def name, do: unquote(Keyword.fetch!(opts, :name))

      unquote_splicing(callbacks)
      unquote(short_form)

      @before_compile Flezha.SDK.Procedure
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    # A stateless procedure writes only `run/3`: the block's `init_state/1` and
    # the author's own both win over this one. The block's is generated by
    # `Flezha.SDK.State`'s own `@before_compile`, which may not have run yet, so
    # the block is read from the attribute rather than from what is defined.
    has_own? =
      Module.defines?(env.module, {:init_state, 1}) or
        Module.get_attribute(env.module, :flezha_state_block, false)

    unless has_own? do
      quote do
        @impl true
        def init_state(_entity_key), do: nil
      end
    end
  end

  # The option checks all three `use` macros make: a keyword list, no unknown
  # option, and the one option that macro requires. `required` is
  # `{key, what_it_is}`, so the refusal says what the author is being asked for.
  @doc false
  def __validate__(opts, known, macro, {required, what}) do
    unless Keyword.keyword?(opts) do
      raise CompileError,
        description: "`use #{inspect(macro)}` takes a keyword list, read #{inspect(opts)}"
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

  # One zero-arity callback per option written, and nothing for an option that
  # is not. `behaviour` is what each `@impl` names, because a short-form module
  # implements two.
  @doc false
  def __callbacks__(opts, known, behaviour) do
    for {key, value} <- opts, key in known do
      quote do
        @impl unquote(behaviour)
        def unquote(key)(), do: unquote(Macro.escape(value))
      end
    end
  end

  # Everything a `sink` decides: which operative behaviour the module gains, or
  # which refusal the declaration earns.
  defp short_form(opts) do
    sink = Keyword.get(opts, :sink)
    kind = Keyword.get(opts, :kind)
    name = Keyword.fetch!(opts, :name)
    entry = Keyword.get(opts, :input, "input")

    cond do
      is_nil(sink) ->
        refuse_keys_without_sink(opts)
        nil

      name in [nil, ""] ->
        raise CompileError,
          description:
            "SinkWithoutId: a `sink` makes this module a whole operative, whose factory id is " <>
              "an address an operator types; write it as `name:`"

      kind not in [:stream, :workflow] ->
        raise CompileError,
          description:
            "SinkWithoutKind: a `sink` makes this module a whole operative; write " <>
              "`kind: :stream` or `kind: :workflow`"

      true ->
        operative(kind, name, entry, sink)
    end
  end

  defp refuse_keys_without_sink(opts) do
    keys = for key <- [:kind, :input], Keyword.has_key?(opts, key), do: key

    if keys != [] do
      raise CompileError,
        description:
          "ShorthandKeyWithoutSink: #{Enum.map_join(keys, ", ", &"`#{&1}`")} name(s) of the " <>
            "operative a `sink` generates; this module declares no `sink`, so there is nothing " <>
            "for them to describe"
    end
  end

  # The operative the short form generates: one entry channel into this module,
  # this module to one plugin sink, and every other operative callback left
  # undefined so its default applies.
  defp operative(kind, name, entry, sink) do
    {behaviour, declaration} =
      case kind do
        :stream -> {Flezha.SDK.StreamOperative, :channels}
        :workflow -> {Flezha.SDK.WorkflowOperative, :procedures}
      end

    quote do
      @behaviour unquote(behaviour)

      @impl unquote(behaviour)
      def factory_id, do: unquote(name)

      @impl unquote(behaviour)
      def unquote(declaration)(c) do
        c
        |> Flezha.SDK.Channels.input(unquote(entry))
        |> Flezha.SDK.Channels.into(__MODULE__)
        |> Flezha.SDK.Channels.sink(unquote(sink))
      end
    end
  end

  @doc """
  Whether this procedure declares a saga undo hook. A workflow that unwinds
  runs `compensate/3` on every Entity whose procedure completed forward; a
  procedure declaring none simply has nothing to undo.
  """
  @spec compensates?(module()) :: boolean()
  def compensates?(module), do: exports?(module, :compensate, 3)

  @doc "Static entity keyset (`[]` unless the procedure overrides `entity_keys/0`)."
  def entity_keys(module) do
    if exports?(module, :entity_keys, 0), do: module.entity_keys(), else: []
  end

  @doc "Which static entities an input targets (defaults to the full keyset)."
  def entities_for(module, input) do
    if exports?(module, :entities_for, 1),
      do: module.entities_for(input),
      else: entity_keys(module)
  end

  @doc "Per-Entity mailbox capacity. Overridden callback wins; else the env; else 1024."
  def mailbox_capacity(module) do
    if exports?(module, :mailbox_capacity, 0) do
      validate_capacity!(module.mailbox_capacity(), module)
    else
      validate_capacity!(Knob.get(:mailbox_capacity), module)
    end
  end

  @doc "Cap on live dynamic entities (default 100_000)."
  def max_entities(module) do
    if exports?(module, :max_entities, 0),
      do: module.max_entities(),
      else: @default_max_entities
  end

  @doc "Idle window after which a dynamic Entity is evicted; nil (the default) never evicts."
  @spec entity_idle_ttl_ms(module()) :: pos_integer() | nil
  def entity_idle_ttl_ms(module) do
    if exports?(module, :entity_idle_ttl_ms, 0), do: module.entity_idle_ttl_ms(), else: nil
  end

  @doc "Whether the procedure journals — it exports `apply_change/2`."
  @spec changeful?(module()) :: boolean()
  def changeful?(module), do: exports?(module, :apply_change, 2)

  @doc "The schema version stamped on the procedure's records (1 unless declared)."
  @spec schema_version(module()) :: pos_integer()
  def schema_version(module) do
    if exports?(module, :schema_version, 0),
      do: module.schema_version(),
      else: @default_schema_version
  end

  @doc "The oldest schema version the procedure reads (its own unless declared)."
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

  @doc "The declared clock (`:wall` unless declared)."
  @spec clock(module()) :: atom()
  def clock(module) do
    if exports?(module, :clock, 0), do: module.clock(), else: :wall
  end

  @doc "Whether the procedure handles a firing — it exports `on_timer/4`."
  @spec on_timer?(module()) :: boolean()
  def on_timer?(module), do: exports?(module, :on_timer, 4)

  @doc "Cap on an Entity's pending timers (10_000 unless declared)."
  @spec max_timers_per_entity(module()) :: pos_integer()
  def max_timers_per_entity(module) do
    if exports?(module, :max_timers_per_entity, 0),
      do: module.max_timers_per_entity(),
      else: @default_max_timers_per_entity
  end

  @doc "What the procedure declares about its timers, resolved once: the name a refusal reports, the cap, the clock."
  @spec timer_policy(module()) :: Flezha.SDK.Timers.policy()
  def timer_policy(module),
    do: %{procedure: module.name(), cap: max_timers_per_entity(module), clock: clock(module)}

  @doc "Per-Entity WAL size at which the controller solicits a snapshot (8 MiB unless overridden)."
  @spec compact_after_bytes(module()) :: pos_integer()
  def compact_after_bytes(module) do
    if exports?(module, :compact_after_bytes, 0),
      do: module.compact_after_bytes(),
      else: @default_compact_after_bytes
  end

  @doc "The compaction threshold a class need not declare."
  @spec default_compact_after_bytes() :: pos_integer()
  def default_compact_after_bytes, do: @default_compact_after_bytes

  @doc "A JSON object read back from the controller as this procedure's state term."
  @spec state_from_json(module(), map()) :: state()
  def state_from_json(module, json) do
    if exports?(module, :state_from_json, 1), do: module.state_from_json(json), else: json
  end

  @doc false
  def exports?(module, fun, arity),
    do: Code.ensure_loaded?(module) and function_exported?(module, fun, arity)

  @doc "Range-check a mailbox capacity — returns `{issue, procedure}` on violation."
  @spec validate_capacity(pos_integer(), module()) :: :ok | {atom(), module()}
  def validate_capacity(cap, module) when is_integer(cap) do
    cond do
      cap <= 0 -> {:MailboxCapacityZero, module}
      cap > @max_mailbox_capacity -> {:MailboxCapacityTooLarge, module}
      true -> :ok
    end
  end

  def validate_capacity(_cap, module), do: {:MailboxCapacityZero, module}

  defp validate_capacity!(cap, module) do
    case validate_capacity(cap, module) do
      :ok -> cap
      {:MailboxCapacityZero, _} -> @default_mailbox_capacity
      {:MailboxCapacityTooLarge, _} -> @max_mailbox_capacity
    end
  end
end
