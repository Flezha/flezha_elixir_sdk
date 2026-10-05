defmodule Flezha.SDK.Relay do
  @moduledoc """
  A relay class, declared with `use` (see `__using__/1`), which defines
  this behaviour's zero-arity callbacks from its options and imports the channel
  builder. The author writes the graph in `channels/1`, a DAG of Tasklets,
  Channels and Cells, or the one tasklet's `run/3` of the §5 short form.

  One settings tree says how the class runs and where its work is kept:

      use Flezha.SDK.Relay,
        factory_id: "checkout",
        run: {:steps, each: :chained, on_failure: :keep_progress},
        durable: {:local, sleep_after: 60_000},
        description: "reserve, charge, ship",
        max_duration_ms: 600_000

  `run` is one of:

    * `:flow` (the default), or `{:flow, exactly: exactly}`, where `exactly` is
      `:at_least_once` (the default), `:effectively_once` or
      `{:effectively_once, ack: ack, correction_ms: ms, beyond_window: :drop}`
      (`beyond_window` defaults to `:process_as_arrival`), and `ack` is
      `:per_input` (the default), `:checkpoint` or
      `{:checkpoint, every: n, max_delay_ms: ms, outputs: :at_checkpoint}`
      (`outputs` defaults to `:as_produced`);
    * `:steps`, or `{:steps, each: each, on_failure: on_failure}`, where `each`
      is `:once` (the default), `:per_input` or `:chained`, and `on_failure` is
      `:undo_all` (the default) or `:keep_progress`.

  `durable` is `false` (in memory: a crash starts over), `:local` (the
  default, on the controller's disk) or `:remote` (in the remote copy before a
  commit point is acknowledged, for a class that runs in steps), each of the
  last two optionally written `{:local, sleep_after: ms}`: an instance idle
  that long sleeps and wakes on its next message with its Cells restored.
  `sleep_after` defaults by the run: a flow instance never sleeps, a steps
  instance sleeps after six hours; `false` never sleeps.

  A setting written under a parent that does not carry it is a `CompileError`
  naming the setting and the parent. A module that writes the callbacks by
  hand (`@behaviour Flezha.SDK.Relay`) meets the same check at
  registration, as `topology.unknown_field`.

  A tasklet of either run ends its instance with `complete/2`: a flow
  instance runs what its Cells already hold to its outputs first.
  `max_duration_ms` is a wall-clock stop for either run.
  """

  alias Flezha.SDK.{Error, Tasklet}

  # The scalar settings `use` takes, each named after the callback it defines.
  @use_options [:description, :max_duration_ms, :run, :durable]

  @six_hours_ms 6 * 60 * 60 * 1000

  defstruct description: nil,
            max_duration_ms: nil,
            run: :flow,
            exactly: :at_least_once,
            checkpoint: nil,
            correction: nil,
            each: :once,
            on_failure: :undo_all,
            durable: :local,
            sleep_after_ms: nil

  @typedoc """
  The tree, read and checked: the chosen run with its members (the other
  run's stay at their defaults), and the durability level with its sleep
  interval (`nil`: never sleeps).
  """
  @type t :: %__MODULE__{
          description: String.t() | nil,
          max_duration_ms: pos_integer() | nil,
          run: :flow | :steps,
          exactly: :at_least_once | :effectively_once,
          checkpoint:
            nil
            | %{
                every: non_neg_integer() | nil,
                max_delay_ms: non_neg_integer() | nil,
                outputs: :as_produced | :at_checkpoint
              },
          correction:
            nil | %{window_ms: non_neg_integer(), beyond_window: :process_as_arrival | :drop},
          each: :once | :per_input | :chained,
          on_failure: :undo_all | :keep_progress,
          durable: :memory | :local | :remote,
          sleep_after_ms: pos_integer() | nil
        }

  @doc """
  Write a relay as a declaration rather than as one-line callbacks.

      use Flezha.SDK.Relay, factory_id: "market"

      @impl true
      def channels(c) do
        c |> input("trades") |> into(Candle) |> output("candles")
      end

  `factory_id` is required; `description`, `max_duration_ms`, `run` and
  `durable` are optional, and **only the options written define anything**, so
  a default stays where `settings/1` resolves it. `channels/1` takes an
  argument and stays a `def` the author writes. An unknown option is a
  `CompileError` naming it, and so is a setting of the tree written under a
  parent that does not carry it, or a value outside its choices.

  `Flezha.SDK.Channels` is imported, so a whole pipe, a merge included, is
  written with no module prefix.

  ## The §5 short form

  A module that writes its one tasklet's `run/3` instead of `channels/1` is
  a whole single-tasklet relay:

      use Flezha.SDK.Relay, factory_id: "upcase", output: "out", input: "words"

      def run(input, state, ctx) do
        emit(ctx, String.upcase(input))
        {:ok, state}
      end

  It generates the tasklet module `Upcase.Tasklet`, whose `name/0` is the
  factory id and which delegates to this module's tasklet callbacks, and a
  `channels/1` wiring the entry channel (`input:`, `"input"` when absent) into
  it and, with `output:`, it to that plugin channel. With no `output:` nothing
  leaves the tasklet but its state. `run/3` is written as in a tasklet, with
  the ctx functions and a `state do ... end` block available; the options
  `use Flezha.SDK.Tasklet` takes (`max_cells:`, `logical_clock:`, ...) apply
  to the generated tasklet, and the class settings to the class, as in the
  full form. Writing `run/3` and `channels/1` both is a `CompileError`
  beginning `topology.short_form_with_topology`, and writing neither is one
  naming both.
  """
  defmacro __using__(opts) do
    quoted = Tasklet.__relay_using__(opts, @use_options, __MODULE__)

    with {:error, issue} <- parse(Keyword.fetch!(opts, :factory_id), opts) do
      raise CompileError, description: "`use #{inspect(__MODULE__)}`: " <> message(issue)
    end

    quoted
  end

  @doc "Stable, controller-facing factory identifier."
  @callback factory_id() :: String.t()

  @doc """
  Declare the graph on the channel builder. A class that runs in steps runs
  it in topological order: each entry channel roots a chain, a message runs
  only the chain of the channel it arrived on, and a tasklet becomes runnable
  once every producer feeding it in that chain has committed.
  """
  @callback channels(Flezha.SDK.Channels.t()) :: any()

  @doc "Optional human description."
  @callback description() :: String.t()

  @doc "Wall-clock stop in milliseconds, for either run."
  @callback max_duration_ms() :: pos_integer()

  @doc "How the class runs: `:flow` or `:steps`, with its members."
  @callback run() :: term()

  @doc "Where the class keeps its work: `false`, `:local` or `:remote`, with its members."
  @callback durable() :: term()

  @optional_callbacks description: 0, max_duration_ms: 0, run: 0, durable: 0

  @doc """
  The settings tree `module` declares, read and checked, or the issue that
  refuses it: `{:UnknownField, declaration, fields}` for a member written
  under a parent that does not carry it, `{:InvalidSetting, declaration,
  setting, value, expected}` for a value outside its choices.
  """
  @spec settings(module()) :: {:ok, t()} | {:error, tuple()}
  def settings(module) do
    values =
      for key <- @use_options, Tasklet.exports?(module, key, 0), do: {key, apply(module, key, [])}

    parse(module.factory_id(), values)
  end

  @doc false
  # The tree as `use` options or callback answers, read into `t()`.
  @spec parse(term(), keyword()) :: {:ok, t()} | {:error, tuple()}
  def parse(factory_id, values) do
    with {:ok, settings} <- class(values),
         {:ok, settings} <- run(Keyword.get(values, :run), settings),
         {:ok, settings} <- durable(Keyword.get(values, :durable), settings) do
      {:ok, settle_sleep(settings)}
    else
      {:error, {:unknown, parent, fields}} ->
        {:error, {:UnknownField, "'#{parent}' of relay '#{factory_id}'", fields}}

      {:error, {:invalid, setting, value, expected}} ->
        {:error, {:InvalidSetting, "relay '#{factory_id}'", setting, value, expected}}
    end
  end

  @doc """
  The effectively-once tier the tree declares, in the shape every Cell and
  the topology read: whether it is on, the checkpoint policy and the
  correction policy.
  """
  @spec tier(t()) :: %{
          effectively_once: boolean(),
          checkpoint: map() | nil,
          correction: map() | nil
        }
  def tier(%__MODULE__{} = s),
    do: %{
      effectively_once: s.exactly == :effectively_once,
      checkpoint: s.checkpoint,
      correction: s.correction
    }

  # --- the class members ---

  defp class(values) do
    with {:ok, description} <- optional(values, :description, &is_binary/1, "a string"),
         {:ok, max} <-
           optional(
             values,
             :max_duration_ms,
             &pos_integer?/1,
             "a positive number of milliseconds"
           ) do
      {:ok, %__MODULE__{description: description, max_duration_ms: max}}
    end
  end

  defp optional(values, key, valid?, expected) do
    case Keyword.get(values, key) do
      nil -> {:ok, nil}
      value -> if valid?.(value), do: {:ok, value}, else: invalid(key, value, expected)
    end
  end

  # --- run ---

  defp run(nil, settings), do: {:ok, settings}

  defp run(value, settings) do
    case branch(:run, value, [:flow, :steps]) do
      {:ok, {:flow, members}} -> flow(members, settings)
      {:ok, {:steps, members}} -> steps(members, settings)
      error -> error
    end
  end

  defp flow(members, settings) do
    with :ok <- only(:flow, members, [:exactly]),
         {:ok, settings} <- exactly(Keyword.get(members, :exactly), settings),
         do: {:ok, %{settings | run: :flow}}
  end

  defp steps(members, settings) do
    with :ok <- only(:steps, members, [:each, :on_failure]),
         {:ok, each} <-
           named(:each, Keyword.get(members, :each), [:once, :per_input, :chained], :once),
         {:ok, on_failure} <-
           named(
             :on_failure,
             Keyword.get(members, :on_failure),
             [:undo_all, :keep_progress],
             :undo_all
           ),
         do: {:ok, %{settings | run: :steps, each: each, on_failure: on_failure}}
  end

  defp exactly(nil, settings), do: {:ok, settings}

  defp exactly(value, settings) do
    case branch(:exactly, value, [:at_least_once, :effectively_once]) do
      {:ok, {:at_least_once, members}} ->
        with :ok <- only(:at_least_once, members, []), do: {:ok, settings}

      {:ok, {:effectively_once, members}} ->
        effectively_once(members, settings)

      error ->
        error
    end
  end

  defp effectively_once(members, settings) do
    with :ok <- only(:effectively_once, members, [:ack, :correction_ms, :beyond_window]),
         {:ok, checkpoint} <- ack(Keyword.get(members, :ack)),
         {:ok, window} <- integer(:correction_ms, Keyword.get(members, :correction_ms)),
         {:ok, beyond} <-
           named(
             :beyond_window,
             Keyword.get(members, :beyond_window),
             [:process_as_arrival, :drop],
             :process_as_arrival
           ) do
      {:ok,
       %{
         settings
         | exactly: :effectively_once,
           checkpoint: checkpoint,
           correction: correction(window, beyond)
       }}
    end
  end

  # A beyond-window fate with no window is a window of zero, which is refused:
  # a fate for a window nobody declared is an authoring error, not a default.
  defp correction(nil, :process_as_arrival), do: nil
  defp correction(window, beyond), do: %{window_ms: window || 0, beyond_window: beyond}

  defp ack(nil), do: {:ok, nil}

  defp ack(value) do
    case branch(:ack, value, [:per_input, :checkpoint]) do
      {:ok, {:per_input, members}} ->
        with :ok <- only(:per_input, members, []), do: {:ok, nil}

      {:ok, {:checkpoint, members}} ->
        checkpoint(members)

      error ->
        error
    end
  end

  defp checkpoint(members) do
    with :ok <- only(:checkpoint, members, [:every, :max_delay_ms, :outputs]),
         {:ok, every} <- integer(:every, Keyword.get(members, :every)),
         {:ok, delay} <- integer(:max_delay_ms, Keyword.get(members, :max_delay_ms)),
         {:ok, outputs} <-
           named(
             :outputs,
             Keyword.get(members, :outputs),
             [:as_produced, :at_checkpoint],
             :as_produced
           ),
         do: {:ok, %{every: every, max_delay_ms: delay, outputs: outputs}}
  end

  # --- durable ---

  defp durable(nil, settings), do: {:ok, %{settings | sleep_after_ms: :run_default}}
  defp durable(false, settings), do: {:ok, %{settings | durable: :memory, sleep_after_ms: nil}}

  defp durable(value, settings) do
    case branch(:durable, value, [false, :local, :remote]) do
      {:ok, {false, members}} ->
        with :ok <- only(false, members, []),
             do: {:ok, %{settings | durable: :memory, sleep_after_ms: nil}}

      {:ok, {level, members}} ->
        with :ok <- only(level, members, [:sleep_after]),
             {:ok, after_ms} <- sleep_after(Keyword.fetch(members, :sleep_after)),
             do: {:ok, %{settings | durable: level, sleep_after_ms: after_ms}}

      error ->
        error
    end
  end

  defp sleep_after(:error), do: {:ok, :run_default}
  defp sleep_after({:ok, false}), do: {:ok, nil}
  defp sleep_after({:ok, ms}) when is_integer(ms) and ms > 0, do: {:ok, ms}

  defp sleep_after({:ok, other}),
    do: invalid(:sleep_after, other, "a positive number of milliseconds or false")

  # A sleeping flow instance has its input journaled before it is forwarded,
  # so a flow class pays for it only when it asks to sleep.
  defp settle_sleep(%{sleep_after_ms: :run_default, run: run} = settings),
    do: %{settings | sleep_after_ms: if(run == :steps, do: @six_hours_ms)}

  defp settle_sleep(settings), do: settings

  # --- the shapes every "one of" is written in ---

  # A branch as the tree writes it: a bare name, or `{name, members}` with the
  # members a keyword list.
  defp branch(setting, value, names) do
    case value do
      name when is_atom(name) ->
        choose(setting, name, [], names)

      {name, members} when is_atom(name) and is_list(members) ->
        choose(setting, name, members, names)

      other ->
        invalid(setting, other, expected(names))
    end
  end

  defp choose(setting, name, members, names) do
    cond do
      name not in names -> invalid(setting, name, expected(names))
      not Keyword.keyword?(members) -> invalid(name, members, "a keyword list")
      true -> {:ok, {name, members}}
    end
  end

  # Any member of `parent` outside `known` is written under the wrong parent.
  defp only(parent, members, known) do
    case for({key, _value} <- members, key not in known, do: key) |> Enum.uniq() do
      [] -> :ok
      unknown -> {:error, {:unknown, shown(parent), Enum.map_join(unknown, ", ", &shown/1)}}
    end
  end

  # A leaf choice: a bare name of `names`, `default` when absent.
  defp named(_setting, nil, _names, default), do: {:ok, default}

  defp named(setting, value, names, _default) do
    with {:ok, {name, members}} <- branch(setting, value, names),
         :ok <- only(name, members, []),
         do: {:ok, name}
  end

  defp integer(_setting, nil), do: {:ok, nil}
  defp integer(_setting, n) when is_integer(n) and n >= 0, do: {:ok, n}
  defp integer(setting, other), do: invalid(setting, other, "a non-negative integer")

  defp invalid(setting, value, expected),
    do: {:error, {:invalid, shown(setting), shown(value), expected}}

  defp expected(names), do: Enum.map_join(names, " | ", &shown/1)

  defp shown(value) when is_atom(value), do: Atom.to_string(value)
  defp shown(value), do: inspect(value)

  defp pos_integer?(value), do: is_integer(value) and value > 0

  defp message({:UnknownField, declaration, fields}),
    do: Error.new("topology.unknown_field", declaration: declaration, fields: fields).message

  defp message({:InvalidSetting, declaration, setting, value, expected}) do
    Error.new("topology.invalid_setting",
      declaration: declaration,
      setting: setting,
      value: value,
      expected: expected
    ).message
  end
end
