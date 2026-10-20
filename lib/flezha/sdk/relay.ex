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

  The core judges the tree when the class is registered: a setting written
  under a parent that does not carry it is refused as `topology.unknown_field`
  naming the setting and the parent, and a value outside its choices as
  `topology.invalid_setting`, whether `use` or hand-written callbacks
  (`@behaviour Flezha.SDK.Relay`) answer the tree.

  A tasklet of either run ends its instance with `complete/2`: a flow
  instance runs what its Cells already hold to its outputs first.
  `max_duration_ms` is a wall-clock stop for either run.
  """

  alias Flezha.SDK.Tasklet

  # The scalar settings `use` takes, each named after the callback it defines.
  @use_options [:description, :max_duration_ms, :run, :durable]

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
  `CompileError` naming it; the tree itself is judged at registration.

  `Flezha.SDK.ChannelBuilder` is imported, so a whole pipe, a merge included, is
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
  `channels/1` wiring the entry channel (`input:`, `"input"` when absent, read
  by `codec:` as `Flezha.SDK.ChannelBuilder.input/3` reads it) into it and, with
  `output:`, it to that plugin channel. With no `output:` nothing
  leaves the tasklet but its state. `run/3` is written as in a tasklet, with
  the ctx functions and a `state do ... end` block available; the options
  `use Flezha.SDK.Tasklet` takes (`max_cells:`, `logical_clock:`, ...) apply
  to the generated tasklet, and the class settings to the class, as in the
  full form. Writing `run/3` and `channels/1` both is a `CompileError`
  beginning `topology.short_form_with_topology`, and writing neither is one
  naming both.
  """
  defmacro __using__(opts), do: Tasklet.__relay_using__(opts, @use_options, __MODULE__)

  @doc "Stable, controller-facing factory identifier."
  @callback factory_id() :: String.t()

  @doc """
  Declare the graph on the channel builder. A class that runs in steps runs
  it in topological order: each entry channel roots a chain, a message runs
  only the chain of the channel it arrived on, and a tasklet becomes runnable
  once every producer feeding it in that chain has committed.
  """
  @callback channels(Flezha.SDK.ChannelBuilder.t()) :: any()

  @doc "Optional human description."
  @callback description() :: String.t()

  @doc "Wall-clock stop in milliseconds, for either run."
  @callback max_duration_ms() :: pos_integer()

  @doc "How the class runs: `:flow` or `:steps`, with its members."
  @callback run() :: term()

  @doc "Where the class keeps its work: `false`, `:local` or `:remote`, with its members."
  @callback durable() :: term()

  @optional_callbacks description: 0, max_duration_ms: 0, run: 0, durable: 0
end
