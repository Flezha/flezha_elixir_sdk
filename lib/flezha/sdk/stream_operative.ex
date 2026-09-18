defmodule Flezha.SDK.StreamOperative do
  @moduledoc """
  A stream operative, declared with `use` (see `__using__/1`), which defines
  this behaviour's zero-arity callbacks from its options and imports the channel
  builder. The callbacks: a `factory_id/0` the controller places by, a
  `channels/1` the author writes, which authors the topology on the builder
  token, an optional human `description/0`, and an optional
  `persistence_enabled/0` (default `true`, the Rust `PERSISTENCE_ENABLED`):
  `false` advances every changeful Entity's record seq and ships nothing, so a
  class toggled back on later never hands the controller a seq it already saw.
  """

  alias Flezha.SDK.Procedure

  # The scalar settings `use` takes, each named after the callback it defines.
  @use_options [:description, :persistence_enabled]

  @doc """
  Write a stream operative as a declaration rather than as one-line callbacks.

      use Flezha.SDK.StreamOperative, factory_id: "market"

      @impl true
      def channels(c) do
        c |> input("trades") |> into(Candle) |> sink("candles")
      end

  `factory_id` is required; `description` and `persistence_enabled` are
  optional, and **only the options written define anything**, so a default
  stays where `function_exported?` resolves it. `channels/1` takes arguments
  and stays a `def` the author writes. An unknown option is a `CompileError`
  naming it.

  `Flezha.SDK.Channels` is imported, so a whole pipe — a merge included — is
  written with no module prefix.

  ## The §5 short form

  A module that writes its one procedure's `run/3` instead of `channels/1` is
  a whole single-procedure operative:

      use Flezha.SDK.StreamOperative, factory_id: "upcase", sink: "out", input: "words"

      def run(input, state, ctx) do
        emit(ctx, String.upcase(input))
        {:ok, state}
      end

  It generates the procedure module `Upcase.Procedure`, whose `name/0` is the
  factory id and which delegates to this module's procedure callbacks, and a
  `channels/1` wiring the entry channel (`input:`, `"input"` when absent) into
  it and, with `sink:`, it to that plugin channel. With no `sink:` nothing
  leaves the procedure but its state. `run/3` is written as in a procedure,
  with the ctx functions and a `state do ... end` block available, and the
  options `use Flezha.SDK.Procedure` takes (`max_entities:`, `clock:`, ...)
  apply to the generated procedure. The short form takes no class setting,
  which is the full form's. Writing `run/3`
  and `channels/1` both is a `CompileError` beginning `ShortFormWithTopology`,
  and writing neither is one naming both.
  """
  defmacro __using__(opts) do
    Procedure.__operative_using__(opts, @use_options, __MODULE__)
  end

  @callback factory_id() :: String.t()
  @callback channels(Flezha.SDK.Channels.t()) :: any()
  @callback description() :: String.t()
  @callback persistence_enabled() :: boolean()

  @optional_callbacks description: 0, persistence_enabled: 0

  @doc "Whether the class ships its changeful Procedures' records."
  @spec persistence_enabled?(module()) :: boolean()
  def persistence_enabled?(module) do
    if Procedure.exports?(module, :persistence_enabled, 0),
      do: module.persistence_enabled(),
      else: true
  end
end
