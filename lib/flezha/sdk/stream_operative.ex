defmodule Flezha.SDK.StreamOperative do
  @moduledoc """
  Behaviour an Elixir stream operative implements: a `factory_id/0` the
  controller places by, a `channels/1` that authors the topology on the builder
  token, an optional human `description/0`, and an optional
  `persistence_enabled/0` (default `true`, the Rust `PERSISTENCE_ENABLED`):
  `false` advances every changeful Entity's record seq and ships nothing, so a
  class toggled back on later never hands the controller a seq it already saw.
  """

  alias Flezha.SDK.Procedure

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
