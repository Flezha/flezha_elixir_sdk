defmodule Flezha.SDK.StreamOperative do
  @moduledoc """
  Behaviour an Elixir stream operative implements: a `factory_id/0` the
  controller places by, a `channels/1` that authors the topology on the builder
  token, and an optional human `description/0`. Mirrors the Rust `StreamOperative`
  trait (`factory_id` + `channels`, plus `PERSISTENCE_ENABLED`, which is deferred
  here — v1 is persistence-free).
  """

  @callback factory_id() :: String.t()
  @callback channels(Flezha.SDK.Channels.t()) :: any()
  @callback description() :: String.t()

  @optional_callbacks description: 0
end
