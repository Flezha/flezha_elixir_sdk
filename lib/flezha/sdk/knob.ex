defmodule Flezha.SDK.Knob do
  @moduledoc """
  The Elixir SDK's reader of the one names table.

  A runner is its own OS process, so it reads the environment rather than the
  controller's application env — but it reads the same rows: the variable a name
  is set with, the type its text is parsed as, and the value it takes when
  nothing sets it are `Flezha.Config.Keys`' answers, not this module's. A row
  whose default differs by who reads it hands the SDK the runner's side.

  A value that does not parse — a pool size of `"0"`, a flag spelled `"maybe"` —
  reads as the default, which is what every other SDK's reader answers too.
  """

  alias Flezha.Config
  alias Flezha.Config.Environment
  alias Flezha.Config.Keys

  @doc "What this runner reads for `name`: its variable, else the row's default."
  @spec get(Keys.name()) :: term()
  def get(name), do: get(name, Config.default(name, :runner))

  @doc """
  What this runner reads for `name`, with the caller's own fallback for a
  quantity the row cannot state — a sweep cadence derived from an entity's TTL.
  """
  @spec get(Keys.name(), term()) :: term()
  def get(name, fallback) do
    row = Keys.fetch!(name)

    with var when is_binary(var) <- row.env,
         raw when is_binary(raw) <- System.get_env(var),
         {:ok, value} <- Environment.parse(row, raw) do
      value
    else
      _ -> fallback
    end
  end
end
