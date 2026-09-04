defmodule Flezha.SDK.Knob do
  @moduledoc """
  The SDK's environment knobs, read in one place so a knob cannot mean
  something different depending on which module read it.
  """

  @doc "A positive integer from `name`, else `default`; anything below 1 reads as 1."
  @spec int(String.t(), pos_integer()) :: pos_integer()
  def int(name, default) do
    with val when is_binary(val) <- System.get_env(name),
         {n, _} <- Integer.parse(val) do
      max(n, 1)
    else
      _ -> default
    end
  end
end
