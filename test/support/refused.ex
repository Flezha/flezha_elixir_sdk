defmodule Flezha.SDK.Test.Refused do
  @moduledoc false
  import ExUnit.Assertions

  @doc """
  The `CompileError` a module body earns, compiled under a name of its own so
  one test can refuse several bodies.
  """
  def refused(body) do
    assert_raise CompileError, fn ->
      Code.compile_string("""
      defmodule Flezha.SDK.Refused#{System.unique_integer([:positive])} do
      #{body}
      end
      """)
    end
  end
end
