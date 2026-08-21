defmodule ElixirRunner.EchoProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "echo"
  def init_state(_), do: nil
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
end

defmodule ElixirRunner.EchoOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "elixir_e2e_echo"

  def channels(c) do
    import Flezha.SDK.Channels
    c |> input("in") |> into(ElixirRunner.EchoProc) |> named("echo") |> sink("elixir_echo_out")
  end
end

defmodule ElixirRunner.PoisonProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "poison"
  def init_state(_), do: nil
  def run(_input, _s, _ctx), do: {:error, {:non_retryable, "poison"}}
end

defmodule ElixirRunner.PoisonOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "elixir_e2e_poison"

  def channels(c) do
    import Flezha.SDK.Channels
    c |> input("in") |> into(ElixirRunner.PoisonProc) |> named("poison") |> sink("elixir_poison_out")
  end
end

defmodule ElixirRunner.KeyedProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "keyed"
  def init_state(_), do: 0
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s + 1})
end

defmodule ElixirRunner.KeyedOp do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "elixir_e2e_keyed"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("in")
    |> key_by(fn v -> v |> to_string() |> String.split(":") |> hd() end)
    |> into(ElixirRunner.KeyedProc)
    |> named("keyed")
    |> sink("elixir_keyed_out")
  end
end

defmodule ElixirRunner.Factories do
  @moduledoc "The e2e factory operatives this standalone runner advertises."
  def all, do: [ElixirRunner.EchoOp, ElixirRunner.PoisonOp, ElixirRunner.KeyedOp]
end
