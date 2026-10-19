defmodule Flezha.SDK.Test.Notices do
  @moduledoc false
  # The core's notices reach Logger from a process of their own, after the
  # call that caused them returns: a test that waits for its notices keeps
  # them inside its own log capture.
  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.SDK.Local

  # A `:logger` handler sending each event to the test that added it.
  @doc false
  def log(event, %{config: %{to: test}}), do: send(test, {:logged, event})

  @doc "Forward every logged event to the calling test until it exits."
  def forward do
    if Process.get(__MODULE__) == nil do
      handler = :"#{__MODULE__}.#{System.unique_integer([:positive])}"
      :ok = :logger.add_handler(handler, __MODULE__, %{config: %{to: self()}})
      on_exit(fn -> :logger.remove_handler(handler) end)
      Process.put(__MODULE__, handler)
    end

    :ok
  end

  @doc "The next event logged under the domain `[:flezha]`, as `{level, text}`."
  def next do
    receive do
      {:logged, %{meta: %{domain: domain}, level: level, msg: {:string, text}}} ->
        if :flezha in domain, do: {level, IO.chardata_to_string(text)}, else: next()

      {:logged, _other} ->
        next()
    after
      10_000 -> flunk("no core notice was logged")
    end
  end

  @doc "Assert the next notice is a local run's banner, logged at `:error`."
  def banner, do: assert({:error, "LOCAL MODE: state is held in memory" <> _} = next())

  @doc "A local run of `module`, stopped when the test exits, once its banner is logged."
  def start_local(module) do
    forward()
    {:ok, run} = Local.start(module)
    on_exit(fn -> Local.stop(run) end)
    banner()
    run
  end
end
