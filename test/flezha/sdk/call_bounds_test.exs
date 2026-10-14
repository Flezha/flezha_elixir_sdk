defmodule Flezha.SDK.CallBoundsTest do
  @moduledoc """
  Every call the Elixir SDK makes into one of its own processes answers its
  caller.

  The target here is suspended with `:sys.suspend/1`: alive, holding the
  message, never replying, which is what a wedged runner or local run looks
  like from the outside. Each client function is exercised in a process of its
  own, all at once, so the clause costs one bound rather than one per site: a
  caller that survives with a refusal passes, a caller the target exits fails,
  and a caller that never comes back fails.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Control, Error, Knob, Local, Runner}

  # Longer than the control bound every site waits, so a site that waits with
  # no bound reports as hung rather than as this clause's own limit.
  @budget Knob.get(:sdk_control_call_ms) + 3_000

  defmodule Wedged do
    @moduledoc false
    use GenServer

    def start(), do: GenServer.start(__MODULE__, :ok)

    @impl true
    def init(:ok), do: {:ok, :ok}
  end

  setup do
    {:ok, wedged} = Wedged.start()
    :sys.suspend(wedged)
    on_exit(fn -> Process.exit(wedged, :kill) end)
    %{wedged: wedged}
  end

  test "every call into a runner or a local run answers, and the caller lives", %{wedged: w} do
    sites = [
      runner_define: fn -> Runner.define(w, __MODULE__) end,
      runner_remove: fn -> Runner.remove(w, "factory") end,
      runner_relays: fn -> Runner.relays(w) end,
      control: fn -> Control.call(w, "Relay.list_relays", []) end,
      local_on_output: fn -> Local.on_output(w, "c", & &1) end,
      local_feed: fn -> Local.feed(w, "c", "v") end,
      local_until_idle: fn -> Local.run_until_idle(w, 50) end
    ]

    answers =
      sites
      |> Enum.map(fn {name, call} -> {name, Task.async(call)} end)
      |> Enum.map(fn {name, task} -> {name, Task.yield(task, @budget) || Task.shutdown(task)} end)

    for {name, answer} <- answers do
      assert {:ok, {:error, %Error{}}} = answer, "#{name} answered #{inspect(answer)}"
    end
  end

  test "nothing else in the SDK waits on its own processes without a bound" do
    direct =
      for path <- Path.wildcard("lib/flezha/sdk/**/*.ex"),
          Path.basename(path) not in ["safe_call.ex", "call.ex"],
          line <- File.stream!(path),
          String.contains?(line, "GenServer.call("),
          do: {Path.basename(path), String.trim(line)}

    # The pool's drain waits its own bound, the time it gives its workers.
    assert direct == [
             {"pool.ex",
              "undrained = GenServer.call(pool, {:drain, wait_ms, how}, wait_ms + 5_000)"}
           ]
  end
end
