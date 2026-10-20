defmodule Flezha.SDK.PoolTest do
  @moduledoc """
  The pool the core's governor sizes: a worker added when every worker is held
  and work for another Cell waits, a grown worker stopped once idle, a worker
  that died replaced, and a drain that waits no longer than it is told.
  """
  # async: false: one local run per VM, and the tasklet reaches the test by name.
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Flezha.SDK.{Channels, Dispatch, Native, Pool}

  defmodule Gate do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "gate"

    @impl true
    def run(input, state, ctx) do
      if input == "hold" do
        send(:pool_test, {:held, self()})

        receive do
          :go -> :ok
        end
      end

      send(:pool_test, {:ran, input})
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule GateOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "pool_gate_op"

    def channels(c),
      do:
        c
        |> Channels.input("in", codec: :text)
        |> Channels.key_by(& &1)
        |> Channels.into(Gate)
        |> Channels.output("out")
  end

  setup do
    Process.register(self(), :pool_test)
    System.put_env("FLEZHA_WORKER_POOL_SIZE", "1")
    on_exit(fn -> System.delete_env("FLEZHA_WORKER_POOL_SIZE") end)
    :ok
  end

  test "a held worker buys another, and the grown one stops once idle" do
    {core, pool} = serve(300)
    feed(core, "hold")
    assert_receive {:held, holder}, 5_000

    feed(core, "free")
    assert_receive {:ran, "free"}, 5_000
    assert workers(pool) == 2

    send(holder, :go)
    assert_receive {:ran, "hold"}, 5_000
    assert eventually(fn -> workers(pool) == 1 end), "the grown worker did not stop"
  end

  test "a worker that died is replaced, and the replacement serves" do
    {core, pool} = serve(0)
    assert eventually(fn -> workers(pool) == 1 end)
    [first] = Map.keys(:sys.get_state(pool).workers)

    Process.exit(first, :kill)
    assert eventually(fn -> Map.keys(:sys.get_state(pool).workers) not in [[], [first]] end)

    feed(core, "after")
    assert_receive {:ran, "after"}, 5_000
  end

  test "a drain gives a held worker no longer than it is told" do
    {core, pool} = serve(0)
    feed(core, "hold")
    assert_receive {:held, holder}, 5_000

    started = System.monotonic_time(:millisecond)
    assert Pool.drain(pool, 300, :stop) == 1
    assert System.monotonic_time(:millisecond) - started < 300 + 3_000
    send(holder, :go)
  end

  # A local run of `GateOp` served by a pool of its own, `retire_ms` its grown
  # workers' retire time (0 the governor's), closed when the test exits.
  defp serve(retire_ms) do
    Native.notice_sink()
    core = Native.context_new(Native.schema_version())
    :ok = Dispatch.declare(core, GateOp)
    :ok = Native.answered(Native.local_start(core, GateOp.factory_id(), "pool-test"))
    {:ok, pool} = Pool.start_link(core: core, cells: Dispatch.cells(), retire_ms: retire_ms)
    Process.unlink(pool)
    on_exit(fn -> Pool.close(%{core: core, pool: pool}, &Native.local_stop/1, 5_000, :stop) end)
    {core, pool}
  end

  defp feed(core, payload),
    do: :ok = core |> Native.local_feed("in", payload) |> Native.answered()

  defp workers(pool), do: map_size(:sys.get_state(pool).workers)

  defp eventually(check, deadline \\ System.monotonic_time(:millisecond) + 5_000) do
    cond do
      check.() ->
        true

      System.monotonic_time(:millisecond) > deadline ->
        false

      true ->
        Process.sleep(20)
        eventually(check, deadline)
    end
  end
end
