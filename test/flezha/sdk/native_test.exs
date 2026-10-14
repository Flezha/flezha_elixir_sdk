defmodule Flezha.SDK.NativeTest do
  @moduledoc """
  The native library's term mapping, driven over a local run with this
  process as the worker: an activation arrives as the map the dispatcher
  branches on, a result is read back from the map it answers, a result that
  does not map is refused while its activation is still answered, and an
  output comes back as the record an emit produced.
  """
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Channels, Declaration, Native}

  defmodule Echo do
    use Flezha.SDK.Tasklet, name: "echo"
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule EchoOp do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "native_echo"

    def channels(c),
      do: c |> Channels.input("in") |> Channels.into(Echo) |> Channels.output("out")
  end

  setup do
    core = Native.context_new(Native.schema_version())
    {:ok, _class} = Declaration.declare(core, EchoOp)
    :ok = Native.local_start(core, "native_echo", "native_echo-1")
    :ok = Native.local_on_sink(core, "out")
    thread = Native.poller(core, self())

    on_exit(fn ->
      Native.local_stop(core)
      Native.stop_thread(thread)
      Native.context_free(core)
    end)

    %{core: core}
  end

  defp next_activation do
    assert_receive {:flz_activation, thread, act}, 5_000
    {thread, act}
  end

  test "an activation is a map of atoms, binaries and metadata pairs", %{core: core} do
    :ok = Native.local_feed(core, "in", "hello")
    {thread, act} = next_activation()

    assert %{
             token: token,
             relay: "native_echo-1",
             tasklet: 0,
             factory: "native_echo",
             kind: :work
           } =
             act

    assert is_integer(token) and is_integer(act.change_seq)

    assert [%{kind: :input, channel: 0, payload: "hello", replay: false, now: now} = item] =
             act.items

    assert is_integer(now) and is_list(item.metadata)

    output = %{target: :plugin, plugin_channel: "out", metadata: [{"k", "v"}], payload: "HELLO"}
    effect = %{verdict: :ok, changes: [], outputs: [output], timers: [], event_time: now}
    assert :ok = Native.complete(core, thread, %{token: token, outcome: :work, effects: [effect]})

    assert %{target: :plugin, plugin_channel: "out", payload: "HELLO", metadata: metadata} =
             Native.local_next_output(core, 5_000)

    assert {"k", "v"} in metadata and {"channel", "out"} in metadata
  end

  test "a result that does not map is refused, and its activation is still answered",
       %{core: core} do
    :ok = Native.local_feed(core, "in", "once")
    {thread, %{token: token}} = next_activation()

    assert {:error, document} = Native.complete(core, thread, %{token: token, outcome: :bogus})
    assert %Flezha.SDK.Error{} = Flezha.SDK.Error.from_document(document)

    # Answered as a retryable failure, so the core hands the input out again.
    {thread, %{token: again, items: [%{payload: "once"}]}} = next_activation()
    refute again == token

    effect = %{verdict: :ok, changes: [], outputs: [], timers: [], event_time: 0}
    assert :ok = Native.complete(core, thread, %{token: again, outcome: :work, effects: [effect]})
    assert :ok = Native.local_run_until_idle(core, 5_000)
  end

  test "a call still inside the context holds its free back, and one after it is refused",
       %{core: core} do
    waiting = Task.async(fn -> Native.local_next_output(core, 500) end)
    Process.sleep(50)
    started = System.monotonic_time(:millisecond)

    assert :ok = Native.context_free(core)
    assert System.monotonic_time(:millisecond) - started >= 300, "freed under a live call"
    assert Task.await(waiting) == nil

    assert {:error, document} = Native.local_feed(core, "in", "late")
    assert document =~ "freed"
  end

  test "a second live core in this VM is refused" do
    other = Native.context_new(Native.schema_version())
    {:ok, _class} = Declaration.declare(other, EchoOp)

    assert {:error, document} = Native.local_start(other, "native_echo", "native_echo-2")

    assert %Flezha.SDK.Error{code: "runtime.core_in_use"} =
             Flezha.SDK.Error.from_document(document)

    Native.context_free(other)
  end
end
