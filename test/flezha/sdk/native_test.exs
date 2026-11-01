defmodule Flezha.SDK.NativeTest do
  @moduledoc """
  The native library's term mapping, driven over a local run with this
  process as the worker: an activation arrives as the map the dispatcher
  branches on, a result is read back from the map it answers, a result that
  does not map is refused while its activation is still answered, and an
  output comes back as the record an emit produced.
  """
  use ExUnit.Case, async: false

  # Each local run logs the core's banner, asserted in `setup`; the refused
  # result's retry logs a warning, asserted by its test.
  @moduletag :capture_log

  alias Flezha.SDK.{ChannelBuilder, Declaration, Native}
  alias Flezha.SDK.Test.Notices

  defmodule Echo do
    use Flezha.SDK.Tasklet, name: "echo"
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule EchoOp do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "native_echo"

    def channels(c),
      do:
        c
        |> ChannelBuilder.input("in")
        |> ChannelBuilder.into(Echo)
        |> ChannelBuilder.output("out")
  end

  setup do
    Native.notice_sink()
    Notices.forward()
    core = Native.context_new(Native.schema_version())
    {:ok, class} = Declaration.declare(core, EchoOp)
    :ok = Native.local_start(core, "native_echo", "native_echo-1")
    Notices.banner()
    :ok = Native.local_on_sink(core, "out")
    # One base seat, which this process fills as the worker; the governor's
    # ask for it is a `{:flz_grow, false}` nothing here receives.
    governor = Native.pool_open(core, 1, 0, self())
    thread = Native.poller(core, governor, self(), false)

    on_exit(fn ->
      Native.local_stop(core)
      Native.pool_stop(governor)
      Native.stop_thread(thread)
      Native.context_free(core)
    end)

    %{core: core, class: class}
  end

  # Whether `pid` runs the native `local_next_output/2` within two seconds.
  # Running, not runnable: a call still queued for a dirty scheduler shows the
  # same function but is not yet counted inside the context.
  defp inside_call?(pid, deadline \\ System.monotonic_time(:millisecond) + 2_000) do
    case Process.info(pid, [:current_function, :status]) do
      [current_function: {Native, :local_next_output, 2}, status: :running] ->
        true

      _elsewhere ->
        System.monotonic_time(:millisecond) < deadline and
          receive(after: (5 -> inside_call?(pid, deadline)))
    end
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

    assert [
             %{
               kind: :input,
               channel: 0,
               payload: "hello",
               replay: false,
               now: now,
               form: :untransformed
             } = item
           ] = act.items

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

    assert {:warning, "tasklet echo attempt 1/3 failed (retrying in " <> retry} = Notices.next()
    assert retry =~ ~s(unknown activation outcome "bogus")

    effect = %{verdict: :ok, changes: [], outputs: [], timers: [], event_time: 0}
    assert :ok = Native.complete(core, thread, %{token: again, outcome: :work, effects: [effect]})
    assert :ok = Native.local_run_until_idle(core, 5_000)
  end

  test "a call still inside the context holds its free back, and one after it is refused",
       %{core: core} do
    waiting =
      Task.async(fn ->
        started = System.monotonic_time(:millisecond)
        {started, Native.local_next_output(core, 500)}
      end)

    # The free is held back only once the call is inside the context, and the
    # call holds the context for its whole 500 ms wait, counted from its start.
    assert inside_call?(waiting.pid)
    assert :ok = Native.context_free(core)
    freed = System.monotonic_time(:millisecond)

    {started, output} = Task.await(waiting)
    assert output == nil
    assert freed - started >= 450, "freed under a live call"

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
