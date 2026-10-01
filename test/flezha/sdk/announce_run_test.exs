defmodule Flezha.SDK.AnnounceRunTest do
  @moduledoc """
  The run every factory entry announces: in the handshake a runner connects
  with, and in the `UpdateFactories` a module registered after start ships.
  The in-node transport is the seam: a stand-in controller registered as this
  VM's `Flezha.SDK.Connect` reads both off its own side of the control stream.
  """
  # The runner is a named singleton and the `Connect` registration is global.
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Channels, Connect, LocalStream, PersistentId}
  alias Flezha.SDK.Proto.RunnerCommands

  defmodule Controller do
    @moduledoc false
    @behaviour Flezha.SDK.Connect

    @impl true
    def connect(payload, controller_side) do
      test = Application.fetch_env!(:flezha_sdk, :announce_run_test)
      :ok = LocalStream.controlling_process(controller_side, test)
      :ok = LocalStream.set_active(controller_side)
      send(test, {:handshake, payload})
      {:ok, "announce-runner"}
    end

    @impl true
    def bind_data_stream(_runner_id, _stream_type, _controller_side), do: {:error, :unused}
  end

  defmodule Echo do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "announce_echo"

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule FlowClass do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "announce_flow"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(Echo)
  end

  defmodule StepsClass do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def run, do: {:steps, each: :chained, on_failure: :keep_progress}
    def factory_id, do: "announce_steps"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(Echo)
  end

  defmodule Late do
    @moduledoc false
    use Flezha.SDK.Operator,
      factory_id: "announce_late",
      sink: "out",
      run: :steps,
      description: "the late class"

    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  setup do
    id_path =
      Path.join(System.tmp_dir!(), "flezha_announce_run_#{System.unique_integer([:positive])}")

    Application.put_env(:flezha_sdk, :announce_run_test, self())
    Application.put_env(:flezha_sdk, :runner_id_path, id_path)
    PersistentId.reset_memo()
    Connect.register(Controller)

    on_exit(fn ->
      Application.delete_env(:flezha_sdk, Connect)
      Application.delete_env(:flezha_sdk, :runner_id_path)
      Application.delete_env(:flezha_sdk, :announce_run_test)
      PersistentId.reset_memo()
      File.rm(id_path)
    end)
  end

  test "every factory entry carries its class's run, before start and after it" do
    handle =
      "announce_ns"
      |> Flezha.SDK.create_runner()
      |> Flezha.SDK.register_factory(FlowClass)
      |> Flezha.SDK.register_factory(StepsClass)
      |> Flezha.SDK.start()

    assert %Flezha.SDK{} = handle
    on_exit(fn -> Flezha.SDK.close(handle) end)

    assert_receive {:handshake, payload}, 5_000

    assert Enum.map(payload["factories"], &Map.take(&1, ~w(id run each on_failure))) == [
             %{"id" => "announce_flow", "run" => "flow"},
             %{
               "id" => "announce_steps",
               "run" => "steps",
               "each" => "chained",
               "on_failure" => "keep_progress"
             }
           ]

    assert %Flezha.SDK{} = Flezha.SDK.register_factory(handle, Late)

    assert Enum.map(
             update_factories().factories,
             &{&1.id, &1.run, &1.each, &1.on_failure, &1.description}
           ) == [
             {"announce_flow", "flow", "", "", ""},
             {"announce_steps", "steps", "chained", "keep_progress", ""},
             {"announce_late", "steps", "once", "undo_all", "the late class"}
           ]
  end

  # The next `UpdateFactories` on the control stream, past any other frame.
  defp update_factories do
    assert_receive {:flz_data, _stream, _payload_id, frame, _metadata}, 5_000

    case RunnerCommands.CommandRequest.decode(frame) do
      {:ok, %{command: {:update_factories, update}}} -> update
      _other -> update_factories()
    end
  end
end
