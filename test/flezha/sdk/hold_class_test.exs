defmodule Flezha.SDK.HoldClassTest do
  @moduledoc """
  The guarantee class table, which is the whole answer to a write the wire
  refuses: one row per frame the Elixir SDK writes, and no policy anywhere else.

  The per-site scenarios this replaces each proved one writer's answer. What is
  proved here is that there is only one answer to prove — a payload id the table
  does not name is refused at the call, and no file outside the seam writes to a
  stream at all.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Hold, LocalStream}
  alias Flezha.SDK.Wire
  @sdk "lib/flezha/sdk"
  @seam "#{@sdk}/hold.ex"
  # The stream handles themselves: where a send is defined, not where one is made.
  @streams ~w(transport.ex local_stream.ex native.ex) |> Enum.map(&"#{@sdk}/#{&1}")

  describe "the class a frame declares" do
    rows = [
      {Wire.payload_id_cell_state_change(), :durable},
      {Wire.payload_id_cell_effect_record(), :durable},
      {Wire.payload_id_cell_state_snapshot(), :durable},
      {Wire.payload_id_delete_cell_state(), :durable},
      {Wire.payload_id_compensation_intent(), :durable},
      {Wire.payload_id_compensation_done(), :durable},
      {Wire.payload_id_park_payload(), :durable},
      {{"control_command", :runner_shutdown_notification}, :durable},
      {{"control_command", :update_factories}, :control},
      {{"control_command", :admin_request}, :control},
      {{"control_command", :spawn_bounded_request}, :control},
      {{"control_command", :task_commit}, :control},
      {{"control_command", :iter_commit}, :control},
      {{"control_command", :iter_park}, :control},
      {{"control_command", :bounded_completed}, :control},
      {{"control_command", :operator_exited}, :control},
      {{"control_command", :response}, :control},
      {"control_ping", :control},
      {"connect_control_channel", :control},
      {"correction_request", :control},
      {"output_part", :preview},
      {"data_heartbeat", :probe},
      {{:output, "op-1"}, :output}
    ]

    for {key, class} <- rows do
      test "#{inspect(key)} is #{class}" do
        assert Hold.class(unquote(Macro.escape(key))) == {:ok, unquote(class)}
      end
    end
  end

  describe "a frame the table does not name" do
    test "is refused at the call rather than given a default policy" do
      assert Hold.class("not_a_frame") == :error

      assert_raise ArgumentError, ~r/no guarantee class declares "not_a_frame"/, fn ->
        Hold.write("not_a_frame", Hold.new(), {<<1>>, %{}})
      end
    end

    test "a control_command variant the table does not name is refused too" do
      assert Hold.class({"control_command", :not_a_variant}) == :error
    end
  end

  describe "a preview" do
    test "is refused, and never held, when it cannot be written at once" do
      assert {{:error, :no_stream}, hold} =
               Hold.write("output_part", Hold.new(), {"p", %{}, "op"})

      assert hold.n == 0

      {sdk_side, controller_side} = LocalStream.open()
      :ok = LocalStream.close(controller_side)
      gone = Hold.bind(Hold.new(), [sdk_side])

      assert {{:error, :closed}, hold} = Hold.write("output_part", gone, {"p", %{}, "op"})
      assert hold.n == 0
    end
  end

  describe "the seam is the only writer" do
    test "no file of the SDK writes to a stream except the seam" do
      writers =
        "#{@sdk}/**/*.ex"
        |> Path.wildcard()
        |> Enum.reject(&(&1 in @streams))
        |> Enum.filter(&(File.read!(&1) =~ "send_frame("))

      assert writers == [@seam],
             "a write outside the seam chooses its own answer to a refused wire; " <>
               "name the frame's payload id and let #{@seam} answer: #{inspect(writers)}"
    end
  end
end
