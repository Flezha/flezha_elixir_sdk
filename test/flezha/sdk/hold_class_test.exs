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

  alias Flezha.SDK.Hold
  alias Flezha.SDK.Wire
  @sdk "lib/flezha/sdk"
  @seam "#{@sdk}/hold.ex"
  # The stream handles themselves: where a send is defined, not where one is made.
  @streams ~w(transport.ex local_stream.ex native.ex) |> Enum.map(&"#{@sdk}/#{&1}")

  describe "the class a frame declares" do
    rows = [
      {Wire.payload_id_entity_state_change(), :durable},
      {Wire.payload_id_entity_effect_record(), :durable},
      {Wire.payload_id_entity_state_snapshot(), :durable},
      {Wire.payload_id_delete_entity_state(), :durable},
      {Wire.payload_id_compensation_intent(), :durable},
      {Wire.payload_id_compensation_done(), :durable},
      {Wire.payload_id_park_payload(), :durable},
      {{"control_command", :runner_shutdown_notification}, :durable},
      {{"control_command", :update_factories}, :control},
      {{"control_command", :admin_request}, :control},
      {{"control_command", :spawn_workflow_request}, :control},
      {{"control_command", :proc_commit}, :control},
      {{"control_command", :iter_commit}, :control},
      {{"control_command", :iter_park}, :control},
      {{"control_command", :workflow_completed}, :control},
      {{"control_command", :response}, :control},
      {"control_ping", :control},
      {"connect_control_channel", :control},
      {"correction_request", :control},
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
