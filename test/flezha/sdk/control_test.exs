defmodule Flezha.SDK.ControlTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.Control

  # A runner server that records each control call's verb and arguments and
  # answers every one with `null`.
  defp recording_server do
    test = self()

    spawn_link(fn -> answer(test) end)
  end

  defp answer(test) do
    receive do
      {:"$gen_cast", {:admin, from, ref, verb, args}} ->
        send(test, {:called, verb, Jason.decode!(args)})
        send(from, {:reply_ack, ref, "null", nil})
        answer(test)
    end
  end

  test "a plugin is enabled by name, and bound and unbound by operator" do
    server = recording_server()

    Control.enable_plugin(server, "writer", "operator_file_sink", opts: %{"path" => "out.txt"})

    assert_received {:called, "PluginManager.enable",
                     [
                       "writer",
                       %{"provider" => "operator_file_sink", "opts" => %{"path" => "out.txt"}}
                     ]}

    Control.enable_plugin(server, "feed", "operator_websocket",
      single: true,
      operator_id: "my_echo",
      hooks: [["on_output", %{}]]
    )

    assert_received {:called, "PluginManager.enable",
                     [
                       "feed",
                       %{
                         "provider" => "operator_websocket",
                         "opts" => %{},
                         "single" => true,
                         "operator_id" => "my_echo",
                         "hooks" => [["on_output", %{}]]
                       }
                     ]}

    Control.bind_plugin(server, "writer", "my_echo")
    assert_received {:called, "PluginManager.bind", ["writer", %{"operator_id" => "my_echo"}]}

    Control.bind_plugin(server, "writer", "my_other", hooks: ["on_output"], channel: "in")

    assert_received {:called, "PluginManager.bind",
                     [
                       "writer",
                       %{"operator_id" => "my_other", "hooks" => ["on_output"], "channel" => "in"}
                     ]}

    Control.unbind_plugin(server, "writer", "my_echo")
    assert_received {:called, "PluginManager.unbind", ["writer", %{"operator_id" => "my_echo"}]}

    Control.disable_plugin(server, "writer")
    assert_received {:called, "PluginManager.disable", ["writer", %{}]}

    Control.list_plugins(server)
    assert_received {:called, "PluginManager.list", []}
  end
end
