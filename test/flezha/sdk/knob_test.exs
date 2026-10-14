defmodule Flezha.SDK.KnobTest do
  @moduledoc """
  One parse rule per knob type, and the same rule in every runtime: the contract
  set's `knob_parse_cases.txt` fed to this SDK's reader, and the order a name is
  read in.
  """
  # A variable is process-wide, so this module owns the ones it writes.
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Contract, Knob}

  defp cases do
    "test/fixtures/knob_parse_cases.txt"
    |> Contract.path!()
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.reject(&(String.starts_with?(&1, "#") or &1 == ""))
    |> Enum.map(fn line ->
      [type, input, expected] = String.split(line, "|")
      {String.to_existing_atom(type), if(input == "<empty>", do: "", else: input), expected}
    end)
  end

  test "every case parses the way the fixture says" do
    assert cases() != []

    for {type, input, expected} <- cases() do
      assert Knob.parse(type, input) == expect(expected),
             "#{type}|#{input}: expected #{expected}, got #{inspect(Knob.parse(type, input))}"
    end
  end

  describe "a name is read from its variable, then the application env, then its default" do
    setup do
      on_exit(fn ->
        System.delete_env("FLEZHA_MAILBOX_CAPACITY")
        Application.delete_env(:flezha_sdk, :mailbox_capacity)
      end)
    end

    test "the variable wins" do
      Application.put_env(:flezha_sdk, :mailbox_capacity, 512)
      System.put_env("FLEZHA_MAILBOX_CAPACITY", "2048")
      assert Knob.get(:mailbox_capacity) == 2_048
    end

    test "a variable that is not a value is a variable nobody set" do
      System.put_env("FLEZHA_MAILBOX_CAPACITY", "0")
      assert Knob.get(:mailbox_capacity) == 1_024

      Application.put_env(:flezha_sdk, :mailbox_capacity, 512)
      assert Knob.get(:mailbox_capacity) == 512
    end

    test "an application env value that is not one of the row's type reads as the default" do
      Application.put_env(:flezha_sdk, :mailbox_capacity, 0)
      assert Knob.get(:mailbox_capacity) == 1_024

      Application.put_env(:flezha_sdk, :mailbox_capacity, "256")
      assert Knob.get(:mailbox_capacity) == 256
    end

    test "a row with no variable is set through the application env" do
      Application.put_env(:flezha_sdk, :sdk_control_call_ms, 7)
      on_exit(fn -> Application.delete_env(:flezha_sdk, :sdk_control_call_ms) end)
      assert Knob.get(:sdk_control_call_ms) == 7
    end

    test "a row with no default reads nil until something sets it" do
      assert Knob.get(:worker_pool_max) == nil
    end
  end

  defp expect("error"), do: :error
  defp expect("true"), do: {:ok, true}
  defp expect("false"), do: {:ok, false}
  defp expect(value), do: {:ok, String.to_integer(value)}
end
