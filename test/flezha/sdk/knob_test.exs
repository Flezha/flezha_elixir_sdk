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

    test "a row with no variable is set through the application env" do
      Application.put_env(:flezha_sdk, :sdk_idle_probe_ms, 7)
      on_exit(fn -> Application.delete_env(:flezha_sdk, :sdk_idle_probe_ms) end)
      assert Knob.get(:sdk_idle_probe_ms) == 7
    end

    test "a caller's computed default stands in for a row that cannot state one" do
      assert Knob.get(:reap_interval_ms, 125) == 125
    end
  end

  describe "a ladder" do
    test "doubles from its base, capping the exponent and then the value" do
      assert Enum.map(1..11, &Knob.delay(:sdk_reconnect, &1, rand: fn n -> div(n, 2) end)) ==
               [100, 200, 400, 800, 1_600, 3_200, 6_400, 12_800, 25_600, 25_600, 25_600]

      assert Knob.delay(:sdk_retry, 20, rand: fn n -> n end) == 5_000
      assert Knob.delay(:park_resend, 9) == 2_000
    end

    test "spreads a jittered wait over half to one and a half of it" do
      assert Knob.delay(:sdk_retry, 1, rand: fn _ -> 1 end) == 26
      assert Knob.delay(:sdk_retry, 1, rand: fn n -> n end) == 75
    end

    test "takes the caller's own first rung" do
      assert Knob.delay(:sdk_retry, 2, base: 10, rand: fn n -> div(n, 2) end) == 20
    end

    test "announces every wait and every reset" do
      ref = make_ref()
      events = [[:flezha, :backoff, :wait], [:flezha, :backoff, :reset]]
      :telemetry.attach_many(ref, events, &__MODULE__.forward/4, self())
      on_exit(fn -> :telemetry.detach(ref) end)

      assert Knob.arm(:park_resend, 1, "p-1") == 2_000

      assert_receive {[:flezha, :backoff, :wait], %{delay_ms: 2_000, attempt: 1},
                      %{ladder: :park_resend, target: "p-1"}}

      assert Knob.reset(:sdk_reconnect, "r-1") == 0

      assert_receive {[:flezha, :backoff, :reset], %{attempt: 0},
                      %{ladder: :sdk_reconnect, target: "r-1"}}
    end
  end

  @doc false
  def forward(event, measurements, metadata, pid), do: send(pid, {event, measurements, metadata})

  defp expect("error"), do: :error
  defp expect("true"), do: {:ok, true}
  defp expect("false"), do: {:ok, false}
  defp expect(value), do: {:ok, String.to_integer(value)}
end
