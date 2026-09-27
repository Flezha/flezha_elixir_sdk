defmodule Flezha.SDK.PrefixTest do
  @moduledoc "The processed-prefix ledger an Operator and its Cells share."
  use ExUnit.Case, async: true

  alias Flezha.SDK.Prefix

  # What a report would send, recorded as sent.
  defp take(ledger) do
    case Prefix.candidate(ledger) do
      nil ->
        nil

      prefix ->
        Prefix.reported(ledger, prefix)
        prefix
    end
  end

  defp settled(ledger, offset) do
    Prefix.hold(ledger, offset)
    Prefix.release(ledger, offset)
  end

  test "a quiet Cell does not hold the prefix once its input is settled" do
    ledger = Prefix.new()
    for offset <- 1..1005, do: settled(ledger, offset)
    assert take(ledger) == 1005
    assert take(ledger) == nil
  end

  test "a pending input holds the prefix below it until every hold on it goes" do
    ledger = Prefix.new()
    Prefix.hold(ledger, 7)
    Prefix.hold(ledger, 7)
    settled(ledger, 8)
    assert take(ledger) == 6
    assert Prefix.pending?(ledger)

    Prefix.release(ledger, 7)
    assert take(ledger) == nil
    Prefix.release(ledger, 7)
    assert take(ledger) == 8
    refute Prefix.pending?(ledger)
  end

  test "nothing is below the first offset" do
    ledger = Prefix.new()
    Prefix.hold(ledger, 0)
    assert take(ledger) == nil
    Prefix.release(ledger, 0)
    assert take(ledger) == 0
  end

  test "after a restore nothing is reported until an input past everything seen arrives" do
    ledger = Prefix.new()
    for offset <- 1..3, do: settled(ledger, offset)
    assert take(ledger) == 3

    Prefix.restored(ledger)
    settled(ledger, 2)
    assert take(ledger) == nil
    settled(ledger, 4)
    assert take(ledger) == 4
  end

  test "a lost record freezes the prefix until a restore" do
    ledger = Prefix.new()
    Prefix.freeze(ledger)
    settled(ledger, 5)
    assert take(ledger) == nil

    Prefix.restored(ledger)
    settled(ledger, 6)
    assert take(ledger) == 6
  end

  test "a Cell with no ledger, or one whose Operator stopped, holds nothing" do
    assert Prefix.release(nil, 3) == :ok
    assert Prefix.restored(nil) == :ok
    assert Prefix.freeze(nil) == :ok

    gone = Prefix.new()
    :ets.delete(gone)
    assert Prefix.release(gone, 3) == :ok
    assert Prefix.freeze(gone) == :ok
  end
end
