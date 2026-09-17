defmodule Flezha.SDK.KeyGroupTest do
  @moduledoc """
  The Elixir half of a three-implementation contract. The controller decides
  which member owns a key; the SDK refuses a key it does not own. If the two
  hashes disagree by one bit, every operative in that group parks its traffic —
  so the fixture vectors here are the SAME ones `determinism.rs` and
  `StableIdTest.java` assert.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.KeyGroup

  # `StableId::derive(identity, seq, ordinal)` in both SDKs: FNV-1a-128 over
  # identity, then the big-endian seq, then the big-endian ordinal.
  defp stable_id(identity, seq, ordinal) do
    KeyGroup.fnv1a128(identity)
    |> KeyGroup.fnv1a128(<<seq::unsigned-big-64>>)
    |> KeyGroup.fnv1a128(<<ordinal::unsigned-big-32>>)
    |> Integer.to_string(16)
    |> String.downcase()
    |> String.pad_leading(32, "0")
  end

  describe "fnv1a128/1,2" do
    test "reproduces the cross-language StableId fixture vectors" do
      assert stable_id("instance/op/key", 42, 0) == "bbf81fd4955afd1724203700a240e822"
      assert stable_id("instance/op/key", 42, 1) == "bbf81fd4965afd1724203700a240e95d"
      assert stable_id("", 0, 0) == "15915375576d2f389a938dc3a9940c9d"
    end

    test "the empty input is the bare offset basis" do
      assert KeyGroup.fnv1a128("") == 0x6C62272E07BB014262B821756295C58D
    end
  end

  describe "group_of/1" do
    test "reproduces the cross-language key-group fixture" do
      # The twin assertions live in `flz_core`'s `key_group_matches_the_cross_language_fixture`
      # and Java's `keyGroupMatchesTheCrossLanguageFixture`.
      assert KeyGroup.group_of("BTC-USD") == 129
      assert KeyGroup.group_of("ETH-USD") == 213
      assert KeyGroup.group_of("SOL-USD") == 60
      assert KeyGroup.group_of("") == 141
      assert KeyGroup.group_of("a") == 100
    end

    test "is the low byte of the digest and stays in range" do
      for key <- ["BTC-USD", "ETH-USD", "", "a", String.duplicate("x", 512)] do
        assert KeyGroup.group_of(key) == rem(KeyGroup.fnv1a128(key), 256)
        assert KeyGroup.group_of(key) in 0..255
      end
    end

    test "is stable across calls" do
      assert KeyGroup.group_of("BTC-USD") == KeyGroup.group_of("BTC-USD")
    end
  end
end
