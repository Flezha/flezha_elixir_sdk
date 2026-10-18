defmodule Flezha.SDK.ChannelCodecsTest do
  @moduledoc """
  This SDK's entry codecs and default output serializer against the shared
  `test/fixtures/channel_codecs.txt`. An output here names no codec, and this
  SDK ships no MessagePack decoder, so the cases it reads are those whose sink
  names none and which leave MessagePack out.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Contract}

  defp cases do
    for line <-
          "test/fixtures/channel_codecs.txt"
          |> Contract.path!()
          |> File.read!()
          |> String.split("\n"),
        line != "" and not String.starts_with?(line, "#") and
          not String.contains?(line, "msgpack"),
        [entry, "none", payload, expected] <- [String.split(line, "|", parts: 4)],
        do: {line, entry, bytes(payload), expected}
  end

  defp bytes("<empty>"), do: ""
  defp bytes("hex:" <> hex), do: Base.decode16!(hex, case: :lower)
  defp bytes(text), do: text

  defp parser(entry) do
    c = Channels.new()
    opts = if entry == "none", do: [], else: [codec: String.to_atom(entry)]
    _ = Channels.input(c, "in", opts)
    Channels.take(c).entries["in"].parser
  end

  test "every shared case with no sink codec sends the bytes the fixture records" do
    rows = cases()
    assert rows != [], "the shared codec cases were read"

    for {line, entry, payload, expected} <- rows do
      sent =
        case parser(entry).(payload) do
          {:ok, value} -> Channels.default_serialize(value)
          {:error, _} -> "decode_error"
        end

      want = if String.ends_with?(expected, "_error"), do: expected, else: bytes(expected)
      assert sent == want, line
    end
  end
end
