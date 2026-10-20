defmodule Flezha.SDK.NativeSurfaceTest do
  @moduledoc """
  `Flezha.SDK.Native`'s stubs against the C ABI fixture, `abi_surface.txt`:
  every entry point a stub reaches is one the fixture lists, and every entry
  point the fixture lists is reached by a stub or named here as one this SDK
  does not reach. An entry point the core adds fails here until this SDK
  either declares it or says why it does not.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.Contract

  # The stubs whose entry point is not `flz_core_<stub>`: the NIF's own
  # threads, and the start it makes with its settings. `nil` reaches none.
  @other_names %{
    start: "flz_core_start_with",
    poller: "flz_core_poll",
    starvation_watch: "flz_core_wait_starvation",
    release_thread: "flz_core_complete",
    stop_thread: nil,
    set_notice_sink: nil
  }

  # Reached by no stub: the NIF frees no core buffer and reads no error slot,
  # since every refusal comes back as `{:error, document}`, and the runner
  # needs no clock, no abandon, no stdin watch of the core, and neither notice
  # entry: the NIF sends the notices to the sink itself. Knobs are read by
  # `Flezha.SDK.Knob`, which the controller parses with too.
  @unreached ~w(flz_core_abandon flz_core_buf_free flz_core_error_document
                flz_core_init_error flz_core_knob flz_core_last_error
                flz_core_monotonic_nanos flz_core_set_log_callback flz_core_start
                flz_core_take_notices flz_core_watch_stdin_eof)

  test "the stubs reach exactly the entry points the fixture lists" do
    listed =
      "test/fixtures/abi_surface.txt"
      |> Contract.path!()
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "#")))
      |> MapSet.new()

    reached =
      stubs()
      |> Enum.map(&Map.get(@other_names, &1, "flz_core_#{&1}"))
      |> Enum.reject(&is_nil/1)
      |> MapSet.new()

    unknown = MapSet.difference(reached, listed)
    assert MapSet.size(unknown) == 0, "stubs reach #{inspect(unknown)}, which the fixture lacks"

    unreached = MapSet.difference(listed, reached)

    assert unreached == MapSet.new(@unreached),
           "no stub reaches #{inspect(unreached)}; this test names #{inspect(@unreached)}"
  end

  # Every function `native.ex` defines as a NIF stub, read from its source so
  # a helper beside them is never taken for one.
  defp stubs do
    Path.expand("../../../lib/flezha/sdk/native.ex", __DIR__)
    |> File.read!()
    |> Code.string_to_quoted!()
    |> Macro.prewalk([], fn
      {:def, _, [{name, _, _}, [do: {{:., _, [:erlang, :nif_error]}, _, _}]]} = node, acc ->
        {node, [name | acc]}

      node, acc ->
        {node, acc}
    end)
    |> elem(1)
  end
end
