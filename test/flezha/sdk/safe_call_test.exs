defmodule Flezha.SDK.SafeCallTest do
  @moduledoc """
  The exit-reason judgement a bounded call makes, as `classify/1` and
  `orderly?/1` answer it to a site holding a reason of its own, and the
  level a bounded call logs each refusal at.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Flezha.SDK.SafeCall

  @call {GenServer, :call, [:server, :request, 5_000]}

  describe "orderly?/1" do
    test ":normal is orderly" do
      assert SafeCall.orderly?(:normal)
    end

    test ":shutdown is orderly" do
      assert SafeCall.orderly?(:shutdown)
    end

    test "{:shutdown, _} is orderly" do
      assert SafeCall.orderly?({:shutdown, :rescale})
    end

    test "a call's exit is judged on its own reason" do
      assert SafeCall.orderly?({:shutdown, @call})
      assert SafeCall.orderly?({{:shutdown, :rescale}, @call})
      refute SafeCall.orderly?({:timeout, @call})
      refute SafeCall.orderly?({:killed, @call})
    end

    test "a crash is not orderly" do
      refute SafeCall.orderly?(:killed)
      refute SafeCall.orderly?(:noproc)
      refute SafeCall.orderly?({%RuntimeError{message: "boom"}, []})
    end
  end

  describe "bounded/4 with a :log reason table" do
    test "a row of false silences that reason only, a missing row warns" do
      dead = spawn(fn -> :ok end)
      ref = Process.monitor(dead)
      assert_receive {:DOWN, ^ref, _, _, _}
      quiet = [on_error: :error, log: %{noproc: false}]

      assert capture_log(fn ->
               assert SafeCall.bounded(dead, :request, 50, quiet) == :error
             end) == ""

      mute = spawn_link(fn -> Process.sleep(:infinity) end)

      log =
        capture_log([level: :warning], fn ->
          assert SafeCall.bounded(mute, :request, 50, quiet) == :error
        end)

      assert log =~ "[warning]"
      assert log =~ "bounded call failed"
      assert log =~ ":timeout"
    end

    test "a target that stops while it is called takes the :orderly row" do
      stopping =
        spawn(fn ->
          receive do: ({:"$gen_call", _from, _request} -> exit(:shutdown))
        end)

      quiet = [on_error: :error, log: %{orderly: false}]

      assert capture_log(fn ->
               assert SafeCall.bounded(stopping, :request, 1_000, quiet) == :error
             end) == ""

      crashing =
        spawn(fn ->
          receive do: ({:"$gen_call", _from, _request} -> exit(:boom))
        end)

      log =
        capture_log([level: :warning], fn ->
          assert SafeCall.bounded(crashing, :request, 1_000, quiet) == :error
        end)

      assert log =~ "bounded call failed"
      assert log =~ ":boom"
    end

    test "a reason without a row of its own takes the :other row" do
      mute = spawn_link(fn -> Process.sleep(:infinity) end)

      assert capture_log(fn ->
               assert SafeCall.bounded(mute, :request, 50,
                        on_error: :error,
                        log: %{noproc: :warning, other: false}
                      ) == :error
             end) == ""
    end
  end

  describe "classify/1" do
    test "a call that timed out is :timeout" do
      assert SafeCall.classify({:timeout, @call}) == :timeout
    end

    test "a call that found no process is :noproc, and so is a bare :noproc" do
      assert SafeCall.classify({:noproc, @call}) == :noproc
      assert SafeCall.classify(:noproc) == :noproc
    end

    test "any other reason is itself" do
      assert SafeCall.classify(:killed) == :killed
      assert SafeCall.classify({:shutdown, :rescale}) == {:shutdown, :rescale}
    end
  end
end
