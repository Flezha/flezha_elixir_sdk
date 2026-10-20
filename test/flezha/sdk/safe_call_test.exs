defmodule Flezha.SDK.SafeCallTest do
  @moduledoc """
  The exit-reason judgement a bounded call makes, as `classify/1` and
  `orderly?/1` answer it to a site holding a reason of its own.
  """
  use ExUnit.Case, async: true

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
