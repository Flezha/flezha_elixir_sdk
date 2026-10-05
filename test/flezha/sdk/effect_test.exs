defmodule Flezha.SDK.EffectTest do
  @moduledoc """
  `effect/4` (`recorded-effects`): a first run keeps its result through the
  runner under the identity every core derives, a kept result is answered
  without running the function, the result the controller kept first wins, an
  invocation with no input-log offset only runs the function, a steps stage
  names its effects by its pass, and the refusals.
  """
  # async: false: the cap test writes the application env.
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Ctx, Effects, Error}

  defmodule T do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "effect_probe"

    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  # A runner that answers each keep with `answer` and reports what it shipped.
  defp runner(answer) do
    test = self()

    spawn_link(fn ->
      receive do
        {:"$gen_cast", {:effect, caller, ref, {:effect_result, result}}} ->
          send(test, {:shipped, result})
          send(caller, {:reply_ack, ref, answer, nil})
      end
    end)
  end

  defp ctx(opts) do
    Ctx.new(T, nil, 0, [relay_id: "rel", effects: Effects.new()] ++ opts)
  end

  test "a first run keeps its result under the identity every core derives" do
    ctx = ctx(origin: {"cell-a", 7}, runner: runner(nil))

    assert {:ok, %{"answer" => 42}} = Ctx.effect(ctx, "model", fn -> %{answer: 42} end)
    assert_receive {:shipped, shipped}
    assert shipped.identity == "8e020c11ce8c7e65014f0b83a666a054"
    assert {shipped.offset, shipped.name, shipped.payload} == {7, "model", ~s({"answer":42})}

    retry = %{ctx | ref: make_ref(), attempt: 2}

    assert {:ok, %{"answer" => 42}} =
             Ctx.effect(retry, "model", fn -> flunk("a retry reads the result it kept") end)
  end

  test "items of one Cell at one offset take their results in the order they ran" do
    ctx = ctx(origin: {"cell-a", 6}, runner: runner(nil))
    assert {:ok, "a"} = Ctx.effect(ctx, "model", fn -> "a" end)
    assert_receive {:shipped, %{identity: first}}

    second = %{ctx | ref: make_ref(), runner: runner(nil)}
    assert {:ok, "b"} = Ctx.effect(second, "model", fn -> "b" end)
    assert_receive {:shipped, %{identity: other}}
    refute other == first, "another item's first attempt takes the next ordinal"
  end

  test "the result the controller kept first is answered" do
    ctx = ctx(origin: {"cell-a", 1}, runner: runner("first"))
    assert {:ok, "first"} = Ctx.effect(ctx, "model", fn -> "second" end)
  end

  test "a codec encodes and decodes the result" do
    ctx = ctx(origin: {"cell-a", 2}, runner: runner(nil))
    codec = {&String.upcase/1, &String.downcase/1}
    assert {:ok, "hammer"} = Ctx.effect(ctx, "tool", fn -> "Hammer" end, codec)
    assert_receive {:shipped, %{payload: "HAMMER"}}
  end

  test "with no offset the function runs every time and nothing is sent" do
    ctx = ctx(runner: self())

    for n <- 1..2,
        do: assert({:ok, ^n} = Ctx.effect(%{ctx | ref: make_ref()}, "model", fn -> n end))

    refute_received {:"$gen_cast", _}
  end

  test "a steps stage names its effects by its pass, and an undo apart from the run" do
    pass = :atomics.new(1, signed: true)
    :atomics.put(pass, 1, 7)
    run = ctx(identity: "cell-a", pass: pass, runner: runner(nil))
    assert {:ok, 1} = Ctx.effect(run, "model", fn -> 1 end)
    assert_receive {:shipped, %{identity: "8e020c11ce8c7e65014f0b83a666a054"}}

    undo = %{run | ref: make_ref(), phase: :compensate, runner: runner(nil)}
    assert {:ok, 2} = Ctx.effect(undo, "model", fn -> 2 end)
    assert_receive {:shipped, %{identity: identity}}
    refute identity == "8e020c11ce8c7e65014f0b83a666a054"

    :atomics.put(pass, 1, -1)
    none = %{run | ref: make_ref(), runner: self()}
    assert {:ok, 3} = Ctx.effect(none, "model", fn -> 3 end)
    refute_received {:"$gen_cast", _}
  end

  test "the refusals name their rule" do
    ctx = ctx(origin: {"cell-a", 3}, runner: nil)

    for name <- ["", String.duplicate("m", 256)] do
      assert {:error, {:non_retryable, %Error{code: "effect.bad_name"}}} =
               Ctx.effect(ctx, name, fn -> flunk("a refused call runs nothing") end)
    end

    plain = ctx(runner: nil)

    assert {:ok, "done"} =
             Ctx.effect(plain, "outer", fn ->
               send(self(), {:inner, Ctx.effect(plain, "inner", fn -> 1 end)})
               "done"
             end)

    assert_received {:inner, {:error, {:non_retryable, %Error{code: "effect.nested"}}}}

    elsewhere = %{ctx | owner: spawn(fn -> :ok end)}

    assert {:error, {:non_retryable, %Error{code: "effect.outside_invocation"}}} =
             Ctx.effect(elsewhere, "model", fn -> 1 end)

    assert {:error, %Error{code: "effect.not_recorded"}} = Ctx.effect(ctx, "model", fn -> 1 end)
  end

  test "a result over the cap is refused and nothing is sent" do
    Application.put_env(:flezha_sdk, :effect_max_bytes, 4)
    on_exit(fn -> Application.delete_env(:flezha_sdk, :effect_max_bytes) end)
    ctx = ctx(origin: {"cell-a", 4}, runner: self())

    assert {:error, {:non_retryable, %Error{code: "effect.too_large"}}} =
             Ctx.effect(ctx, "model", fn -> "too long" end)

    refute_received {:"$gen_cast", _}
  end

  test "a raise in the function propagates and keeps nothing" do
    ctx = ctx(origin: {"cell-a", 5}, runner: self())
    assert_raise RuntimeError, "down", fn -> Ctx.effect(ctx, "model", fn -> raise "down" end) end
    refute_received {:"$gen_cast", _}
    assert {:error, %Error{}} = Ctx.effect(%{ctx | runner: nil}, "model", fn -> 1 end)
  end
end
