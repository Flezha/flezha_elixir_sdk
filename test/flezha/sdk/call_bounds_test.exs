defmodule Flezha.SDK.CallBoundsTest do
  @moduledoc """
  Every call the Elixir SDK makes into one of its own processes answers its
  caller.

  The target here is suspended with `:sys.suspend/1` — alive, holding the
  message, never replying, which is what a wedged Entity or Operative looks
  like from the outside. Each client function is exercised in a process of its
  own, all at once, so the clause costs one bound rather than one per site: a
  caller that survives with a refusal passes, a caller the target exits fails,
  and a caller that never comes back fails.

  Three calls wait with no bound at all, and the last two clauses hold them to it
  from both sides: they must still be waiting when everything else has answered,
  and they must remain the only three the SDK writes.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Flezha.SDK.{Commands, Credit, Entity, Local, Operative, RunnerServer}
  alias Flezha.SDK.Wire

  # Longer than the widest bound under test — the restore fold — so a site that
  # still waits with no bound reports as hung rather than as this clause's own
  # limit. Every site runs at once, so the clause costs one bound, not twenty.
  # The snapshot's 25 s bound would make this clause the slowest thing in the
  # suite for no new information: it is the same expression as the restore with
  # a different row, and the last clause is what holds it to that row.
  @budget 13_000

  defmodule Wedged do
    @moduledoc false
    use GenServer

    def start(), do: GenServer.start(__MODULE__, :ok)

    @impl true
    def init(:ok), do: {:ok, :ok}
  end

  # Answers a lookup and nothing else: the compensation-marker call is made
  # only after a lookup succeeds, so the one site that needs a half-wedged
  # target gets one.
  defmodule LookupOnly do
    @moduledoc false
    use GenServer

    def start(), do: GenServer.start(__MODULE__, :ok)

    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call({:lookup, _procedure, _key, _materialise}, _from, s),
      do: {:reply, {:ok, self()}, s}

    def handle_call(_other, _from, s), do: {:noreply, s}
  end

  setup do
    {:ok, wedged} = Wedged.start()
    {:ok, lookup_only} = LookupOnly.start()
    :sys.suspend(wedged)

    on_exit(fn ->
      Process.exit(wedged, :kill)
      Process.exit(lookup_only, :kill)
    end)

    %{wedged: wedged, lookup_only: lookup_only}
  end

  describe "a suspended target refuses its caller" do
    @tag timeout: 60_000
    test "every call with a deadline answers, and the caller lives", ctx do
      answers =
        capture_answers(fn ->
          answers(refusing_sites(ctx), @budget)
        end)

      assert answers == %{
               commands_markers:
                 {:answered,
                  {"error",
                   [
                     "restore_init_state: entity_busy: the operative did not take the " <>
                       "compensation markers"
                   ]}},
               credit_session: {:answered, nil},
               entity_apply_state:
                 {:answered, {:error, "entity_busy: the Entity did not answer"}},
               entity_evict: {:answered, :busy},
               entity_idle_for: {:answered, false},
               entity_read: {:answered, {:error, :busy}},
               entity_restore: {:answered, {:error, "entity_busy: the Entity did not answer"}},
               local_feed: {:answered, {:error, "the local run did not answer"}},
               local_on_sink: {:answered, {:error, "the local run did not answer"}},
               local_operative: {:answered, nil},
               local_until_idle:
                 {:answered, {:error, "the local run did not answer within 50ms"}},
               operative_entities: {:answered, {:error, :operative_busy}},
               operative_lookup: {:answered, {:error, :operative_busy}},
               runner_define: {:answered, {:error, :runner_busy}},
               runner_namespace: {:answered, nil},
               runner_remove: {:answered, {:error, :runner_busy}}
             }
    end

    test "the three calls that must not be abandoned still wait", ctx do
      # Short on purpose: proving a call does NOT answer costs the whole budget.
      # This clause shows the caller is not refused early; the next one is what
      # holds the two sites to `:infinity`, because a caller blocked at 5 000 ms
      # and one blocked for ever look identical after 300 ms.
      assert answers(protocol_waits(ctx), 300) == %{
               credit_acquire: :hung,
               entity_enqueue_block: :hung,
               entity_rebind: :hung
             }
    end

    test "and nothing else in the SDK waits without one" do
      lines =
        for path <- Path.wildcard("lib/flezha/sdk/**/*.ex"),
            # The bounded call itself, the one call every other one goes through.
            Path.basename(path) != "safe_call.ex",
            line <- File.stream!(path),
            do: {Path.basename(path), line}

      # Every direct call is one of the protocol waits: a bare `GenServer.call`
      # anywhere else is one that never reached the seam, so it would still exit
      # its caller.
      direct = for {file, line} <- lines, String.contains?(line, "GenServer.call("), do: file
      assert Enum.sort(direct) == ["credit.ex", "entity.ex"]

      # And exactly three calls wait with no bound — the two direct ones and the
      # rebind, which takes `:infinity` through the seam so its refusal on a gone
      # Entity is still declared. A fourth is a bound someone removed.
      never_bound =
        for {file, line} <- lines,
            String.contains?(line, "GenServer.call(") or String.contains?(line, "Call.entity("),
            String.contains?(line, ":infinity"),
            do: file

      assert Enum.sort(never_bound) == ["credit.ex", "entity.ex", "entity.ex"]
    end

    test "a call that grows with the state names its own row, never the flat bound" do
      entity = File.read!("lib/flezha/sdk/entity.ex")

      # A refusal is not a cancellation — the Entity finishes the work either
      # way — so these three must not give up while the Controller is still
      # waiting for the answer. Each names the row derived from the Controller
      # bound that awaits it; taking `Call.entity/3`'s flat default instead
      # would report a failure that did not happen.
      rows =
        for line <- String.split(entity, "\n"),
            String.contains?(line, "Call.entity("),
            [_, row] <- Regex.scan(~r/timeout: Knob\.get\((:\w+)\)/, line),
            do: row

      assert Enum.sort(rows) == [
               ":sdk_entity_apply_ms",
               ":sdk_entity_restore_ms",
               ":sdk_entity_snapshot_ms"
             ]
    end
  end

  defp refusing_sites(%{wedged: w, lookup_only: lookup_only}) do
    [
      {:credit_session, fn -> Credit.session(w) end},
      {:runner_define, fn -> RunnerServer.define(w, __MODULE__, %{}) end},
      {:runner_remove, fn -> RunnerServer.remove(w, __MODULE__) end},
      {:runner_namespace, fn -> RunnerServer.namespace(w) end},
      {:operative_lookup, fn -> Operative.lookup(w, "p", "k") end},
      {:operative_entities, fn -> Operative.entities(w) end},
      {:local_on_sink, fn -> Local.on_sink(w, "c", & &1) end},
      {:local_feed, fn -> Local.feed(w, "c", "v") end},
      {:local_operative, fn -> Local.operative(w) end},
      {:local_until_idle, fn -> Local.run_until_idle(w, 50) end},
      {:entity_restore, fn -> Entity.restore(w, <<>>, <<>>, <<>>, 0) end},
      {:entity_read, fn -> Entity.read(w, 100) end},
      {:entity_apply_state, fn -> Entity.apply_state(w, %{}) end},
      {:entity_idle_for, fn -> Entity.idle_for?(w, 1, 100) end},
      {:entity_evict, fn -> Entity.evict(w) end},
      {:commands_markers, fn -> restore_with_markers(lookup_only) end}
    ]
  end

  defp protocol_waits(%{wedged: w}) do
    [
      {:credit_acquire, fn -> Credit.acquire(w, 1) end},
      {:entity_enqueue_block, fn -> Entity.enqueue_block(w, "v") end},
      {:entity_rebind, fn -> Entity.rebind(w, []) end}
    ]
  end

  # The compensation markers ride a restore, so the site is driven through the
  # command that makes it; the response the controller would read is the answer.
  defp restore_with_markers(op) do
    me = self()
    entity_id = Wire.encode_entity_id("op-1", "proc", "k")

    cmd = %{
      entity_id: entity_id,
      snapshot_header: <<>>,
      snapshot_bytes: <<>>,
      change_log_bytes: <<>>,
      terminal_seq: 0,
      has_compensation_intent: true,
      has_compensation_done: false
    }

    respond = fn status, errors, _raw, _meta -> send(me, {:responded, status, errors}) end
    Commands.dispatch(:restore_init_state, cmd, respond, %{operatives: %{"op-1" => {op, "f"}}})

    receive do
      {:responded, status, errors} -> {status, errors}
    after
      @budget -> :no_response
    end
  end

  # A refusal is logged once per site by the call seam; the clause is
  # about the answers, so the lines are captured rather than printed.
  defp capture_answers(fun) do
    parent = self()
    capture_log(fn -> send(parent, {:answers, fun.()}) end)

    receive do
      {:answers, answers} -> answers
    after
      1_000 -> flunk("the capture produced no answers")
    end
  end

  # Every site at once, each in an unlinked process, so a caller the target
  # exits is reported rather than inherited.
  defp answers(sites, budget) do
    parent = self()

    running =
      for {name, fun} <- sites, into: %{} do
        {pid, ref} = spawn_monitor(fn -> send(parent, {:answered, self(), fun.()}) end)
        {ref, {name, pid}}
      end

    collect(running, %{}, System.monotonic_time(:millisecond) + budget)
  end

  defp collect(running, answers, _deadline) when map_size(running) == 0, do: answers

  defp collect(running, answers, deadline) do
    receive do
      {:answered, pid, value} ->
        {ref, {name, _pid}} = Enum.find(running, fn {_r, {_n, p}} -> p == pid end)
        Process.demonitor(ref, [:flush])
        collect(Map.delete(running, ref), Map.put(answers, name, {:answered, value}), deadline)

      {:DOWN, ref, :process, _pid, reason} ->
        {name, _pid} = Map.fetch!(running, ref)
        collect(Map.delete(running, ref), Map.put(answers, name, {:died, reason}), deadline)
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        for {_ref, {_name, pid}} <- running, do: Process.exit(pid, :kill)

        Enum.reduce(running, answers, fn {_ref, {name, _pid}}, acc ->
          Map.put(acc, name, :hung)
        end)
    end
  end
end
