defmodule Flezha.SDK.Hold do
  @moduledoc """
  A bounded drop-oldest FIFO, and the reconnect hold built on it.

  Two processes write to a secondary stream — the Operative ships an operative's
  plugin-bound outputs, each of its Entities ships that Entity's change records —
  and both keep what no stream will take until one binds, so the queue, the cap
  and the shed are shared here rather than written twice. The cap rides the
  queue: `FLEZHA_RECONNECT_RING_CAPACITY` is read once, when the holder starts,
  and the knob then means the same thing to every holder on one wire.

  The Operative's evicted-key trace is the same structure at its own cap and
  uses `push/2` alone, because what a shed *means* is the caller's: a forgotten
  eviction trace and a refused frame are not the same event.
  """

  alias Flezha.SDK.{Counters, Knob}

  # The knob every SDK's reconnect hold reads, so one setting governs the wire
  # whichever runtime produced the frame.
  @cap_env "FLEZHA_RECONNECT_RING_CAPACITY"
  @default_cap 1_024

  @type t :: {:queue.queue(), non_neg_integer(), pos_integer()}

  @doc "An empty hold bounded at `cap` — the reconnect cap when none is given."
  @spec new(pos_integer()) :: t()
  def new(cap \\ cap()), do: {:queue.new(), 0, cap}

  @doc "The reconnect hold's cap: `FLEZHA_RECONNECT_RING_CAPACITY`, else 1024."
  @spec cap() :: pos_integer()
  def cap, do: Knob.int(@cap_env, @default_cap)

  @doc "Push `item`; at the cap the oldest goes, so the hold cannot outgrow it."
  @spec push(t(), term()) :: t()
  def push({queue, n, cap}, item) when n >= cap, do: {:queue.in(item, :queue.drop(queue)), n, cap}
  def push({queue, n, cap}, item), do: {:queue.in(item, queue), n + 1, cap}

  @doc """
  Keep a frame no stream would take. What a full hold sheds is a real loss and
  is counted where every drop-oldest on this wire is counted — a silent one is
  what the hold exists to stop.
  """
  @spec keep(t(), term(), Counters.t()) :: t()
  def keep(hold, item, counters) do
    if full?(hold), do: Counters.inc_drops(counters, :drop_oldest)
    push(hold, item)
  end

  @doc "Everything held, oldest first, and an empty hold to replay it into."
  @spec drain(t()) :: {[term()], t()}
  def drain({_queue, 0, _cap} = hold), do: {[], hold}
  def drain({queue, _n, cap}), do: {:queue.to_list(queue), new(cap)}

  @doc "Whether `item` is still remembered."
  @spec member?(t(), term()) :: boolean()
  def member?({queue, _n, _cap}, item), do: :queue.member(item, queue)

  defp full?({_queue, n, cap}), do: n >= cap
end
