defmodule Flezha.SDK.Timers do
  @moduledoc """
  An Entity's pending timers and the session an invocation registers through —
  the kernel's pending set and timer session re-expressed in this SDK's own
  core. One ordered set per kind; registering a pending timestamp again
  changes nothing. Every registration, cancellation and firing is an op — the
  map `Flezha.State.Wire` encodes for a `format_id = 3` record — so the set is
  folded state: `apply/2` is the one mutation seam, live and on restore, and
  `register_ops/1` re-states the whole set after a compaction. A session works
  on a COPY of the set, so a failed attempt's registrations do not survive its
  retry; only the attempt that completes commits, and a firing's session opens
  on the fired timer's removal so it rides the same record as what `on_timer/4`
  applies. Arming is the Entity's, on the BEAM's timer service; counting is
  telemetry's, from the tally a commit answers.
  """

  defstruct event_time: :gb_sets.empty(), processing_time: :gb_sets.empty()

  @type kind :: :event_time | :processing_time
  @type op :: %{kind: kind(), scheduled_at: non_neg_integer(), register: boolean()}
  @typedoc "What a Procedure declares about its timers: the names a refusal reports, the cap, its clock."
  @type policy :: %{procedure: String.t(), cap: pos_integer(), clock: atom()}
  @type refusal ::
          {:event_time_timer_without_event_time_clock, String.t(), atom()}
          | {:timer_cap_exceeded, String.t(), pos_integer()}
  @type tally :: %{
          registered: non_neg_integer(),
          fired: non_neg_integer(),
          cancelled: non_neg_integer(),
          refused: non_neg_integer()
        }
  @type t :: %__MODULE__{}

  defmodule Session do
    @moduledoc false
    defstruct [:pending, :policy, :now_ms, ops: [], firing?: false, refused: 0]
    @type t :: %__MODULE__{}
  end

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec register_op(kind(), non_neg_integer()) :: op()
  def register_op(kind, at), do: %{kind: kind, scheduled_at: at, register: true}

  @spec cancel_op(kind(), non_neg_integer()) :: op()
  def cancel_op(kind, at), do: %{kind: kind, scheduled_at: at, register: false}

  @doc "Fold one op — the single mutation seam, live and on restore. Whether the set changed."
  @spec apply(t(), op()) :: {boolean(), t()}
  def apply(%__MODULE__{} = t, op), do: fold_op(t, op)

  @spec apply_all(t(), [op()]) :: t()
  def apply_all(%__MODULE__{} = t, ops),
    do: Enum.reduce(ops, t, fn op, acc -> acc |> fold_op(op) |> elem(1) end)

  @spec contains?(t(), kind(), non_neg_integer()) :: boolean()
  def contains?(%__MODULE__{} = t, kind, at), do: :gb_sets.is_element(at, Map.fetch!(t, kind))

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{} = t),
    do: :gb_sets.size(t.event_time) + :gb_sets.size(t.processing_time)

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{} = t), do: size(t) == 0

  @doc "Timestamps of `kind` at or below `upto`, ascending."
  @spec due(t(), kind(), non_neg_integer()) :: [non_neg_integer()]
  def due(%__MODULE__{} = t, kind, upto),
    do: t |> Map.fetch!(kind) |> :gb_sets.iterator() |> take_due(upto, [])

  @spec all(t(), kind()) :: [non_neg_integer()]
  def all(%__MODULE__{} = t, kind), do: :gb_sets.to_list(Map.fetch!(t, kind))

  @doc "The full set as registrations — what a compaction re-states ABOVE the snapshot's seq."
  @spec register_ops(t()) :: [op()]
  def register_ops(%__MODULE__{} = t) do
    Enum.map(all(t, :event_time), &register_op(:event_time, &1)) ++
      Enum.map(all(t, :processing_time), &register_op(:processing_time, &1))
  end

  # --- the session ---

  @doc "A session over a COPY of `pending`; `firing` seeds it with that timer's removal as op 0."
  @spec session(t(), policy(), non_neg_integer(), {kind(), non_neg_integer()} | nil) ::
          Session.t()
  def session(pending, policy, now_ms, firing \\ nil)

  def session(%__MODULE__{} = pending, policy, now_ms, nil),
    do: %Session{pending: pending, policy: policy, now_ms: now_ms}

  def session(%__MODULE__{} = pending, policy, now_ms, {kind, at}) do
    {_changed?, pending} = fold_op(pending, cancel_op(kind, at))

    %Session{
      pending: pending,
      policy: policy,
      now_ms: now_ms,
      ops: [cancel_op(kind, at)],
      firing?: true
    }
  end

  @doc "The invocation's `now`, which `register_after/2` anchors on."
  @spec now(Session.t()) :: non_neg_integer()
  def now(%Session{now_ms: ms}), do: ms

  @doc "Register `kind` at `at`: the absolute timestamp, or a refusal (counted) with the set unchanged."
  @spec register(Session.t(), kind(), non_neg_integer()) ::
          {:ok, non_neg_integer(), Session.t()} | {:error, refusal(), Session.t()}
  def register(%Session{policy: policy} = s, kind, at) do
    cond do
      kind == :event_time and policy.clock != :event_time ->
        refuse(s, {:event_time_timer_without_event_time_clock, policy.procedure, policy.clock})

      not contains?(s.pending, kind, at) and size(s.pending) >= policy.cap ->
        refuse(s, {:timer_cap_exceeded, policy.procedure, policy.cap})

      true ->
        {_changed?, s} = note(s, register_op(kind, at))
        {:ok, at, s}
    end
  end

  @doc "A processing-time timer `delay_ms` after the session's `now`."
  @spec register_after(Session.t(), integer()) ::
          {:ok, non_neg_integer(), Session.t()} | {:error, refusal(), Session.t()}
  def register_after(%Session{now_ms: now} = s, delay_ms),
    do: register(s, :processing_time, max(now + delay_ms, 0))

  @doc "Cancel by the scheduled timestamp; whether one was pending."
  @spec cancel(Session.t(), kind(), non_neg_integer()) :: {boolean(), Session.t()}
  def cancel(%Session{} = s, kind, at), do: note(s, cancel_op(kind, at))

  @doc "The copy as the live set, the ops to make durable in order, and the tally."
  @spec commit(Session.t()) :: {t(), [op()], tally()}
  def commit(%Session{} = s) do
    ops = Enum.reverse(s.ops)
    zero = %{registered: 0, fired: 0, cancelled: 0, refused: s.refused}

    tally =
      ops
      |> Enum.with_index()
      |> Enum.reduce(zero, fn
        {%{register: true}, _index}, t -> %{t | registered: t.registered + 1}
        {_removal, 0}, t when s.firing? -> %{t | fired: t.fired + 1}
        {_cancel, _index}, t -> %{t | cancelled: t.cancelled + 1}
      end)

    {s.pending, ops, tally}
  end

  # An op joins the session only when it changed the copy.
  defp note(%Session{} = s, op) do
    case fold_op(s.pending, op) do
      {true, pending} -> {true, %{s | pending: pending, ops: [op | s.ops]}}
      {false, _} -> {false, s}
    end
  end

  defp refuse(%Session{} = s, refusal), do: {:error, refusal, %{s | refused: s.refused + 1}}

  defp fold_op(%__MODULE__{} = t, %{kind: kind, scheduled_at: at, register: register}) do
    set = Map.fetch!(t, kind)
    changed? = :gb_sets.is_element(at, set) != register
    set = if register, do: :gb_sets.add(at, set), else: :gb_sets.delete_any(at, set)
    {changed?, Map.put(t, kind, set)}
  end

  defp take_due(iter, upto, acc) do
    case :gb_sets.next(iter) do
      {at, iter} when at <= upto -> take_due(iter, upto, [at | acc])
      _ -> Enum.reverse(acc)
    end
  end
end
