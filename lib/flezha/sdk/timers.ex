defmodule Flezha.SDK.Timers do
  @moduledoc """
  A Cell's pending timers as this host sees them, and the session an
  invocation registers through. `flz_core` keeps the timers themselves, fires
  them and restores them; the host's copy is what the timer cap and a
  cancel's answer read. One ordered set per kind; registering a pending
  timestamp again changes nothing. A session works on a COPY of the set, so a
  failed attempt's registrations do not survive its retry, and a firing's
  session opens on the fired timer's removal.
  """

  alias Flezha.SDK.Error

  defstruct event_time: :gb_sets.empty(), processing_time: :gb_sets.empty()

  @type kind :: :event_time | :processing_time
  @type op :: %{kind: kind(), scheduled_at: non_neg_integer(), register: boolean()}
  @typedoc "What a Tasklet declares about its timers: the names a refusal reports, the cap, its clock."
  @type policy :: %{tasklet: String.t(), cap: pos_integer(), clock: atom()}
  @typedoc "`runtime.event_time_timer_without_event_time_clock` or `runtime.timer_cap_exceeded`."
  @type refusal :: Error.t()
  @type t :: %__MODULE__{}

  defmodule Session do
    @moduledoc false
    defstruct [:pending, :policy, :now_ms, ops: []]
    @type t :: %__MODULE__{}
  end

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc "The set holding `pending`, `{kind, at}` pairs."
  @spec of([{kind(), non_neg_integer()}]) :: t()
  def of(pending) do
    Enum.reduce(pending, new(), fn {kind, at}, t -> elem(fold_op(t, register_op(kind, at)), 1) end)
  end

  @spec register_op(kind(), non_neg_integer()) :: op()
  def register_op(kind, at), do: %{kind: kind, scheduled_at: at, register: true}

  @spec cancel_op(kind(), non_neg_integer()) :: op()
  def cancel_op(kind, at), do: %{kind: kind, scheduled_at: at, register: false}

  @spec contains?(t(), kind(), non_neg_integer()) :: boolean()
  def contains?(%__MODULE__{} = t, kind, at), do: :gb_sets.is_element(at, Map.fetch!(t, kind))

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{} = t),
    do: :gb_sets.size(t.event_time) + :gb_sets.size(t.processing_time)

  # --- the session ---

  @doc "A session over a COPY of `pending`; `firing` seeds it with that timer's removal as op 0."
  @spec session(t(), policy(), non_neg_integer(), {kind(), non_neg_integer()} | nil) ::
          Session.t()
  def session(pending, policy, now_ms, firing \\ nil)

  def session(%__MODULE__{} = pending, policy, now_ms, nil),
    do: %Session{pending: pending, policy: policy, now_ms: now_ms}

  def session(%__MODULE__{} = pending, policy, now_ms, {kind, at}) do
    {_changed?, pending} = fold_op(pending, cancel_op(kind, at))

    %Session{pending: pending, policy: policy, now_ms: now_ms, ops: [cancel_op(kind, at)]}
  end

  @doc "The invocation's `now`, which `register_after/2` anchors on."
  @spec now(Session.t()) :: non_neg_integer()
  def now(%Session{now_ms: ms}), do: ms

  @doc "Register `kind` at `at`: the absolute timestamp, or a refusal with the set unchanged."
  @spec register(Session.t(), kind(), non_neg_integer()) ::
          {:ok, non_neg_integer(), Session.t()} | {:error, refusal(), Session.t()}
  def register(%Session{policy: policy} = s, kind, at) do
    cond do
      kind == :event_time and policy.clock != :event_time ->
        refuse(
          s,
          Error.new("runtime.event_time_timer_without_event_time_clock",
            tasklet: policy.tasklet,
            clock: policy.clock
          )
        )

      not contains?(s.pending, kind, at) and size(s.pending) >= policy.cap ->
        refuse(
          s,
          Error.new("runtime.timer_cap_exceeded", tasklet: policy.tasklet, max: policy.cap)
        )

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

  @doc """
  Cancel by the scheduled timestamp; whether one was pending here. The op is
  recorded either way: a Cell restored by the core holds timers its host copy
  never saw.
  """
  @spec cancel(Session.t(), kind(), non_neg_integer()) :: {boolean(), Session.t()}
  def cancel(%Session{} = s, kind, at) do
    op = cancel_op(kind, at)
    {changed?, pending} = fold_op(s.pending, op)
    {changed?, %{s | pending: pending, ops: [op | s.ops]}}
  end

  @doc "The copy as the live set, and the ops to record in order."
  @spec commit(Session.t()) :: {t(), [op()]}
  def commit(%Session{} = s), do: {s.pending, Enum.reverse(s.ops)}

  # A registration joins the session only when it changed the copy.
  defp note(%Session{} = s, op) do
    case fold_op(s.pending, op) do
      {true, pending} -> {true, %{s | pending: pending, ops: [op | s.ops]}}
      {false, _} -> {false, s}
    end
  end

  defp refuse(%Session{} = s, refusal), do: {:error, refusal, s}

  defp fold_op(%__MODULE__{} = t, %{kind: kind, scheduled_at: at, register: register}) do
    set = Map.fetch!(t, kind)
    changed? = :gb_sets.is_element(at, set) != register
    set = if register, do: :gb_sets.add(at, set), else: :gb_sets.delete_any(at, set)
    {changed?, Map.put(t, kind, set)}
  end
end
