defmodule Flezha.SDK.Entity.Interval do
  @moduledoc """
  One open checkpoint interval's accumulation — the twin of the shared core's
  `StagedInterval`, and the one place the ack granularity's rules live. Nothing
  held here is flushable on its own: it ships in the record that closes the
  interval, or a restore discards it whole.

  `fold/4` takes one invocation's effects and answers `{record | nil, staged}`.
  Under per-input granularity (`policy` is nil) every effectful invocation
  ships its own record, acknowledging its own input. Under checkpoint
  granularity the changes, the timer ops and the last offset stage, the
  interval opens on the first offset, outputs are held or passed by the
  policy's posture, and a close ships one record for the whole interval — on an
  explicit `Ctx.checkpoint/1`, on a park, on `every` inputs, or on the delay.

  `changes`, `timers` and `outputs` accumulate in REVERSE production order and
  are restored to it by the close, so a long interval costs no repeated list
  append; nothing outside this module reads them directly.
  """

  defstruct changes: [],
            timers: [],
            outputs: [],
            offset: nil,
            inputs: 0,
            # nil exactly when the interval is closed.
            opened_at_ms: nil,
            # The last folded invocation's clock value, which a deadline close
            # carries too.
            event_time: 0

  @type policy :: %{
          every: pos_integer() | nil,
          outputs: :as_produced | :at_checkpoint,
          max_delay_ms: non_neg_integer() | nil
        }

  @typedoc "One invocation's durable effects, as the Entity hands them over."
  @type effects :: %{
          changes: [term()],
          timers: [map()],
          outputs: [map()],
          offset: non_neg_integer() | nil,
          event_time: non_neg_integer(),
          checkpointed: boolean(),
          parked: boolean()
        }

  @typedoc "What one effect record carries — the shared core's `ShippedRecord`."
  @type shipped :: %{
          changes: [term()],
          timers: [map()],
          outputs: [map()],
          offset: non_neg_integer() | nil,
          event_time: non_neg_integer()
        }

  @type t :: %__MODULE__{}

  @doc "Fold one invocation in; the record it ships, or nil when an open interval swallowed it."
  @spec fold(t(), policy() | nil, effects(), non_neg_integer()) :: {shipped() | nil, t()}
  def fold(%__MODULE__{} = staged, nil, effects, _now_ms) do
    # Per-input: one record per effectful invocation, each acknowledging its
    # own input. An effect-free invocation contributes nothing durable unless
    # it parked.
    if effects.parked or effects.changes != [] or effects.outputs != [] or effects.timers != [] do
      {%{
         changes: effects.changes,
         timers: effects.timers,
         outputs: effects.outputs,
         offset: effects.offset,
         event_time: effects.event_time
       }, staged}
    else
      {nil, staged}
    end
  end

  def fold(%__MODULE__{} = staged, policy, effects, now_ms) do
    {staged, flushed_now} = stage(staged, policy, effects, now_ms)

    cond do
      effects.checkpointed or effects.parked or due?(staged, policy, now_ms) ->
        # Even a close that accumulated nothing ships: the record exists to
        # carry the offset.
        {record, staged} = close(staged)
        {%{record | outputs: record.outputs ++ flushed_now}, staged}

      flushed_now != [] ->
        # Mid-interval: an as-produced emit still flushes, but in a record
        # acknowledging nothing — the state it was computed from is not durable
        # yet, so a record carrying its offset would let a restore resume past
        # inputs it never saw.
        {%{
           changes: [],
           timers: [],
           outputs: flushed_now,
           offset: nil,
           event_time: effects.event_time
         }, staged}

      true ->
        {nil, staged}
    end
  end

  @doc "Due to close on its own: the input count reached, or the delay passed."
  @spec due?(t(), policy(), non_neg_integer()) :: boolean()
  def due?(%__MODULE__{} = staged, policy, now_ms) do
    (policy.every != nil and staged.inputs >= policy.every) or
      delay_elapsed?(staged, policy, now_ms)
  end

  @doc "When the delay closes this interval, or nil when nothing is open or no delay is declared."
  @spec deadline_ms(t(), policy()) :: non_neg_integer() | nil
  def deadline_ms(%__MODULE__{opened_at_ms: opened}, %{max_delay_ms: delay})
      when is_integer(opened) and is_integer(delay),
      do: opened + delay

  def deadline_ms(%__MODULE__{}, _policy), do: nil

  @doc "Close on the delay, with no invocation of its own; nil when nothing was open."
  @spec close_on_deadline(t()) :: {shipped() | nil, t()}
  def close_on_deadline(%__MODULE__{} = staged) do
    if open?(staged), do: close(staged), else: {nil, staged}
  end

  @doc "Drop the interval: a restore re-seats the Entity on a past in which none of it was applied."
  @spec clear(t()) :: t()
  def clear(%__MODULE__{event_time: event_time}), do: %__MODULE__{event_time: event_time}

  @doc "Whether an interval is open — exactly when an offset opened one."
  @spec open?(t()) :: boolean()
  def open?(%__MODULE__{opened_at_ms: opened}), do: opened != nil

  defp stage(%__MODULE__{} = staged, policy, effects, now_ms) do
    staged = %{
      staged
      | event_time: effects.event_time,
        changes: prepend(staged.changes, effects.changes),
        timers: prepend(staged.timers, effects.timers)
    }

    # Held outputs wait for the close; as-produced ones leave now.
    {staged, flushed_now} =
      if policy.outputs == :at_checkpoint,
        do: {%{staged | outputs: prepend(staged.outputs, effects.outputs)}, []},
        else: {staged, effects.outputs}

    {open(staged, effects.offset, now_ms), flushed_now}
  end

  defp open(staged, nil, _now_ms), do: staged

  defp open(staged, offset, now_ms) do
    %{
      staged
      | offset: offset,
        inputs: staged.inputs + 1,
        opened_at_ms: staged.opened_at_ms || now_ms
    }
  end

  defp close(%__MODULE__{} = staged) do
    record = %{
      changes: Enum.reverse(staged.changes),
      timers: Enum.reverse(staged.timers),
      outputs: Enum.reverse(staged.outputs),
      offset: staged.offset,
      event_time: staged.event_time
    }

    {record, %__MODULE__{event_time: staged.event_time}}
  end

  defp delay_elapsed?(%__MODULE__{opened_at_ms: opened}, %{max_delay_ms: delay}, now_ms)
       when is_integer(opened) and is_integer(delay),
       do: now_ms - opened >= delay

  defp delay_elapsed?(%__MODULE__{}, _policy, _now_ms), do: false

  defp prepend(acc, []), do: acc
  defp prepend(acc, items), do: Enum.reduce(items, acc, &[&1 | &2])
end
