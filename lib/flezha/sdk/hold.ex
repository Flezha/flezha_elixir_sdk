defmodule Flezha.SDK.Hold do
  @moduledoc """
  Every frame the Elixir SDK writes to a stream, and the guarantee class its
  payload id declares.

  A writer names the frame it is writing and never what happens when the wire
  refuses it. The table below answers that once, for every writer, so adding a
  frame kind is adding a row rather than adding a behaviour — and a payload id
  the table does not name is refused at the call rather than given a default.

  Three classes carry a guarantee:

    * `:control` — refused to its caller, or counted where the caller is a cast,
      and never held. A reply re-driven after its waiter timed out is a stale
      answer, and the reconnect re-issues the request it belonged to.
    * `:durable` — held in a bounded drop-oldest ring, the shed counted where
      every drop-oldest on this wire is, and replayed in write order on the next
      bound stream.
    * `:output` — held already framed, so a keyed emit's channel and its
      `flz.*` identity stamps survive the replay.

  `:probe` carries none: a heartbeat reports a moment that has passed, so
  replaying it after a rebind is worse than losing it. Its verdict is evidence
  instead, on the same flood ladder the shared core's heartbeat feeds.

  The hold is also the Operative's evicted-key trace, which uses `push/2` and
  `member?/2` alone: what a shed *means* is the caller's, and a forgotten
  eviction trace is not a refused frame.
  """

  require Logger

  alias Flezha.SDK.{Counters, Credit, Knob}
  alias Flezha.State.Wire
  alias Flezha.Transport.Stream, as: TransportStream

  @typedoc "The guarantee a frame is owed, or `:probe` for one owed none."
  @type class :: :control | :durable | :output | :probe

  @typedoc """
  What a writer names: a payload id, a `control_command` and its `Command`
  variant, or an operative output whose payload id is the operative id.
  """
  @type key :: String.t() | {String.t(), atom()} | {:output, String.t()}

  @typedoc "A body, its metadata, and optionally the key that picks the stream."
  @type payload :: {binary(), map()} | {binary(), map(), term()}

  @typedoc "What a write answers: written, held with the reason, or refused."
  @type verdict :: :ok | {:held, term()} | {:error, term()} | :missed | :reconnect

  @type t :: %__MODULE__{}

  defstruct queue: nil,
            n: 0,
            cap: nil,
            streams: [],
            counters: nil,
            credit: nil,
            misses: [],
            asked_at: nil

  # payload id -> class. `control_command` is the one id that needs a second
  # key: the frame's `Command` variant, because a shutdown state ship is a
  # durable record and every other variant is a control frame.
  @classes %{
    Wire.payload_id_entity_state_change() => :durable,
    Wire.payload_id_entity_effect_record() => :durable,
    Wire.payload_id_entity_state_snapshot() => :durable,
    Wire.payload_id_delete_entity_state() => :durable,
    Wire.payload_id_compensation_intent() => :durable,
    Wire.payload_id_compensation_done() => :durable,
    Wire.payload_id_park_payload() => :durable,
    # The controller re-raises a lost correction when the input is re-delivered,
    # so nothing is owed a hold and the refusal is the caller's to read.
    "correction_request" => :control,
    "control_ping" => :control,
    "connect_control_channel" => :control,
    "data_heartbeat" => :probe
  }

  # The `Command` variant a `control_command` carries. A shutdown state ship is
  # the operative's final state and is durable; every other variant is a
  # request or a reply whose reconnect re-issues it, or whose caller is told the
  # write was refused, as an admin request's is.
  @command_classes %{
    runner_shutdown_notification: :durable,
    update_factories: :control,
    admin_request: :control,
    spawn_workflow_request: :control,
    proc_commit: :control,
    iter_commit: :control,
    iter_park: :control,
    workflow_completed: :control,
    response: :control
  }

  # The payload ids the controller's credit window is offered against, so a
  # replayed frame debits the session that took it rather than the one that
  # refused it. A compaction snapshot and an eviction's delete ride outside the
  # window, as the shared core's do.
  @credited [
    Wire.payload_id_entity_state_change(),
    Wire.payload_id_entity_effect_record(),
    "correction_request"
  ]

  @doc """
  An empty hold. `:cap` bounds it — the reconnect cap when none is given —
  `:counters` is where a shed is counted, `:streams` what a write goes to, and
  `:credit` the window this writer's records are debited against.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    %__MODULE__{
      queue: :queue.new(),
      cap: declared_cap(Keyword.get(opts, :cap)),
      counters: Keyword.get(opts, :counters),
      credit: Keyword.get(opts, :credit),
      streams: Keyword.get(opts, :streams, [])
    }
  end

  @doc """
  The reconnect hold's declared cap: `FLEZHA_RECONNECT_RING_CAPACITY`, else 1024.

  An owner of more than one hold reads it once and states it on each, so its
  holds are bounded by one number instead of agreeing by accident: the
  variable is process-wide and every construction reads it afresh.
  """
  @spec cap() :: pos_integer()
  def cap, do: Knob.get(:reconnect_ring_capacity)

  @doc "The streams a write goes to; `[]` while nothing is bound."
  @spec bind(t(), [term()]) :: t()
  def bind(%__MODULE__{} = hold, streams), do: %{hold | streams: streams}

  @doc "Whether a stream is bound at all."
  @spec bound?(t()) :: boolean()
  def bound?(%__MODULE__{streams: streams}), do: streams != []

  @doc """
  Which of `secondaries` a key's frames go on. One stream per key, so every
  writer of them picks the same one and their order on the wire is the order
  they were written: an Entity and the Operative replaying what that Entity
  handed on when it stopped agree by computing this, never by comparing notes.
  """
  @spec secondary_for([term(), ...], term()) :: term()
  def secondary_for([_ | _] = secondaries, key),
    do: Enum.at(secondaries, rem(:erlang.phash2(key), length(secondaries)))

  @doc "The guarantee class `key` declares; `:error` when the table has no row."
  @spec class(key()) :: {:ok, class()} | :error
  def class({:output, _operative_id}), do: {:ok, :output}

  def class({"control_command", variant}) when is_atom(variant),
    do: Map.fetch(@command_classes, variant)

  def class(payload_id) when is_binary(payload_id), do: Map.fetch(@classes, payload_id)

  def class(_other), do: :error

  @doc """
  Write `payload` under the guarantee class `key` declares, and answer both the
  verdict and the hold the answer left behind.

  A `:durable` or `:output` frame the wire refuses is held here and replayed by
  `replay/1`; its `{:held, reason}` still names the refusal, for the one caller
  that reads it. A `:control` frame is never held. A `:probe` reaches every
  bound stream and answers `:reconnect` when the misses cross the flood ladder.
  """
  @spec write(key(), t(), payload()) :: {verdict(), t()}
  # A writer that names no route sends on the stream its own payload id
  # chooses, so every frame of one kind keeps its order on one stream.
  def write(key, %__MODULE__{} = hold, {body, meta}),
    do: write(key, hold, {body, meta, payload_id(key)})

  def write(key, %__MODULE__{} = hold, {_body, _meta, _route} = payload) do
    case class(key) do
      {:ok, class} ->
        answer(class, key, hold, payload)

      :error ->
        raise ArgumentError,
              "no guarantee class declares #{inspect(key)}; add its row to " <>
                "Flezha.SDK.Hold rather than choosing a policy at the write"
    end
  end

  @doc """
  Replay everything held, oldest first, onto the streams now bound. One the wire
  refuses again re-enters the hold behind the rest, as every replay here does.
  """
  @spec replay(t()) :: t()
  def replay(%__MODULE__{n: 0} = hold), do: hold
  def replay(%__MODULE__{streams: []} = hold), do: hold

  def replay(%__MODULE__{} = hold) do
    {items, empty} = drain(hold)

    Enum.reduce(items, empty, fn {key, body, meta, route}, acc ->
      {_verdict, acc} = write(key, acc, {body, meta, route})
      acc
    end)
  end

  @doc "Push `item`; at the cap the oldest goes, so the hold cannot outgrow it."
  @spec push(t(), term()) :: t()
  def push(%__MODULE__{queue: queue, n: n, cap: cap} = hold, item) when n >= cap,
    do: %{hold | queue: :queue.in(item, :queue.drop(queue))}

  def push(%__MODULE__{queue: queue, n: n} = hold, item),
    do: %{hold | queue: :queue.in(item, queue), n: n + 1}

  @doc "Everything held, oldest first, and an empty hold to replay it into."
  @spec drain(t()) :: {[term()], t()}
  def drain(%__MODULE__{n: 0} = hold), do: {[], hold}

  def drain(%__MODULE__{queue: queue} = hold),
    do: {:queue.to_list(queue), %{hold | queue: :queue.new(), n: 0}}

  @doc "Whether `item` is still remembered."
  @spec member?(t(), term()) :: boolean()
  def member?(%__MODULE__{queue: queue}, item), do: :queue.member(item, queue)

  # A cap is a positive integer or it is not a cap. The variable path
  # refuses a non-positive `FLEZHA_RECONNECT_RING_CAPACITY` and falls back
  # to the declared default; a caller's `0` is truthy in Elixir, so the
  # same fallback has to be said here. A cap of zero would make `push/2`
  # drop from an empty queue and raise on the first frame the wire refused.
  defp declared_cap(cap) when is_integer(cap) and cap > 0, do: cap
  defp declared_cap(_), do: cap()

  # --- the classes ---

  # The payload id `key` writes under.
  defp payload_id({:output, operative_id}), do: operative_id
  defp payload_id({payload_id, _variant}), do: payload_id
  defp payload_id(payload_id) when is_binary(payload_id), do: payload_id

  defp answer(:control, key, hold, {body, meta, route}),
    do: {send_one(hold, key, body, meta, route), hold}

  defp answer(:probe, key, hold, {body, meta, _route}) do
    # Delivered when ANY bound stream took it: the rule the shared core's
    # heartbeat uses, so a runner with one live and one dead secondary does not
    # reconnect for ever.
    delivered? =
      Enum.reduce(hold.streams, false, fn handle, acc ->
        TransportStream.send_frame(handle, payload_id(key), body, meta) == :ok or acc
      end)

    if delivered?, do: {:ok, %{hold | misses: []}}, else: note_miss(hold)
  end

  defp answer(class, key, hold, {body, meta, route}) when class in [:durable, :output] do
    case send_one(hold, key, body, meta, route) do
      :ok -> {:ok, hold}
      {:error, reason} -> {{:held, reason}, keep(hold, {key, body, meta, route}, reason)}
    end
  end

  defp send_one(%__MODULE__{streams: []}, _key, _body, _meta, _route), do: {:error, :no_stream}

  defp send_one(%__MODULE__{streams: streams, credit: credit}, key, body, meta, route) do
    id = payload_id(key)
    # Debit-on-send: the window is debited by the WRITE and never by the hold,
    # so a frame that reaches no wire costs nothing and a replayed one debits
    # the session that takes it.
    if credit && id in @credited, do: Credit.acquire(credit, byte_size(body))
    TransportStream.send_frame(secondary_for(streams, route), id, body, meta)
  end

  # A shed is a real loss and is counted where every drop-oldest on this wire
  # is; a silent one is what the hold exists to stop. One line opens the burst,
  # as the shared core's ring does, rather than one per frame.
  defp keep(%__MODULE__{} = hold, {key, _body, _meta, _route} = item, reason) do
    cond do
      hold.n >= hold.cap and hold.counters ->
        Counters.inc_drops(hold.counters, :drop_oldest)

      hold.n == 0 ->
        Logger.warning(
          "[Flezha.SDK] #{payload_id(key)} held for the next bound stream: #{inspect(reason)}"
        )

      true ->
        :ok
    end

    push(hold, item)
  end

  # The flood ladder the shared core runs on its own heartbeat: a miss inside
  # the window counts, the threshold asks for a reconnect once, and the ask
  # re-arms after the retry window so an unanswered one cannot disarm it.
  defp note_miss(%__MODULE__{misses: misses, asked_at: asked_at} = hold) do
    now = System.monotonic_time(:millisecond)
    recent = Enum.filter(misses, &(now - &1 <= Knob.get(:broken_pipe_window_ms)))
    armed? = asked_at == nil or now - asked_at >= Knob.get(:reconnect_request_retry_ms)

    if armed? and length(recent) + 1 >= Knob.get(:broken_pipe_reconnect_threshold),
      do: {:reconnect, %{hold | misses: [], asked_at: now}},
      else: {:missed, %{hold | misses: [now | recent]}}
  end
end
