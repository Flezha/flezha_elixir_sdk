defmodule Flezha.SDK.Workflow.Instance do
  @moduledoc """
  One workflow instance's runtime state, as the driver process holds it: the
  queue of triggers every pass starts from (messages and timer firings, in
  arrival order), the compensation markers a saga reads, and the committed
  task-journal the bind carried for a crash-resume.

  It is a struct rather than a process. The reference carries a cancellation
  flag, a restore condition variable and a bounded channel here because its
  driver is a pool thread; on the BEAM the driver *is* a process, so a stop is
  a message it receives, the restore gate is a `receive`, and the inbox is a
  queue this struct owns. What is left is the state itself, which is what the
  journal, the resume and the unwind all read.
  """

  alias Flezha.SDK.Knob
  alias Flezha.SDK.Workflow.Spec

  defmodule Item do
    @moduledoc """
    One message through the entrance: the entry channel it named, its value as
    that channel's parser made it, and its input-log offset (`nil` when
    unjournaled).
    """
    defstruct [:channel, :payload, :offset]

    @type t :: %__MODULE__{
            channel: String.t(),
            payload: term(),
            offset: non_neg_integer() | nil
          }
  end

  defmodule Firing do
    @moduledoc "A due timer of one Cell, waiting for its pass: the node, the Cell's key, the firing."
    defstruct [:node_ref, :cell_key, :fire]

    @type t :: %__MODULE__{node_ref: reference(), cell_key: String.t() | nil, fire: struct()}

    @doc "The firing a journaled trigger names, on `node_ref`."
    @spec from_trigger(reference() | nil, map()) :: t()
    def from_trigger(node_ref, %{cell_key: key, kind: kind, at: at}),
      do: %__MODULE__{
        node_ref: node_ref,
        cell_key: key,
        fire: %Flezha.SDK.Cell.Fire{kind: kind, at: at}
      }

    @doc "The trigger a timer-fed pass's root commit names for this firing."
    @spec trigger(t()) :: map()
    def trigger(%__MODULE__{cell_key: key, fire: fire}),
      do: %{cell_key: key, kind: fire.kind, at: fire.at}
  end

  defmodule Committed do
    @moduledoc "What one committed tasklet of an iteration ran."
    defstruct [:task_idx, :iter, cells_ran: []]

    @type t :: %__MODULE__{
            task_idx: non_neg_integer(),
            iter: non_neg_integer(),
            cells_ran: [binary()]
          }
  end

  defmodule Resume do
    @moduledoc """
    The committed task-journal state one bind carried: the iteration to resume
    at, the tasklets committed inside it with what each released, the
    previous terminal batch, every forward commit the compensation planner
    needs, and the result the last committed iteration ended the instance
    with (`nil` when it did not).
    """
    defstruct committed_task_idxs: [],
              iter: 0,
              entry_channel: "",
              handoff: [],
              task_handoffs: %{},
              committed_tasks: [],
              consumed_offset: nil,
              rewind_undo: [],
              completion: nil

    @type t :: %__MODULE__{}
  end

  @enforce_keys [:instance_id, :factory_id, :spec]
  defstruct instance_id: nil,
            factory_id: nil,
            spec: nil,
            inbox: nil,
            owed_inbox: 0,
            parked_offset: nil,
            park_idle_ms: nil,
            stage_cells: %{},
            journal: nil,
            iter: 0,
            compensation_done: nil,
            cancel: nil

  @type t :: %__MODULE__{}

  @doc "A fresh instance of `spec`, resuming from `journal` when the bind carried one."
  @spec new(String.t(), Spec.t(), keyword()) :: t()
  def new(instance_id, %Spec{} = spec, opts \\ []) do
    journal = Keyword.get(opts, :journal)

    %__MODULE__{
      instance_id: instance_id,
      factory_id: spec.factory_id,
      spec: spec,
      inbox: :queue.new(),
      owed_inbox: Keyword.get(opts, :owed_inbox, 0),
      park_idle_ms: Keyword.get(opts, :park_idle_ms) || Knob.get(:park_idle_ms),
      journal: journal,
      iter: if(journal, do: journal.iter, else: 0),
      compensation_done: MapSet.new()
    }
  end

  # --- inbox ---

  @doc "Append one message on `channel`, at the input-log offset its metadata carries."
  @spec push_inbox(t(), String.t(), term(), map()) :: t()
  def push_inbox(%__MODULE__{} = i, channel, payload, metadata) do
    item = %Item{channel: channel, payload: payload, offset: input_offset(metadata)}
    %{arrived(i) | inbox: :queue.in(item, i.inbox)}
  end

  @doc """
  A message the entrance parked to the dead-letter queue instead of queueing:
  it arrived, so it settles its share of the owed tail, and it is consumed
  without a pass.
  """
  @spec entry_parked(t(), map()) :: t()
  def entry_parked(%__MODULE__{} = i, metadata),
    do: i |> arrived() |> note_parked(input_offset(metadata))

  @doc "A message consumed without a pass: the park record releases its offset."
  @spec note_parked(t(), non_neg_integer() | nil) :: t()
  def note_parked(%__MODULE__{} = i, nil), do: i
  def note_parked(%__MODULE__{parked_offset: nil} = i, offset), do: %{i | parked_offset: offset}

  def note_parked(%__MODULE__{} = i, offset),
    do: %{i | parked_offset: max(i.parked_offset, offset)}

  @doc "Queue a due timer firing behind whatever arrived before it."
  @spec push_firing(t(), Firing.t()) :: t()
  def push_firing(%__MODULE__{} = i, %Firing{} = firing),
    do: %{i | inbox: :queue.in(firing, i.inbox)}

  @doc "Whether this exact firing already waits in the queue."
  @spec queued?(t(), Firing.t()) :: boolean()
  def queued?(%__MODULE__{inbox: inbox}, %Firing{} = firing), do: :queue.member(firing, inbox)

  @doc "Whether nothing waits for a pass."
  @spec idle?(t()) :: boolean()
  def idle?(%__MODULE__{inbox: inbox}), do: :queue.is_empty(inbox)

  @doc "Drop the queued firings `keep?` refuses."
  @spec filter_firings(t(), (Firing.t() -> boolean())) :: t()
  def filter_firings(%__MODULE__{} = i, keep?) do
    %{
      i
      | inbox: :queue.filter(fn item -> not is_struct(item, Firing) or keep?.(item) end, i.inbox)
    }
  end

  @doc """
  Take the first trigger this boundary may start, in arrival order: a firing
  always, a message only where it seeds a pass (`messages?`); or `:empty`.
  """
  @spec pop_trigger(t(), boolean()) :: {:ok, Item.t() | Firing.t(), t()} | {:empty, t()}
  def pop_trigger(%__MODULE__{} = i, messages?),
    do: pop_first(i, &(messages? or is_struct(&1, Firing)))

  @doc """
  Take the first message, leaving any firing queued: a pass the crash
  interrupted resumes over its re-forwarded message before anything else runs.
  """
  @spec pop_inbox(t()) :: {:ok, Item.t(), t()} | {:empty, t()}
  def pop_inbox(%__MODULE__{} = i), do: pop_first(i, &is_struct(&1, Item))

  defp pop_first(%__MODULE__{} = i, eligible?) do
    case Enum.split_while(:queue.to_list(i.inbox), &(not eligible?.(&1))) do
      {_before, []} -> {:empty, i}
      {before, [trigger | rest]} -> {:ok, trigger, %{i | inbox: :queue.from_list(before ++ rest)}}
    end
  end

  @doc """
  How long an empty inbox is waited on before the loop parks: the bound on the
  tail a rehydrating bind still owes, else the park idle window.
  """
  @spec idle_ms(t()) :: non_neg_integer()
  def idle_ms(%__MODULE__{owed_inbox: owed}) when owed > 0,
    do: Knob.get(:sdk_owed_inbox_wait_ms)

  def idle_ms(%__MODULE__{park_idle_ms: ms}), do: ms

  # Any message that arrives, queued or parked, settles its share of the tail
  # a rehydrating bind still owes.
  defp arrived(%__MODULE__{} = i), do: %{i | owed_inbox: max(i.owed_inbox - 1, 0)}

  # The controller stamps a journaled input with its input-log offset; an
  # unjournaled one carries none.
  defp input_offset(metadata) do
    case Integer.parse(Map.get(metadata, "flz.offset", "")) do
      {offset, _rest} -> offset
      :error -> nil
    end
  end

  # --- compensation markers ---

  @doc """
  Record what a restore said about one Cell. Only the `done` half is kept:
  it is the one that proves an undo was in flight when the crash landed, and
  what needs undoing comes from the journal's committed tasklets. An intent
  marker accumulates during ordinary forward progress too, so on its own it
  would say nothing about a rollback.
  """
  @spec note_compensation(t(), binary(), boolean(), boolean()) :: t()
  def note_compensation(%__MODULE__{} = i, _identity, _intent, false), do: i

  def note_compensation(%__MODULE__{} = i, identity, _intent, true),
    do: %{i | compensation_done: MapSet.put(i.compensation_done, identity)}

  @spec compensation_done?(t(), binary()) :: boolean()
  def compensation_done?(%__MODULE__{} = i, identity),
    do: MapSet.member?(i.compensation_done, identity)

  @doc "Whether any Cell had already compensated before the crash."
  @spec any_compensation_done?(t()) :: boolean()
  def any_compensation_done?(%__MODULE__{} = i), do: MapSet.size(i.compensation_done) > 0

  # --- the journal the bind carried ---

  @doc "The resume this instance carries, consumed once: a re-seed is not a re-resume."
  @spec take_journal(t()) :: {Resume.t() | nil, t()}
  def take_journal(%__MODULE__{} = i), do: {i.journal, %{i | journal: nil}}
end
