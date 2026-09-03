defmodule Flezha.SDK.Workflow.Instance do
  @moduledoc """
  One workflow instance's runtime state, as the driver process holds it: the
  inbox a `:per_input` loop consumes, the compensation markers a saga reads,
  and the committed proc-journal the bind carried for a crash-resume.

  It is a struct rather than a process. The reference carries a cancellation
  flag, a restore condition variable and a bounded channel here because its
  driver is a pool thread; on the BEAM the driver *is* a process, so a stop is
  a message it receives, the restore gate is a `receive`, and the inbox is a
  queue this struct owns. What is left is the state itself, which is what the
  journal, the resume and the unwind all read.
  """

  alias Flezha.SDK.Workflow.Spec

  defmodule Item do
    @moduledoc "One post-start input with its durable-inbox offset; `0` when unjournaled."
    defstruct [:payload, offset: 0]
    @type t :: %__MODULE__{payload: binary(), offset: non_neg_integer()}
  end

  defmodule Committed do
    @moduledoc "What one committed procedure of an iteration ran."
    defstruct [:proc_idx, :iter, entities_ran: []]

    @type t :: %__MODULE__{
            proc_idx: non_neg_integer(),
            iter: non_neg_integer(),
            entities_ran: [binary()]
          }
  end

  defmodule Resume do
    @moduledoc """
    The committed proc-journal state one bind carried: the iteration to resume
    at, the procedures committed inside it with what each released, the
    previous terminal batch, and every forward commit the compensation planner
    needs.
    """
    defstruct committed_proc_idxs: [],
              iter: 0,
              handoff: [],
              proc_handoffs: %{},
              committed_procs: [],
              consumed_offset: 0,
              rewind_undo: []

    @type t :: %__MODULE__{}
  end

  @enforce_keys [:instance_id, :factory_id, :spec]
  defstruct instance_id: nil,
            factory_id: nil,
            spec: nil,
            inbox: nil,
            owed_inbox: 0,
            stage_entities: %{},
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
      journal: journal,
      iter: if(journal, do: journal.iter, else: 0),
      compensation_done: MapSet.new()
    }
  end

  # --- inbox ---

  @doc "Append one post-start input."
  @spec push_inbox(t(), binary(), non_neg_integer()) :: t()
  def push_inbox(%__MODULE__{} = i, payload, offset \\ 0) do
    item = %Item{payload: payload, offset: offset}
    %{i | inbox: :queue.in(item, i.inbox), owed_inbox: max(i.owed_inbox - 1, 0)}
  end

  @doc "Take the next input, or `:empty`."
  @spec pop_inbox(t()) :: {:ok, Item.t(), t()} | {:empty, t()}
  def pop_inbox(%__MODULE__{} = i) do
    case :queue.out(i.inbox) do
      {{:value, item}, rest} -> {:ok, item, %{i | inbox: rest}}
      {:empty, _} -> {:empty, i}
    end
  end

  @doc "Whether the loop may park: nothing queued and nothing the bind still owes."
  @spec may_park?(t()) :: boolean()
  def may_park?(%__MODULE__{} = i), do: i.owed_inbox == 0 and :queue.is_empty(i.inbox)

  # --- compensation markers ---

  @doc """
  Record what a restore said about one Entity. Only the `done` half is kept:
  it is the one that proves an undo was in flight when the crash landed, and
  what needs undoing comes from the journal's committed procedures. An intent
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

  @doc "Whether any Entity had already compensated before the crash."
  @spec any_compensation_done?(t()) :: boolean()
  def any_compensation_done?(%__MODULE__{} = i), do: MapSet.size(i.compensation_done) > 0

  # --- the journal the bind carried ---

  @doc "The resume this instance carries, consumed once: a re-seed is not a re-resume."
  @spec take_journal(t()) :: {Resume.t() | nil, t()}
  def take_journal(%__MODULE__{} = i), do: {i.journal, %{i | journal: nil}}
end
