defmodule Flezha.SDK.Entity.State do
  @moduledoc """
  One Entity's state, shared by its process (`Flezha.SDK.Entity`: the mailbox,
  the ladder, the park) and its durable half (`Flezha.SDK.Entity.Records`: the
  records, the slot, the restore and the snapshot).

  Three map fields keep the struct flat (credo refuses a 32nd, which is the
  Erlang flat-map boundary): everything timer-shaped is `timers`, everything
  the park owns is `park` (its policy included), and everything the
  effectively-once tier owns is
  `tier`.
  """

  defstruct [
    :operative,
    :operative_id,
    :node_ref,
    :node_name,
    :input_channel,
    :module,
    :entity_key,
    :capacity,
    :policy,
    :retry_max,
    :retry_backoff_ms,
    :counters,
    :identity,
    :slots,
    :credit,
    ctx_opts: [],
    # One field, so the struct stays a flat map: the pending set, the declared
    # clock, the BEAM timer refs armed for processing time, and the event-time
    # high-water.
    timers: %{pending: %Flezha.SDK.Timers{}, clock: :wall, refs: %{}, high_water: nil},
    # One field again, for the same reason: whether the class declares the
    # effectively-once tier, its checkpoint policy, the node's sink routes the
    # Entity captures its outputs through, whether it has an inter-procedure
    # edge, the open interval, the consumed floor, the armed deadline, the
    # records owed to an edge report, the declared correction policy and the
    # replay slice buffered until its end marker.
    tier: %{
      on?: false,
      checkpoint: nil,
      sink_routes: [],
      edges?: false,
      staged: %Flezha.SDK.Entity.Interval{},
      floor: -1,
      deadline: nil,
      owed: :queue.new(),
      correction: nil,
      slice: []
    },
    changeful?: false,
    persistent?: false,
    schema_version: 1,
    change_seq: 0,
    last_input_ms: 0,
    secondary: nil,
    proc_state: nil,
    queue: :queue.new(),
    size: 0,
    processing?: false,
    # One field, so the struct stays flat: whether a park holds the queue, the
    # ack-correlation id of the park that holds it and the next park's ordinal.
    # `enabled?` is false for a workflow stage's Entity: an exhausted ladder
    # there fails the procedure rather than parking the input to the DLQ.
    park: %{held?: false, id: nil, seq: 0, enabled?: true},
    waiters: :queue.new()
  ]

  @doc "`<operative>/<node>/<key>` — the Entity's name in logs and park ids."
  def label(%__MODULE__{operative_id: oid, node_name: name, entity_key: key}) do
    "#{oid}/#{name || "node"}/#{key || "single"}"
  end
end
