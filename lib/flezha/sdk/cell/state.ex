defmodule Flezha.SDK.Cell.State do
  @moduledoc """
  One Cell's state, shared by its process (`Flezha.SDK.Cell`: the mailbox,
  the ladder, the park) and its durable half (`Flezha.SDK.Cell.Records`: the
  records, the slot, the restore and the snapshot).

  Four map fields keep the struct flat. A struct's keys are its fields plus
  `__struct__`, and 32 keys is the Erlang flat-map boundary, so 31 fields is the
  last shape that stays flat; `correction_window_test.exs` pins the count.
  Everything timer-shaped is `timers`, everything the park owns is `park` (its
  policy included), everything the effectively-once tier owns is `tier`, and
  everything the state stream owns is `stream`. A new field joins one of the
  four rather than the top level.
  """

  defstruct [
    :operative,
    :operative_id,
    :node_ref,
    :node_name,
    :input_channel,
    :module,
    :cell_key,
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
    # clock, the BEAM timer refs armed for processing time, the event-time
    # high-water, and for a workflow stage's Cell the gate its due firings pass
    # through (`:settled`, or `:held` when the removal waits for the stage's
    # commit) with the removals held.
    timers: %{
      pending: %Flezha.SDK.Timers{},
      clock: :wall,
      refs: %{},
      high_water: nil,
      gate: nil,
      held: []
    },
    # One field again, for the same reason: whether the class declares the
    # effectively-once tier, its checkpoint policy, the node's sink routes the
    # Cell captures its outputs through, whether it has an inter-tasklet
    # edge, the open interval, the consumed floor, the armed deadline, the
    # records owed to an edge report, the declared correction policy and the
    # replay slice buffered until its end marker.
    tier: %{
      on?: false,
      checkpoint: nil,
      sink_routes: [],
      edges?: false,
      staged: %Flezha.SDK.Cell.Interval{},
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
    # This Cell's write seam: the secondary it writes its records to, chosen
    # by identity, the records no stream would take, and the credit window they
    # are debited against. Seeded in `Flezha.SDK.Cell.init/1`, because its
    # cap is read from the environment.
    stream: nil,
    task_state: nil,
    queue: :queue.new(),
    size: 0,
    processing?: false,
    # One field, so the struct stays flat: whether a park holds the queue, the
    # ack-correlation id of the park that holds it and the next park's ordinal.
    # `enabled?` is false for a workflow stage's Cell: an exhausted ladder
    # there fails the tasklet rather than parking the input to the DLQ.
    park: %{held?: false, id: nil, seq: 0, enabled?: true},
    waiters: :queue.new()
  ]

  @doc "`<operative>/<node>/<key>` — the Cell's name in logs and park ids."
  def label(%__MODULE__{operative_id: oid, node_name: name, cell_key: key}) do
    "#{oid}/#{name || "node"}/#{key || "single"}"
  end
end
