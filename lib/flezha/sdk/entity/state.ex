defmodule Flezha.SDK.Entity.State do
  @moduledoc """
  One Entity's state, shared by its process (`Flezha.SDK.Entity`: the mailbox,
  the ladder, the park) and its durable half (`Flezha.SDK.Entity.Records`: the
  records, the slot, the restore and the snapshot).
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
    held?: false,
    park_id: nil,
    park_seq: 0,
    waiters: :queue.new()
  ]

  @doc "`<operative>/<node>/<key>` — the Entity's name in logs and park ids."
  def label(%__MODULE__{operative_id: oid, node_name: name, entity_key: key}) do
    "#{oid}/#{name || "node"}/#{key || "single"}"
  end
end
