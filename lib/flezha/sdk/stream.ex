defmodule Flezha.SDK.Stream do
  @moduledoc """
  A builder-scoped stream handle. Carries the source node it flows from and the
  pending edge transforms accumulated on it (fused into one closure per consumer
  edge at build time). Immutable: reusing a handle in two `into`/`sink` calls
  fans out. Valid only inside `channels/1`.
  """

  @enforce_keys [:builder, :source]
  defstruct [
    :builder,
    :source,
    chain: [],
    keyed: false,
    terminal: false,
    chain_id: nil,
    on_full: nil,
    retry_max: nil,
    retry_backoff_ms: nil
  ]

  @type source :: {:entry, String.t()} | {:node, reference()}
  @type t :: %__MODULE__{
          builder: reference(),
          source: source(),
          chain: [term()],
          keyed: boolean(),
          terminal: boolean(),
          chain_id: reference() | nil,
          on_full: :drop_nothing | :drop_newest | :drop_oldest | nil,
          retry_max: pos_integer() | nil,
          retry_backoff_ms: non_neg_integer() | nil
        }
end
