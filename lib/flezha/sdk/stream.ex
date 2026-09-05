defmodule Flezha.SDK.Stream do
  @moduledoc """
  A builder-scoped stream handle. Carries the channel it flows from and the
  chain it is currently appending links to; the links themselves live in the
  builder's record, so the handle is a cursor and nothing more. Immutable:
  reusing a handle in two `into`/`sink` calls fans out, and a handle with no
  chain yet opens a fresh one. Valid only inside `channels/1`.
  """

  @enforce_keys [:builder, :source]
  defstruct [:builder, :source, chain_id: nil]

  @type source :: {:entry, String.t()} | {:node, reference()}
  @type t :: %__MODULE__{
          builder: reference(),
          source: source(),
          chain_id: reference() | nil
        }
end
