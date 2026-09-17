defmodule Flezha.SDK.Merge do
  @moduledoc """
  A merged subscription under construction, built by `Flezha.SDK.Channels.merge/1`:
  one procedure fed by two or more source streams, each with the mapping that
  lifts its value into the merged input.

      c
      |> merge()
      |> from(ticks, &{:tick, &1})
      |> from(quotes, &{:quote, &1})
      |> key_by(&symbol_of/1)
      |> into(Detector)

  Every step is `Flezha.SDK.Channels`', dispatching on the handle it receives,
  so one import writes a whole pipe. This module is the handle they carry
  between them: the sources recorded so far, each already mapped, and the key
  the merged node is declared on. Every source the author wrote reaches the
  record, a repeated one included; `Flezha.SDK.Topology` is what names
  `MergeTooFewSources` and `DuplicateMergeSource`.
  """

  alias Flezha.SDK.Stream

  @enforce_keys [:builder]
  defstruct [:builder, sources: [], keys: []]

  @type t :: %__MODULE__{
          builder: reference(),
          sources: [Stream.t()],
          keys: [(term() -> term())]
        }
end
