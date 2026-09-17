defmodule Flezha.SDK.Merge do
  @moduledoc """
  A merged subscription under construction, built by `Flezha.SDK.Channels.merge/1`:
  one procedure fed by two or more source streams, each with the mapping that
  lifts its value into the merged input.

      c
      |> Channels.merge()
      |> Merge.from(ticks, &{:tick, &1})
      |> Merge.from(quotes, &{:quote, &1})
      |> Merge.key_by(&symbol_of/1)
      |> Merge.into(Detector)

  `from/3` records the mapping on the source's own chain, after any transform
  that handle already carries; `key_by/2` is declared once, on the merge, and
  runs on the mapped value of every source; `into/2` and `into_named/3` close
  every source chain on one node and answer that node's output stream. Every
  source the author wrote reaches the record, a repeated one included:
  `Flezha.SDK.Topology` is what names `MergeTooFewSources` and
  `DuplicateMergeSource`.
  """

  alias Flezha.SDK.{Channels, Stream}

  @enforce_keys [:builder]
  defstruct [:builder, sources: [], keys: []]

  @type t :: %__MODULE__{
          builder: reference(),
          sources: [Stream.t()],
          keys: [(term() -> term())]
        }

  @doc "Add a source stream and the mapping that lifts its value into the merged input."
  @spec from(t(), Stream.t(), (term() -> term())) :: t()
  def from(%__MODULE__{} = m, %Stream{} = source, mapping) when is_function(mapping, 1),
    do: %{m | sources: m.sources ++ [Channels.map(source, mapping)]}

  @doc """
  Key the merged node on the mapped value. A second declaration is recorded as
  a second key on every source, which the check refuses as `DuplicateKeyBy`.
  """
  @spec key_by(t(), (term() -> term())) :: t()
  def key_by(%__MODULE__{} = m, key) when is_function(key, 1),
    do: %{m | keys: m.keys ++ [key]}

  @doc "Close the merge on a procedure node; unnamed mentions of one module unify."
  @spec into(t(), module()) :: Stream.t()
  def into(%__MODULE__{} = m, module), do: Channels.close_merge(m, module, nil)

  @doc "Close the merge on a named procedure node."
  @spec into_named(t(), module(), String.t()) :: Stream.t()
  def into_named(%__MODULE__{} = m, module, name) when is_binary(name),
    do: Channels.close_merge(m, module, name)
end
