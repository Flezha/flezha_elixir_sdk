defmodule Flezha.SDK.KeyGroup do
  @moduledoc """
  The partition key-group range twin of the Rust `KeyGroupRange`, decoded from
  `AddOperative` fields 7–10. `partition_count < 2` is unpartitioned (one
  comparison). The hash is a direct call to `Flezha.Partition.group_of/1` — the
  controller's own pinned FNV-1a-128 — reached through this SDK seam so the
  packaging extraction stays mechanical.
  """

  @enforce_keys [:index, :count, :from, :to]
  defstruct [:index, :count, :from, :to]

  @type t :: %__MODULE__{
          index: non_neg_integer(),
          count: non_neg_integer(),
          from: non_neg_integer(),
          to: non_neg_integer()
        }

  @key_groups 256

  @doc "Decode `{partition_index, partition_count, key_group_from, key_group_to}`."
  @spec decode(non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: t()
  def decode(index, count, from, to) do
    if count < 2 do
      %__MODULE__{index: 0, count: 0, from: 0, to: @key_groups - 1}
    else
      %__MODULE__{index: index, count: count, from: from, to: to}
    end
  end

  @spec partitioned?(t()) :: boolean()
  def partitioned?(%__MODULE__{count: count}), do: count >= 2

  @doc "`:ok` when the key's group is in range, else a Rust-worded contract error."
  @spec admit(t(), String.t()) :: :ok | {:error, String.t()}
  def admit(%__MODULE__{count: count}, _key) when count < 2, do: :ok

  def admit(%__MODULE__{index: index, count: count, from: from, to: to}, key) do
    group = Flezha.Partition.group_of(key)

    if group >= from and group <= to do
      :ok
    else
      {:error,
       "key '#{key}' hashes to key group #{group}, which partition #{index} of #{count} " <>
         "does not own (it owns groups #{from}..#{to}) — the routing item / flz.key must " <>
         "equal the key_by result"}
    end
  end

  @spec owns?(t(), String.t()) :: boolean()
  def owns?(%__MODULE__{count: count}, _key) when count < 2, do: true
  def owns?(range, key), do: match?(:ok, admit(range, key))
end
