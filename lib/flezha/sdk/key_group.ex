defmodule Flezha.SDK.KeyGroup do
  @moduledoc """
  Key groups: the hash that puts a routing key in one of #{256} groups, and the
  partition key-group range twin of the Rust `KeyGroupRange`, decoded from
  `AddRelay` fields 7–10. `partition_count < 2` is unpartitioned (one
  comparison).

  The hash is FNV-1a-128 with the constants frozen by every SDK's `StableId`.
  The controller splits and assigns ranges with this same hash, so a key a
  partition admits here is a key the controller routed to it.
  """

  import Bitwise

  @enforce_keys [:index, :count, :from, :to]
  defstruct [:index, :count, :from, :to]

  @type t :: %__MODULE__{
          index: non_neg_integer(),
          count: non_neg_integer(),
          from: non_neg_integer(),
          to: non_neg_integer()
        }

  @key_groups 256

  @offset_basis 0x6C62272E07BB014262B821756295C58D
  @prime 0x0000000001000000000000000000013B
  @mask 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF

  @doc "How many key groups the space is divided into."
  @spec key_groups() :: pos_integer()
  def key_groups, do: @key_groups

  @doc """
  FNV-1a-128 over `bytes`, starting from the frozen offset basis.
  """
  @spec fnv1a128(binary()) :: non_neg_integer()
  def fnv1a128(bytes) when is_binary(bytes), do: fnv1a128(@offset_basis, bytes)

  @doc """
  FNV-1a-128 continuing from `hash` — the chaining form the `StableId` fixture
  vectors are built from (identity, then big-endian seq, then ordinal).
  """
  @spec fnv1a128(non_neg_integer(), binary()) :: non_neg_integer()
  def fnv1a128(hash, <<byte, rest::binary>>),
    do: fnv1a128(band(bxor(hash, byte) * @prime, @mask), rest)

  def fnv1a128(hash, <<>>), do: hash

  @doc """
  The key group a routing key belongs to: `fnv1a128(key) mod 256`.
  """
  @spec group_of(binary()) :: non_neg_integer()
  def group_of(key) when is_binary(key), do: band(fnv1a128(key), @key_groups - 1)

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
    group = group_of(key)

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
