defmodule Flezha.SDK.PersistentId do
  @moduledoc """
  The SDK's durable stable identity, presented on every connect. Format
  `"<namespace>-<profile_hash8>-<rand6>"`, where `profile_hash8` is an
  order-independent FNV-1a-64 over the sorted, NUL-delimited factory ids folded
  to the low 32 bits. Persisted with an atomic write at `FLEZHA_RUNNER_ID_PATH`
  (default `/var/lib/flezha-runner/id`), memoized per boot, and degraded to a
  fresh per-boot id with a single `warn` when the path is unwritable — never a
  refusal to start. Byte-compatible with the Rust `persistent_id.rs` contract.
  """

  require Logger

  @env_path "FLEZHA_RUNNER_ID_PATH"
  @default_path "/var/lib/flezha-runner/id"
  @fnv64_offset 0xCBF29CE484222325
  @fnv64_prime 0x00000100000001B3
  @mask64 0xFFFFFFFFFFFFFFFF
  @alphabet ~c"abcdefghijklmnopqrstuvwxyz0123456789"

  @doc """
  The stable id for this boot. Memoized in `:persistent_term`; loads a persisted
  id if present, else derives and persists a fresh one.
  """
  @spec stable_id(String.t(), [String.t()]) :: String.t()
  def stable_id(namespace, factory_ids) do
    case :persistent_term.get({__MODULE__, :id}, nil) do
      nil ->
        id = derive_or_load(namespace, factory_ids)
        :persistent_term.put({__MODULE__, :id}, id)
        id

      id ->
        id
    end
  end

  @doc false
  def reset_memo, do: :persistent_term.erase({__MODULE__, :id})

  @doc "The order-independent profile hash (8 hex chars) over the factory ids."
  @spec profile_hash8([String.t()]) :: String.t()
  def profile_hash8(factory_ids) do
    hash =
      factory_ids
      |> Enum.sort()
      |> Enum.join(<<0>>)
      |> fnv1a64()

    Integer.to_string(Bitwise.band(hash, 0xFFFFFFFF), 16)
    |> String.downcase()
    |> String.pad_leading(8, "0")
  end

  defp derive_or_load(namespace, factory_ids) do
    path = path()

    case load(path) do
      {:ok, existing} ->
        existing

      :error ->
        id = derive(namespace, factory_ids)
        persist(path, id)
        id
    end
  end

  defp derive(namespace, factory_ids),
    do: "#{namespace}-#{profile_hash8(factory_ids)}-#{rand6()}"

  defp load(path) do
    case File.read(path) do
      {:ok, contents} ->
        trimmed = String.trim(contents)
        if trimmed == "", do: :error, else: {:ok, trimmed}

      {:error, _} ->
        :error
    end
  end

  defp persist(path, id) do
    tmp = path <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(tmp, id),
         :ok <- File.rename(tmp, path) do
      :ok
    else
      {:error, reason} ->
        Logger.warning(
          "[Flezha.SDK] could not persist runner id at #{path} (#{inspect(reason)}); " <>
            "using a fresh per-boot identity"
        )

        :ok
    end
  end

  defp fnv1a64(bin) do
    :binary.bin_to_list(bin)
    |> Enum.reduce(@fnv64_offset, fn byte, hash ->
      Bitwise.band(Bitwise.bxor(hash, byte) * @fnv64_prime, @mask64)
    end)
  end

  defp rand6, do: for(_ <- 1..6, into: "", do: <<Enum.random(@alphabet)>>)

  defp path, do: System.get_env(@env_path, @default_path)
end
