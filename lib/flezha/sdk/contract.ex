defmodule Flezha.SDK.Contract do
  @moduledoc """
  The directory the contract set this package reads is resolved in: the schema
  the codec compiles, and the fixtures its suite is held to.

  `$FLEZHA_CONTRACT_DIR` when it is set, else the core tree the package sits in
  as a submodule, else `contract/` in the package, where a release's fixture
  archive is unpacked. The core tree comes first so a stale copy left in the
  package never shadows its live files. The first that resolves is the answer,
  and none resolving is a refusal naming all three rather than a skipped read.
  A directory named by the variable is taken as given; the other two resolve
  when they hold the set's own list, `test/fixtures/contract.txt`.
  """

  @package Path.expand("../../..", __DIR__)

  @doc "The contract directory, resolved once per call in the order above."
  @spec dir!() :: Path.t()
  def dir!, do: locate!(@package, System.get_env("FLEZHA_CONTRACT_DIR"))

  @doc "The contract directory for a package at `package`, given `$FLEZHA_CONTRACT_DIR`."
  @spec locate!(Path.t(), String.t() | nil) :: Path.t()
  def locate!(package, explicit) do
    candidates = [
      {"the enclosing core tree", Path.expand("../..", package)},
      {"contract/", Path.join(package, "contract")}
    ]

    explicit || Enum.find_value(candidates, &resolved/1) ||
      raise "no contract set: looked at " <>
              "$FLEZHA_CONTRACT_DIR (unset), " <>
              Enum.map_join(candidates, ", ", fn {name, dir} -> "#{name} (#{dir})" end) <>
              ". Unpack a release's fixture archive at contract/ or set FLEZHA_CONTRACT_DIR."
  end

  @doc "`relative` inside the contract directory."
  @spec path!(Path.t()) :: Path.t()
  def path!(relative), do: Path.join(dir!(), relative)

  defp resolved({_name, dir}),
    do: if(File.regular?(Path.join(dir, "test/fixtures/contract.txt")), do: dir)
end
