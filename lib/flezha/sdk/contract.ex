defmodule Flezha.SDK.Contract do
  @moduledoc """
  The directory the contract set this package reads is resolved in: the schema
  the codec compiles, and the fixtures its suite is held to.

  `$FLEZHA_CONTRACT_DIR` when it is set, else `contract/` in the package, where a
  release's fixture archive is unpacked, else the core tree the package sits in
  as a submodule. The first that resolves is the answer, and none resolving is a
  refusal naming all three rather than a skipped read. A directory named by the
  variable is taken as given; the other two resolve when they hold the set's
  own list, `test/fixtures/contract.txt`.
  """

  @package Path.expand("../../..", __DIR__)

  @doc "The contract directory, resolved once per call in the order above."
  @spec dir!() :: Path.t()
  def dir! do
    explicit = System.get_env("FLEZHA_CONTRACT_DIR")

    candidates = [
      {"contract/", Path.join(@package, "contract")},
      {"the enclosing core tree", Path.expand("../..", @package)}
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
