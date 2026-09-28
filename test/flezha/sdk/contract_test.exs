defmodule Flezha.SDK.ContractTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.Contract

  @moduletag :tmp_dir

  test "an in-tree contract/ copy does not shadow the core tree", %{tmp_dir: checkout} do
    package = Path.join(checkout, "sdk/elixir")
    carry_manifest(checkout)
    carry_manifest(Path.join(package, "contract"))

    assert Contract.locate!(package, nil) == checkout

    File.rm!(Path.join(checkout, "test/fixtures/contract.txt"))
    assert Contract.locate!(package, nil) == Path.join(package, "contract")
  end

  defp carry_manifest(tree) do
    File.mkdir_p!(Path.join(tree, "test/fixtures"))
    File.write!(Path.join(tree, "test/fixtures/contract.txt"), "")
  end
end
