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

  test "a member the core tree lacks resolves from the package's copy", %{tmp_dir: checkout} do
    package = Path.join(checkout, "sdk/elixir")
    member = "flz_transport/errors.json"
    File.mkdir_p!(Path.join(package, "contract/flz_transport"))
    File.write!(Path.join([package, "contract", member]), "{}")

    assert Contract.locate!(package, nil, member) == Path.join(package, "contract")

    File.mkdir_p!(Path.join(checkout, "flz_transport"))
    File.write!(Path.join(checkout, member), "{}")
    assert Contract.locate!(package, nil, member) == checkout
  end

  test "no candidate holding the member is a refusal naming it", %{tmp_dir: checkout} do
    package = Path.join(checkout, "sdk/elixir")

    assert_raise RuntimeError, ~r/no contract set holds flz_transport\/errors.json/, fn ->
      Contract.locate!(package, nil, "flz_transport/errors.json")
    end
  end

  defp carry_manifest(tree) do
    File.mkdir_p!(Path.join(tree, "test/fixtures"))
    File.write!(Path.join(tree, "test/fixtures/contract.txt"), "")
  end
end
