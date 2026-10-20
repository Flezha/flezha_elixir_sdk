defmodule Flezha.SDK.PublicNamesTest do
  @moduledoc """
  The contract set's `sdk_public_names.txt`, Elixir column: every name a row
  gives is public (an exported function with docs not hidden, or a callback of
  the behaviour), and every `!` name is not (absent, or hidden from the docs).
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.Contract

  @elixir 5

  defp names do
    "test/fixtures/sdk_public_names.txt"
    |> Contract.path!()
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Enum.flat_map(fn row ->
      cells = String.split(row, "|")
      for name <- String.split(Enum.at(cells, @elixir), " ", trim: true), do: {hd(cells), name}
    end)
    |> Enum.reject(&match?({_concept, "-"}, &1))
  end

  test "every concept's Elixir names are public, and every ! name is not" do
    assert names() != []

    for {concept, name} <- names() do
      case name do
        "!" <> hidden -> refute public?(hidden), "#{concept}: #{hidden} must not be public"
        shown -> assert public?(shown), "#{concept}: #{shown} is not public"
      end
    end
  end

  defp public?(name) do
    [path, arity] = String.split(name, "/")
    {module, [fun]} = path |> String.split(".") |> Enum.split(-1)
    module = Module.concat(module)
    {fun, arity} = {String.to_atom(fun), String.to_integer(arity)}

    Code.ensure_loaded?(module) and documented?(module, fun, arity) and
      (function_exported?(module, fun, arity) or callback?(module, fun, arity))
  end

  defp callback?(module, fun, arity),
    do:
      function_exported?(module, :behaviour_info, 1) and
        {fun, arity} in module.behaviour_info(:callbacks)

  # A module or function whose docs are hidden is not public, exported or not;
  # a struct's `__struct__/0` is hidden in every module and stands for its type.
  defp documented?(module, fun, arity) do
    {:docs_v1, _, _, _, module_doc, _, docs} = Code.fetch_docs(module)

    module_doc != :hidden and
      (fun == :__struct__ or
         not Enum.any?(docs, &match?({{_kind, ^fun, ^arity}, _, _, :hidden, _}, &1)))
  end
end
