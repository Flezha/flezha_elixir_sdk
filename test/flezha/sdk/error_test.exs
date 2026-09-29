defmodule Flezha.SDK.ErrorTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.Contract
  alias Flezha.SDK.Error

  test "every case of the shared rendering fixture renders the bytes the Rust renderer does" do
    fixture =
      "test/fixtures/error_rendering.json" |> Contract.path!() |> File.read!() |> Jason.decode!()

    for %{"code" => code, "details" => details, "text" => text, "json" => json} <- fixture do
      error = Error.new(code, details)
      assert Error.render(error, :text) == text, code
      assert Error.render(error, :json) == json, code
    end
  end

  test "a JSON error document reads back as the error it renders" do
    error = Error.new("quota.exceeded", kind: :max_operators, current: 3, limit: 3)

    assert Error.from_document(Error.render(error, :json)) == error
    assert Error.from_document("not json") == nil
  end

  test "a code outside the catalogue is uncatalogued and names itself" do
    assert %Error{
             code: "internal.uncatalogued",
             class: :internal,
             details: %{"reason" => "nope.never"}
           } =
             Error.new("nope.never")
  end

  test "an error nested in another's details is its own object" do
    inner = Error.new("namespace.not_found", namespace: "ghost")
    outer = Error.new("internal.uncatalogued", issues: [inner])

    assert outer.details["issues"] == [
             %{
               "code" => "namespace.not_found",
               "message" => inner.message,
               "details" => inner.details
             }
           ]
  end
end
