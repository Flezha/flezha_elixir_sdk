defmodule Flezha.SDK.ResponseTest do
  @moduledoc """
  `Flezha.SDK.Response.decode/2` over the core's decoder: an answer read
  through the call's `decode:`, the contract set's timeout case, and a payload
  that is no envelope.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Contract, Error, Response}

  test "an answer goes through the call's decode" do
    payload =
      Jason.encode!(%{
        call: "c",
        target: "anna-42",
        created: true,
        request: Base.encode64("anna-42:hello"),
        value: Base.encode64(~s({"tokens":812}))
      })

    assert {:ok, %Response{request: "anna-42:hello", target: "anna-42", created: true} = response} =
             Response.decode(payload, &Jason.decode/1)

    assert response.result == {:ok, %{"tokens" => 812}}
  end

  test "the fixture's timeout case decodes to its expected fields" do
    %{"payload" => payload, "expect" => expect} =
      "test/fixtures/call_response_envelope.json"
      |> Contract.path!()
      |> File.read!()
      |> Jason.decode!()
      |> Map.fetch!("cases")
      |> Enum.find(&(&1["name"] == "error_timeout"))

    assert {:ok, %Response{result: {:error, %Error{code: code}}} = response} =
             Response.decode(payload, &{:ok, &1})

    assert {response.request, response.target, response.created, code} ==
             {Base.decode64!(expect["request"]), expect["target"], expect["created"],
              expect["error_code"]}
  end

  test "a payload that is no envelope is the core's malformed response" do
    assert {:error, %Error{code: "call.malformed_response"}} =
             Response.decode(~s({"target":"t","created":false}), &{:ok, &1})
  end
end
