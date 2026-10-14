defmodule Flezha.SDK.DeclarationTest do
  @moduledoc """
  A relay module as the class declaration `flz_core` judges: what the builder
  records, the defaults the Elixir surface implies and declares outright, and
  the refusals the surface makes itself.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Declaration, Error}

  defmodule Entry do
    use Flezha.SDK.Tasklet, name: "entry"
    @impl true
    def run(input, state, ctx) do
      emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule Next do
    use Flezha.SDK.Tasklet, name: "next"
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule Hop do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "hop"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(Entry)
        |> Channels.named("hop")
        |> Channels.into(Next)
  end

  defmodule Twice do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "twice"

    def channels(c) do
      first = c |> Channels.input("a") |> Channels.into_named(Entry, "first")
      c |> Channels.input("b") |> Channels.into_named(Entry, "second") |> Channels.output("out")
      first |> Channels.output("first_out")
    end
  end

  defmodule Keyset do
    use Flezha.SDK.Tasklet, name: "keyset", cell_keys: ["a", "b", "c", "d"]
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule Compensating do
    use Flezha.SDK.Tasklet,
      name: "compensating",
      schema_version: 3,
      min_readable_schema_version: 2

    defchange seen(state, item) do
      [item | state]
    end

    @impl true
    def init_state(_key), do: []
    @impl true
    def run(input, state, ctx), do: {:ok, seen(state, ctx, input)}
    @impl true
    def compensate(_outputs, state, _ctx), do: {:ok, state}
    @impl true
    def event_time(_input), do: 0
  end

  defmodule Defaults do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "defaults"
    def run, do: {:steps, each: :per_input}

    def channels(c),
      do: c |> Channels.input("in") |> Channels.into(Keyset) |> Channels.into(Compensating)
  end

  defmodule RouterNoKeys do
    use Flezha.SDK.Tasklet, name: "router_no_keys"
    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
    @impl true
    def cells_for(_input), do: ["a"]
  end

  defmodule SurfaceRefused do
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "surface_refused"

    def channels(c) do
      c |> Channels.input("in") |> Channels.keyed()
      c |> Channels.input("other") |> Channels.into(RouterNoKeys)
    end
  end

  defp class(module) do
    {:ok, class} = Declaration.build(module)
    Jason.decode!(class.json)
  end

  test "named/2 names the channel and leaves the tasklet its own name" do
    decl = class(Hop)
    assert Enum.map(decl["tasklets"], & &1["name"]) == ["entry", "next"]

    assert %{"name" => "hop", "chains" => [[%{"link" => "named", "name" => "hop"} | _]]} =
             Enum.find(decl["channels"], &(&1["kind"] == "inter_task"))

    assert {:ok, derived} = Declaration.check(Hop)
    assert Enum.map(derived["channels"], & &1["name"]) == ["in", "hop"]
  end

  test "into_named/3 names the node, so one module backs two" do
    assert Enum.map(class(Twice)["tasklets"], & &1["name"]) == ["first", "second"]
    assert {:ok, _derived} = Declaration.check(Twice)
  end

  test "the defaults the Elixir surface implies are declared outright" do
    decl = class(Defaults)
    assert decl["run"] == %{"steps" => %{"each" => "per_input"}}
    assert %{"sdk_lang" => 3, "change_format_id" => 7} = decl

    [keyset, compensating] = decl["tasklets"]

    assert %{"cell_mode" => "static", "cell_keys" => ~w(a b c d), "cells_for_overridden" => true} =
             keyset

    assert %{"change_is_nochange" => true, "compensate_overridden" => false, "clock" => "wall"} =
             keyset

    assert %{
             "compensate_overridden" => true,
             "clock" => "event_time",
             "change_is_nochange" => false,
             "schema_version" => 3,
             "min_readable_schema_version" => 2
           } = compensating
  end

  test "what the declaration cannot carry is refused by the surface, every issue at once" do
    assert {:error, %Error{code: "topology.keyed_on_entry", details: details}} =
             Declaration.build(SurfaceRefused)

    assert ["topology.keyed_on_entry", "topology.cells_for_missing"] =
             Enum.map(details["issues"], & &1["code"])
  end

  test "the transport is uds or quic, and the native one is refused by name" do
    assert %Flezha.SDK{transport: :uds} = Flezha.SDK.create_runner("ns")
    assert %Flezha.SDK{transport: :quic} = Flezha.SDK.create_runner("ns", transport: :quic)

    assert %Flezha.SDK{namespace_secret: "s3cret"} =
             Flezha.SDK.create_runner("ns", namespace_secret: "s3cret")

    assert {:error, %Error{code: "runtime.invalid_transport", message: message}} =
             Flezha.SDK.create_runner("ns", transport: :native)

    assert message =~ "native" and message =~ "uds or quic"
  end

  test "the runner credential is left out of what a handle and a crash report print" do
    handle = Flezha.SDK.create_runner("ns", namespace_secret: "s3cret")
    refute inspect(handle) =~ "s3cret"

    status = %{state: %{opts: [namespace: "ns", namespace_secret: "s3cret"], core: nil}}
    refute inspect(Flezha.SDK.Runner.format_status(status)) =~ "s3cret"
  end
end
