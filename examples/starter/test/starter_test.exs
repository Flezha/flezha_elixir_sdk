defmodule Starter.RelayTest do
  @moduledoc """
  The relay run in this process with no controller (`Flezha.SDK.Local`): each
  hit is routed to the cell of its page, as a runner routes it.
  """
  use ExUnit.Case, async: false

  # A local run logs the core's banner: nothing it holds is durable.
  @moduletag :capture_log

  alias Flezha.SDK.Local

  test "counts each page on its own" do
    {:ok, run} = Local.start(Starter.Relay)
    on_exit(fn -> Local.close(run) end)

    test = self()
    :ok = Local.on_output(run, "counts", &send(test, {:count, Jason.decode!(&1.payload)}))
    for page <- ~w(home home about home), do: :ok = Local.feed(run, "hit", ~s({"page":"#{page}"}))
    :ok = Local.run_until_idle(run, 5_000)

    counts =
      for _ <- 1..4 do
        assert_receive {:count, count}
        count
      end

    # Two cells run side by side, so only each page's own answers are in order.
    assert Enum.group_by(counts, & &1["page"], & &1["total"]) ==
             %{"home" => [1, 2, 3], "about" => [1]}
  end
end
