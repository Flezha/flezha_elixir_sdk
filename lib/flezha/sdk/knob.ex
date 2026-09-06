defmodule Flezha.SDK.Knob do
  @moduledoc """
  The Elixir SDK's reader of the one names table, and of the ladders beside it.

  A runner is its own OS process, so it reads the environment rather than the
  controller's application env — but it reads the same rows: the variable a name
  is set with, the type its text is parsed as, and the value it takes when
  nothing sets it are `Flezha.Config.Keys`' answers, not this module's. A row
  whose default differs by who reads it hands the SDK the runner's side.

  A value that does not parse — a pool size of `"0"`, a flag spelled `"maybe"` —
  reads as the default, which is what every other SDK's reader answers too.

  A wait before a retry is a declared row too, so the SDK's four ladders are
  armed and waited from here rather than from arithmetic of its own. Both halves
  live in one module because that is what the packaging boundary asks for: this
  is the single SDK file that names the controller's configuration, so the hex
  extraction swaps it and leaves the rest of the SDK alone
  (`scripts/check-sdk-boundary.sh`).
  """

  alias Flezha.Config
  alias Flezha.Config.Backoff
  alias Flezha.Config.Environment
  alias Flezha.Config.Keys

  @doc """
  The data heartbeat's key set, as the controller declares it.

  A runner must send exactly the keys the controller mirrors, and that list is a
  declared table like any other — every liveness row that names a `heartbeat:`.
  It is read here for the same reason the configuration rows are: this is the
  one SDK file that names the controller, so the hex extraction swaps it and the
  rest of the SDK asks this module
  (`scripts/check-sdk-boundary.sh`).
  """
  @spec heartbeat_keys() :: [String.t()]
  defdelegate heartbeat_keys(), to: Flezha.Transport.LivenessCounters

  @doc "What this runner reads for `name`: its variable, else the row's default."
  @spec get(Keys.name()) :: term()
  def get(name), do: get(name, Config.default(name, :runner))

  @doc """
  What this runner reads for `name`, with the caller's own fallback for a
  quantity the row cannot state — a sweep cadence derived from an entity's TTL.
  """
  @spec get(Keys.name(), term()) :: term()
  def get(name, fallback) do
    row = Keys.fetch!(name)

    with var when is_binary(var) <- row.env,
         raw when is_binary(raw) <- System.get_env(var),
         {:ok, value} <- Environment.parse(row, raw) do
      value
    else
      _ -> fallback
    end
  end

  @doc "The row `ladder` declares, for a site that takes one of its bounds as a default."
  @spec ladder(Backoff.ladder()) :: Backoff.row()
  defdelegate ladder(ladder), to: Backoff

  @doc "The wait before `attempt` against `target`, announced — for a site that arms a timer."
  @spec arm(Backoff.ladder(), pos_integer(), term(), keyword()) :: non_neg_integer()
  defdelegate arm(ladder, attempt, target, opts \\ []), to: Backoff

  @doc "`arm/4` and the sleep, for a site that retries in place."
  @spec wait(Backoff.ladder(), pos_integer(), term(), keyword()) :: :ok
  defdelegate wait(ladder, attempt, target, opts \\ []), to: Backoff

  @doc "Announce that `target` succeeded on `ladder`; answers the attempt count a reset leaves."
  @spec reset(Backoff.ladder(), term()) :: 0
  defdelegate reset(ladder, target), to: Backoff
end
