defmodule Flezha.SDK.Control do
  @moduledoc """
  Administering the namespace this runner's connection proved, from ordinary
  application code. Sending to a relay is not here: it is
  `Flezha.SDK.send/5`.

  No tasklet has to be running and the application holds no credential of its
  own: the controller judges every call as the identity the connection already
  established, so nothing an unattended process holds expires. A user
  grants a namespace's applications the authority once and revokes it the same
  way, and a revocation takes effect on the next call:

      flz role bind runner@team-orders app team-orders

  Every function here is one line over `call/3`, because the verb travels as
  data on the wire, through `flz_core`'s one administration entry. Adding a verb
  costs a function and nothing else.
  """

  alias Flezha.SDK.{Error, Native}

  @typedoc "A runner handle, or the server behind one."
  @type runner :: Flezha.SDK.t() | pid() | atom()

  @doc """
  Start a relay of `factory_id`, on the specification `flz relay start`
  takes: `id:`, `init_values:` and `opts:`, each optional. Answers the
  id it runs under, the generated one when no `id:` was given, and whether this
  call started it or found a live instance already holding the id.

  Without `id:` every call starts a new relay, so a start retried after a
  lost answer starts a second one, and a second stream runs until it is stopped.
  Pass `id:` when the start may be retried.
  """
  @spec start_relay(runner(), String.t(), keyword()) ::
          {:ok, %{id: String.t(), outcome: :started | :already_running}} | {:error, term()}
  def start_relay(runner, factory_id, opts \\ []) do
    fields = given(%{"factory_id" => factory_id}, opts, [:id, :init_values, :opts])

    with {:ok, answer} <- call(runner, "Relay.start", [fields]), do: started(answer)
  end

  @doc "Stop a relay, or the whole partition group it names."
  @spec stop_relay(runner(), String.t()) :: {:ok, term()} | {:error, term()}
  def stop_relay(runner, relay_id),
    do: call(runner, "Relay.stop", [relay_id])

  @doc """
  Enable the plugin `name` in this namespace, of the provider `provider` (for
  example `"relay_websocket"`). Options: `opts:`, the provider's options;
  `single: true` to run one copy in the whole cluster; and
  `relay_id:` with `hooks:` to bind that relay in the same call.
  """
  @spec enable_plugin(runner(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def enable_plugin(runner, name, provider, opts \\ []) do
    fields = %{"provider" => provider, "opts" => Keyword.get(opts, :opts, %{})}

    call(runner, "PluginManager.enable", [
      name,
      given(fields, opts, [:single, :relay_id, :hooks])
    ])
  end

  @doc """
  Bind the relay `relay_id` to the enabled plugin `name`, with the
  optional `hooks:` and `channel:`.
  """
  @spec bind_plugin(runner(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def bind_plugin(runner, name, relay_id, opts \\ []) do
    fields = given(%{"relay_id" => relay_id}, opts, [:hooks, :channel])
    call(runner, "PluginManager.bind", [name, fields])
  end

  @doc "Unbind the relay `relay_id` from the plugin `name`, which stays enabled."
  @spec unbind_plugin(runner(), String.t(), String.t()) :: {:ok, term()} | {:error, term()}
  def unbind_plugin(runner, name, relay_id),
    do: call(runner, "PluginManager.unbind", [name, %{"relay_id" => relay_id}])

  @doc "Disable the plugin `name`, stopping it wherever it runs."
  @spec disable_plugin(runner(), String.t()) :: {:ok, term()} | {:error, term()}
  def disable_plugin(runner, name), do: call(runner, "PluginManager.disable", [name, %{}])

  @doc "The relay ids running in this namespace."
  @spec list_relays(runner()) :: {:ok, term()} | {:error, term()}
  def list_relays(runner), do: call(runner, "Relay.list_relays", [])

  @doc "The plugins enabled in this namespace: name, provider, single, bound relays."
  @spec list_plugins(runner()) :: {:ok, term()} | {:error, term()}
  def list_plugins(runner), do: call(runner, "PluginManager.list", [])

  @doc """
  A relay's live state, and the change sequence it was read at. `cell` is
  `<tasklet>[/<cell_key>]`: a tasklet's Cells, or one of them; `nil` reads
  every Cell of the relay.
  """
  @spec cell_state(runner(), String.t(), String.t() | nil) :: {:ok, term()} | {:error, term()}
  def cell_state(runner, relay_id, cell \\ nil),
    do: call(runner, "Relay.cell_state", [relay_id, cell])

  @doc """
  Any published function, by name.

  What succeeds follows from the role the namespace is bound to, never from a
  list kept here. `args` are the call's arguments other than the namespace,
  which the controller fills from the connection.
  """
  @spec call(runner(), String.t(), [term()]) :: {:ok, term()} | {:error, Error.t()}
  def call(runner, verb, args) when is_binary(verb) and is_list(args) do
    with {:ok, core} <- Flezha.SDK.core(runner),
         json when is_binary(json) <-
           Native.answered(Native.admin_call(core, verb, Jason.encode!(args))),
         do: decoded(json, verb)
  end

  defp decoded(json, verb) do
    case Jason.decode(json) do
      {:ok, answer} -> {:ok, answer}
      {:error, _not_json} -> {:error, Error.new("control.unreadable_answer", verb: verb)}
    end
  end

  defp started(%{"id" => id, "outcome" => "started"}), do: {:ok, %{id: id, outcome: :started}}

  defp started(%{"id" => id, "outcome" => "already_running"}),
    do: {:ok, %{id: id, outcome: :already_running}}

  defp started(_other),
    do: {:error, Error.new("control.unreadable_answer", verb: "Relay.start")}

  # The `keys` set in `opts`, added to `fields` under their names.
  defp given(fields, opts, keys) do
    for key <- keys,
        (value = Keyword.get(opts, key)) != nil,
        into: fields,
        do: {Atom.to_string(key), value}
  end
end
