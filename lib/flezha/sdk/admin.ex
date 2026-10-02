defmodule Flezha.SDK.Admin do
  @moduledoc """
  Administering the namespace this runner's connection proved, from ordinary
  application code.

  No tasklet has to be running and the application holds no credential of its
  own: the controller judges every call as the identity the connection already
  established, so nothing an unattended process holds expires. A user
  grants a namespace's applications the authority once and revokes it the same
  way, and a revocation takes effect on the next call:

      flz role bind runner@team-orders app team-orders

  Every function here is one line over `call/3`, because the verb travels as
  data on the wire. Adding a verb costs a function and nothing else. This host
  is its own core and shares no bindings with `flz_core`, so the shape is
  re-expressed here rather than inherited; the frame and the verb names are the
  same ones every other SDK sends.
  """

  alias Flezha.SDK.Error
  alias Flezha.SDK.RunnerServer

  @typedoc "A runner handle, or the server behind one."
  @type runner :: Flezha.SDK.t() | pid() | atom()

  @doc """
  Start an operator of `factory_id`, on the specification `flz operator start`
  takes: `id:`, `init_values:` and `opts:`, each optional. Answers the
  id it runs under, the generated one when no `id:` was given, and whether this
  call started it or found a live instance already holding the id.

  Without `id:` every call starts a new operator, so a start retried after a
  lost answer starts a second one, and a second stream runs until it is stopped.
  Pass `id:` when the start may be retried.
  """
  @spec start_operator(runner(), String.t(), keyword()) ::
          {:ok, %{id: String.t(), outcome: :started | :already_running}} | {:error, term()}
  def start_operator(runner, factory_id, opts \\ []) do
    fields = given(%{"factory_id" => factory_id}, opts, [:id, :init_values, :opts])

    with {:ok, answer} <- call(runner, "Operator.start", [fields]), do: started(answer)
  end

  @doc "Stop an operator, or the whole partition group it names."
  @spec stop_operator(runner(), String.t()) :: {:ok, term()} | {:error, term()}
  def stop_operator(runner, operator_id),
    do: call(runner, "Operator.stop", [operator_id])

  @doc """
  Inject a message into an operator's input stream, blocking for the delivery
  verdict. `channel` may be empty for none.
  """
  @spec send_to(runner(), String.t(), String.t(), String.t()) :: {:ok, term()} | {:error, term()}
  def send_to(runner, operator_id, message, channel \\ ""),
    do: call(runner, "Operator.send", [operator_id, message, channel])

  @doc """
  Enable the plugin `name` in this namespace, of the provider `provider` (for
  example `"operator_websocket"`). Options: `opts:`, the provider's options;
  `single: true` to run one copy in the whole cluster; and
  `operator_id:` with `hooks:` to bind that operator in the same call.
  """
  @spec enable_plugin(runner(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def enable_plugin(runner, name, provider, opts \\ []) do
    fields = %{"provider" => provider, "opts" => Keyword.get(opts, :opts, %{})}

    call(runner, "PluginManager.enable", [
      name,
      given(fields, opts, [:single, :operator_id, :hooks])
    ])
  end

  @doc """
  Bind the operator `operator_id` to the enabled plugin `name`, with the
  optional `hooks:` and `channel:`.
  """
  @spec bind_plugin(runner(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def bind_plugin(runner, name, operator_id, opts \\ []) do
    fields = given(%{"operator_id" => operator_id}, opts, [:hooks, :channel])
    call(runner, "PluginManager.bind", [name, fields])
  end

  @doc "Unbind the operator `operator_id` from the plugin `name`, which stays enabled."
  @spec unbind_plugin(runner(), String.t(), String.t()) :: {:ok, term()} | {:error, term()}
  def unbind_plugin(runner, name, operator_id),
    do: call(runner, "PluginManager.unbind", [name, %{"operator_id" => operator_id}])

  @doc "Disable the plugin `name`, stopping it wherever it runs."
  @spec disable_plugin(runner(), String.t()) :: {:ok, term()} | {:error, term()}
  def disable_plugin(runner, name), do: call(runner, "PluginManager.disable", [name, %{}])

  @doc "The operator ids running in this namespace."
  @spec list_operators(runner()) :: {:ok, term()} | {:error, term()}
  def list_operators(runner), do: call(runner, "Operator.list_operators", [])

  @doc "The plugins enabled in this namespace: name, provider, single, bound operators."
  @spec list_plugins(runner()) :: {:ok, term()} | {:error, term()}
  def list_plugins(runner), do: call(runner, "PluginManager.list", [])

  @doc "One cell's live state, and the change sequence it was read at."
  @spec cell_state(runner(), String.t(), String.t()) :: {:ok, term()} | {:error, term()}
  def cell_state(runner, operator_id, cell_key),
    do: call(runner, "Operator.cell_state", [operator_id, cell_key])

  @doc """
  Any published function, by name.

  What succeeds follows from the role the namespace is bound to, never from a
  list kept here. `args` are the call's arguments other than the namespace,
  which the controller fills from the connection.
  """
  @spec call(runner(), String.t(), [term()]) :: {:ok, term()} | {:error, Error.t()}
  def call(runner, verb, args) when is_binary(verb) and is_list(args) do
    with {:ok, server} <- server(runner),
         {:ok, json} <- RunnerServer.admin_call(server, verb, Jason.encode!(args)) do
      case Jason.decode(json) do
        {:ok, answer} -> {:ok, answer}
        {:error, _not_json} -> {:error, Error.new("admin.unreadable_answer", verb: verb)}
      end
    end
  end

  defp server(server) when is_pid(server) or is_atom(server), do: {:ok, server}
  defp server(runner), do: Flezha.SDK.server(runner)

  defp started(%{"id" => id, "outcome" => "started"}), do: {:ok, %{id: id, outcome: :started}}

  defp started(%{"id" => id, "outcome" => "already_running"}),
    do: {:ok, %{id: id, outcome: :already_running}}

  defp started(_other), do: {:error, Error.new("admin.unreadable_answer", verb: "Operator.start")}

  # The `keys` set in `opts`, added to `fields` under their names.
  defp given(fields, opts, keys) do
    for key <- keys,
        (value = Keyword.get(opts, key)) != nil,
        into: fields,
        do: {Atom.to_string(key), value}
  end
end
