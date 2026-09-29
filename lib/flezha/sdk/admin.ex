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
    fields =
      %{"factory_id" => factory_id}
      |> put_given("id", Keyword.get(opts, :id))
      |> put_given("init_values", Keyword.get(opts, :init_values))
      |> put_given("opts", Keyword.get(opts, :opts))

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

  @doc "Enable a plugin, built from the fields `flz plugin enable` takes."
  @spec enable_plugin(runner(), String.t(), map()) :: {:ok, term()} | {:error, term()}
  def enable_plugin(runner, plugin_id, fields),
    do: call(runner, "PluginManager.enable", [plugin_id, fields])

  @doc "Disable a plugin, or every partitioned instance of it."
  @spec disable_plugin(runner(), String.t()) :: {:ok, term()} | {:error, term()}
  def disable_plugin(runner, plugin_id),
    do: call(runner, "PluginManager.disable", [plugin_id])

  @doc "The operator ids running in this namespace."
  @spec list_operators(runner()) :: {:ok, term()} | {:error, term()}
  def list_operators(runner), do: call(runner, "Operator.list_operators", [])

  @doc "The plugins enabled on the controller node answering this call."
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

  defp put_given(fields, _key, nil), do: fields
  defp put_given(fields, key, value), do: Map.put(fields, key, value)
end
