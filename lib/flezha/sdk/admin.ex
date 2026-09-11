defmodule Flezha.SDK.Admin do
  @moduledoc """
  Administering the namespace this runner's connection proved, from ordinary
  application code.

  No procedure has to be running and the application holds no credential of its
  own: the controller judges every call as the identity the connection already
  established, so nothing an unattended process holds expires. An operator
  grants a namespace's applications the authority once and revokes it the same
  way, and a revocation takes effect on the next call:

      flz role bind runner@team-orders app team-orders

  Every function here is one line over `call/3`, because the verb travels as
  data on the wire. Adding a verb costs a function and nothing else. This host
  is its own core and shares no bindings with `flz_core`, so the shape is
  re-expressed here rather than inherited; the frame and the verb names are the
  same ones every other SDK sends.
  """

  alias Flezha.SDK.RunnerServer

  @typedoc "A runner handle, or the server behind one."
  @type runner :: Flezha.SDK.t() | pid() | atom()

  @doc "Start an operative, on the specification `flz operative start` takes."
  @spec start_operative(runner(), String.t(), String.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def start_operative(runner, operative_id, factory_id, opts \\ []) do
    fields =
      %{"id" => operative_id, "factory_id" => factory_id}
      |> put_given("kind", Keyword.get(opts, :kind))
      |> put_given("init_values", Keyword.get(opts, :init_values))
      |> put_given("opts", Keyword.get(opts, :opts))

    call(runner, "Operative.start", [fields])
  end

  @doc "Stop an operative, or the whole partition group it names."
  @spec stop_operative(runner(), String.t()) :: {:ok, term()} | {:error, term()}
  def stop_operative(runner, operative_id),
    do: call(runner, "Operative.stop", [operative_id])

  @doc """
  Inject a message into an operative's input stream, blocking for the delivery
  verdict. `channel` may be empty for none.
  """
  @spec send_to(runner(), String.t(), String.t(), String.t()) :: {:ok, term()} | {:error, term()}
  def send_to(runner, operative_id, message, channel \\ ""),
    do: call(runner, "Operative.send", [operative_id, message, channel])

  @doc "Enable a plugin, built from the fields `flz plugin enable` takes."
  @spec enable_plugin(runner(), String.t(), map()) :: {:ok, term()} | {:error, term()}
  def enable_plugin(runner, plugin_id, fields),
    do: call(runner, "PluginManager.enable", [plugin_id, fields])

  @doc "Disable a plugin, or every partitioned instance of it."
  @spec disable_plugin(runner(), String.t()) :: {:ok, term()} | {:error, term()}
  def disable_plugin(runner, plugin_id),
    do: call(runner, "PluginManager.disable", [plugin_id])

  @doc "The operative ids running in this namespace."
  @spec list_operatives(runner()) :: {:ok, term()} | {:error, term()}
  def list_operatives(runner), do: call(runner, "Operative.list_operatives", [])

  @doc "The plugins enabled on the controller node answering this call."
  @spec list_plugins(runner()) :: {:ok, term()} | {:error, term()}
  def list_plugins(runner), do: call(runner, "PluginManager.list", [])

  @doc "One entity's live state, and the change sequence it was read at."
  @spec entity_state(runner(), String.t(), String.t()) :: {:ok, term()} | {:error, term()}
  def entity_state(runner, operative_id, entity_key),
    do: call(runner, "Operative.entity_state", [operative_id, entity_key])

  @doc """
  Any published function, by name.

  What succeeds follows from the role the namespace is bound to, never from a
  list kept here. `args` are the call's arguments other than the namespace,
  which the controller fills from the connection.
  """
  @spec call(runner(), String.t(), [term()]) :: {:ok, term()} | {:error, term()}
  def call(runner, verb, args) when is_binary(verb) and is_list(args) do
    with {:ok, server} <- server(runner),
         {:ok, json} <- RunnerServer.admin_call(server, verb, Jason.encode!(args)) do
      Jason.decode(json)
    end
  end

  defp server(server) when is_pid(server) or is_atom(server), do: {:ok, server}
  defp server(runner), do: Flezha.SDK.server(runner)

  defp put_given(fields, _key, nil), do: fields
  defp put_given(fields, key, value), do: Map.put(fields, key, value)
end
