defmodule Flezha.SDK.Error do
  @moduledoc """
  The one error this SDK answers anything Flezha refuses with: a stable `code`
  spelled `family.snake`, a `class`, a plain `message`, the `details` the
  message names, and a `hint` naming the next command when there is one.

  A refusal the controller sent carries its own rendering, read by
  `from_wire/1`. A refusal this SDK makes itself, a declaration or a runtime
  misuse, is built by `new/2` from the catalogue of the contract set
  (`flz_transport/errors.json`), the one the controller and the Rust core render
  from, so a code reads the same in every language.

  This module is also the Elixir renderer the controller's `Flezha.Error`
  shares: `render/2`, `object/1` and `render_template/2` take any map carrying
  the five fields.
  """

  alias Flezha.SDK.Contract

  @catalogue_path Contract.path!("flz_transport/errors.json")
  @external_resource @catalogue_path
  @catalogue @catalogue_path |> File.read!() |> Jason.decode!()

  @uncatalogued "internal.uncatalogued"

  defstruct [:code, :class, :message, details: %{}, hint: nil]

  @type class ::
          :internal | :invalid | :unreachable | :denied | :not_found | :conflict | :unavailable

  @type t :: %__MODULE__{
          code: String.t(),
          class: class(),
          message: String.t(),
          details: %{optional(String.t()) => term()},
          hint: String.t() | nil
        }

  @classes Map.new(@catalogue["classes"], fn {class, exit} -> {String.to_atom(class), exit} end)

  @doc "The catalogue as it was compiled in."
  @spec catalogue() :: map()
  def catalogue(), do: @catalogue

  @doc "The exit code a class answers."
  @spec exit_code(%{class: class()}) :: pos_integer()
  def exit_code(%{class: class}), do: Map.fetch!(@classes, class)

  @doc """
  The error `code` names, its templates filled from `details`. A code the
  catalogue does not know is `internal.uncatalogued`, naming it.
  """
  @spec new(String.t(), map() | keyword()) :: t()
  def new(code, details \\ %{}) do
    details = plain_details(Map.new(details))

    case Map.fetch(@catalogue["codes"], code) do
      {:ok, entry} ->
        %__MODULE__{
          code: code,
          class: class(entry["class"]),
          message: render_template(entry["message"], details),
          details: details,
          hint: entry["hint"] && render_template(entry["hint"], details)
        }

      :error ->
        new(@uncatalogued, %{"reason" => code})
    end
  end

  @doc """
  The error a JSON error document carries (`{"error": {...}}`), as the core and
  the controller render it; `nil` when `document` is not one.
  """
  @spec from_document(String.t()) :: t() | nil
  def from_document(document) do
    case Jason.decode(document) do
      {:ok, %{"error" => %{"code" => code} = error}} when is_binary(code) and code != "" ->
        %__MODULE__{
          code: code,
          class: class(error["class"]),
          message: error["message"] || "",
          details: error["details"] || %{},
          hint: error["hint"]
        }

      _ ->
        nil
    end
  end

  @doc """
  The error a controller reply carried as a wire `Error` message; `nil` for a
  reply that carries none.
  """
  @spec from_wire(map() | nil) :: t() | nil
  def from_wire(nil), do: nil
  def from_wire(%{code: ""}), do: nil

  def from_wire(%{code: code} = wire) do
    %__MODULE__{
      code: code,
      class: class(wire.class),
      message: wire.message,
      details: decoded_details(wire.details_json),
      hint: if(wire.hint == "", do: nil, else: wire.hint)
    }
  end

  @doc "`:text` is `error: <message> (<code>)` and a `hint:` line; `:json` the one-line document."
  @spec render(map(), :text | :json | String.t()) :: String.t()
  def render(error, format) when format in [:json, "json"], do: "{\"error\":#{object(error)}}"
  def render(%{hint: nil} = error, _text), do: "error: #{error.message} (#{error.code})"
  def render(error, _text), do: "error: #{error.message} (#{error.code})\nhint: #{error.hint}"

  @doc "The `{code, class, message, details, hint}` object, as JSON with a fixed key order."
  @spec object(map()) :: String.t()
  def object(error) do
    "{\"code\":#{Jason.encode!(error.code)},\"class\":#{Jason.encode!(Atom.to_string(error.class))}," <>
      "\"message\":#{Jason.encode!(error.message)},\"details\":#{sorted_json(error.details)}," <>
      "\"hint\":#{Jason.encode!(error.hint)}}"
  end

  @doc "`details` as the compact, key-sorted JSON object the wire carries."
  @spec details_json(map()) :: String.t()
  def details_json(error), do: sorted_json(error.details)

  @doc """
  Details a caller can read without knowing Elixir: string keys, and values
  that are text, numbers, booleans, lists or string-keyed objects. Any other
  term is its inspect.
  """
  @spec plain_details(map()) :: map()
  def plain_details(details),
    do: Map.new(details, fn {key, value} -> {to_string(key), plain(value)} end)

  defp plain(value)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: value

  defp plain(value) when is_atom(value), do: Atom.to_string(value)
  defp plain(value) when is_list(value), do: Enum.map(value, &plain/1)
  defp plain(%{__struct__: _} = struct) when is_map_key(struct, :code), do: error_details(struct)
  defp plain(%{} = map) when not is_struct(map), do: plain_details(map)
  defp plain(value), do: inspect(value)

  # An error nested in another's details, such as one issue of several, is its
  # own object.
  defp error_details(error),
    do: %{"code" => error.code, "message" => error.message, "details" => error.details}

  @doc """
  `template` with every `{key}` replaced by `details[key]` as text: a string as
  written, a list of strings joined by `, `, anything else as compact JSON. A
  key the details do not carry is left as written.
  """
  @spec render_template(String.t(), map()) :: String.t()
  def render_template(template, details) do
    Regex.replace(~r/\{([a-z0-9_]+)\}/, template, fn whole, key ->
      case Map.fetch(details, key) do
        {:ok, value} -> as_text(value)
        :error -> whole
      end
    end)
  end

  defp as_text(value) when is_binary(value), do: value

  defp as_text(value) when is_list(value) do
    if Enum.all?(value, &is_binary/1), do: Enum.join(value, ", "), else: sorted_json(value)
  end

  defp as_text(value), do: sorted_json(value)

  # Compact JSON with every object's keys in byte order, so this renderer and
  # the Rust one agree whatever order a map was built in.
  defp sorted_json(%{} = map) do
    body =
      map
      |> Enum.map(fn {key, value} -> {to_string(key), value} end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map_join(",", fn {key, value} -> Jason.encode!(key) <> ":" <> sorted_json(value) end)

    "{" <> body <> "}"
  end

  defp sorted_json(list) when is_list(list),
    do: "[" <> Enum.map_join(list, ",", &sorted_json/1) <> "]"

  defp sorted_json(value), do: Jason.encode!(value)

  defp decoded_details(json) do
    case Jason.decode(json) do
      {:ok, %{} = details} -> details
      _not_an_object -> %{}
    end
  end

  # A class word read off the wire or a document maps onto the seven known
  # atoms and never mints one.
  defp class(word), do: Enum.find(Map.keys(@classes), :internal, &(Atom.to_string(&1) == word))
end

defimpl Jason.Encoder, for: Flezha.SDK.Error do
  def encode(error, _opts), do: Flezha.SDK.Error.object(error)
end
