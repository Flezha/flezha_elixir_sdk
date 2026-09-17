defmodule Flezha.SDK.State do
  @moduledoc """
  The `state` block and `defchange`: §3's change derivation, in Elixir.

  Declared state is mutated only through declared changes, because the change
  log IS how an entity is rebuilt. A `state` block writes that path for the
  field-granular half:

      use Flezha.SDK.Procedure, name: "position"

      state do
        field :qty, :integer, default: 0
        field :fill_ids, :list, default: []
      end

      defchange record_fill(state, qty, id) do
        %{state | qty: state.qty + qty, fill_ids: [id | state.fill_ids]}
      end

  The block generates `init_state/1` (the map of defaults, overridable),
  `state_from_json/1` (each field read by its string key, falling back to its
  default), `apply_change/2` clauses for `{:set, field, value}` on every field
  and `{:push, field, item}` on every `:list` field, and the recorders
  `set_<field>(state, ctx, value)` and `push_<field>(state, ctx, item)`, each
  journaling exactly one change through `Flezha.SDK.Ctx.apply/3`.

  `defchange name(state, args…) do body end` is the manual boundary: a mutation
  that is conditional, spans several fields, or is computed from the prior state
  is not one generated variant, so it is written once and generated into both
  halves — the `apply_change(state, {:name, args…})` clause and the recorder
  `name(state, ctx, args…)` — which is what makes it one journal record.

  A module with a `state` block that also writes `apply_change/2` or
  `state_from_json/1` by hand fails to compile, naming both: the block owns
  them.

  §3.1's keyed-map and deque modes are deferred for Elixir; this block carries
  the field and `:list` modes only. Renaming or retyping a field is a
  non-additive state edit and takes a schema-version bump, as §3.0 says for
  every SDK.
  """

  @field_types [:integer, :float, :string, :boolean, :list, :map, :term]

  @doc false
  defmacro __using__(_opts) do
    quote do
      import Flezha.SDK.State, only: [state: 1, defchange: 2]
      Module.register_attribute(__MODULE__, :flezha_state_fields, accumulate: true)
      Module.register_attribute(__MODULE__, :flezha_changes, accumulate: true)
      @before_compile Flezha.SDK.State
    end
  end

  @doc """
  Declare the fields an entity's state carries. See the module doc.
  """
  defmacro state(do: block) do
    quote do
      @flezha_state_block true
      import Flezha.SDK.State, only: [field: 3, state: 1, defchange: 2]
      unquote(block)
    end
  end

  @doc """
  One state field: a name, one of `:integer`, `:float`, `:string`, `:boolean`,
  `:list`, `:map` or `:term`, and a `default:`.
  """
  defmacro field(name, type, opts) do
    quote bind_quoted: [name: name, type: type, opts: opts] do
      Flezha.SDK.State.__field__(__MODULE__, name, type, opts)
    end
  end

  @doc false
  def __field__(module, name, type, opts) do
    unless is_atom(name) do
      raise CompileError,
        description: "a `field` name is an atom, read #{inspect(name)}"
    end

    unless type in @field_types do
      raise CompileError,
        description:
          "`field #{inspect(name)}` declares the type #{inspect(type)}; a field is one of " <>
            Enum.map_join(@field_types, ", ", &inspect/1)
    end

    unless Keyword.has_key?(opts, :default) do
      raise CompileError,
        description:
          "`field #{inspect(name)}` declares no `default:`; a state field's starting value is " <>
            "what a fresh entity holds and what a JSON object falls back to"
    end

    Module.put_attribute(
      module,
      :flezha_state_fields,
      {name, type, Keyword.fetch!(opts, :default)}
    )
  end

  @doc """
  A hand-written change: one body, generated into the fold and the recorder, so
  a mutation spanning several fields is one journal record.
  """
  defmacro defchange({name, _meta, args}, do: body) when is_atom(name) do
    [state_arg | rest] = args
    ctx = Macro.var(:__flezha_ctx__, __MODULE__)
    payload = quote(do: {unquote(name), unquote_splicing(rest)})

    quote do
      @flezha_changes unquote(name)

      @doc false
      def __flezha_apply_change__(unquote(state_arg), unquote(payload)), do: unquote(body)

      def unquote(name)(unquote(state_arg), unquote(ctx), unquote_splicing(rest)) do
        Flezha.SDK.Ctx.apply(unquote(ctx), unquote(state_arg), unquote(payload))
      end
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    fields = Module.get_attribute(env.module, :flezha_state_fields, []) |> Enum.reverse()
    changes = Module.get_attribute(env.module, :flezha_changes, [])
    block? = Module.get_attribute(env.module, :flezha_state_block, false)

    if block? or changes != [] do
      refuse_hand_written(env.module, block?)
    end

    cond do
      block? -> derived(fields)
      changes != [] -> fold()
      true -> nil
    end
  end

  # The block owns `apply_change/2` and `state_from_json/1`, so a module that
  # writes one by hand beside it has two answers and no way to say which wins.
  defp refuse_hand_written(module, block?) do
    for {name, arity} <- [apply_change: 2, state_from_json: 1],
        Module.defines?(module, {name, arity}) do
      raise CompileError,
        description:
          "#{inspect(module)} defines `#{name}/#{arity}` beside " <>
            if(block?, do: "a `state` block", else: "`defchange`") <>
            ", which generates it; write the fields and the named changes, or write the " <>
            "callbacks by hand, not both"
    end
  end

  # Everything the block generates: the two callbacks the state shape decides,
  # the fold over every derived and named change, and one recorder per mutation.
  defp derived(fields) do
    defaults = {:%{}, [], Enum.map(fields, fn {name, _type, default} -> {name, default} end)}

    from_json =
      {:%{}, [],
       Enum.map(fields, fn {name, _type, default} ->
         {name, quote(do: Map.get(json, unquote(Atom.to_string(name)), unquote(default)))}
       end)}

    quote do
      unquote(init_state())
      unquote(fold())

      @impl true
      def state_from_json(json) when is_map(json), do: unquote(from_json)

      @doc false
      def __flezha_apply_change__(state, {:set, field, value}) when is_map_key(state, field),
        do: Map.put(state, field, value)

      @doc false
      def __flezha_apply_change__(state, {:push, field, item}) when is_map_key(state, field),
        do: Map.put(state, field, Map.fetch!(state, field) ++ [item])

      unquote_splicing(recorders(fields))
      unquote(defaults_doc(defaults))
    end
  end

  # The one `apply_change/2` the behaviour asks for, over every clause the block
  # and `defchange` generated between them. A module with `defchange` and no
  # block gets this and nothing else: its state shape is its own `init_state/1`.
  defp fold do
    quote do
      @impl true
      def apply_change(state, change), do: __flezha_apply_change__(state, change)
    end
  end

  # `init_state/1` is overridable, which is Elixir's spelling of Rust's
  # `init = f`: an author who needs the entity key writes their own.
  defp init_state do
    quote do
      @impl true
      def init_state(_entity_key), do: __flezha_state_defaults__()

      defoverridable init_state: 1
    end
  end

  defp defaults_doc(defaults) do
    quote do
      @doc false
      def __flezha_state_defaults__, do: unquote(defaults)
    end
  end

  # One recorder per derivable mutation: `set_<field>` for every field, and
  # `push_<field>` for every `:list` field, appending as a Rust `Vec` push does.
  defp recorders(fields) do
    Enum.flat_map(fields, fn {name, type, _default} ->
      setter =
        quote do
          def unquote(:"set_#{name}")(state, ctx, value) do
            Flezha.SDK.Ctx.apply(ctx, state, {:set, unquote(name), value})
          end
        end

      if type == :list do
        [
          setter,
          quote do
            def unquote(:"push_#{name}")(state, ctx, item) do
              Flezha.SDK.Ctx.apply(ctx, state, {:push, unquote(name), item})
            end
          end
        ]
      else
        [setter]
      end
    end)
  end
end
