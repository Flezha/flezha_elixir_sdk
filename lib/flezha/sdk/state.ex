defmodule Flezha.SDK.State do
  @moduledoc """
  The `state` block and `defchange`: §3's change derivation, in Elixir.

  Declared state is mutated only through declared changes, because the change
  log IS how a cell is rebuilt. A `state` block declares the fields and their
  defaults, and `defchange` writes each change once:

      use Flezha.SDK.Tasklet, name: "position"

      state do
        field :qty, :integer, default: 0
        field :fill_ids, :list, default: []
      end

      defchange record_fill(state, qty, id) do
        %{state | qty: state.qty + qty, fill_ids: [id | state.fill_ids]}
      end

  The block generates `init_state/1` (the map of defaults, overridable) and
  `state_from_json/1` (each field read by its string key, falling back to its
  default). It generates no change.

  `defchange name(state, args…) do body end` is the one way to change state,
  in a module with or without a block: it is written once and generated into
  both halves — the `apply_change(state, {:name, args…})` clause and the
  recorder `name(state, ctx, args…)`, which journals the change through
  `Flezha.SDK.Ctx.apply/3` and answers the new state — so a mutation spanning
  several fields is one journal record. A module with no `defchange` exports
  no `apply_change/2` and is changeless.

  A module that `use`s `Flezha.SDK.Tasklet` or a relay macro and writes
  `apply_change/2` by hand fails to compile, directing the author to
  `defchange`; a hand-written `apply_change/2` belongs to the reference form,
  `@behaviour Flezha.SDK.Tasklet`. A block beside a hand-written
  `state_from_json/1` fails to compile too: the block owns it.

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
  Declare the fields a cell's state carries. See the module doc.
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
            "what a fresh cell holds and what a JSON object falls back to"
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
    # The recorder takes plain arguments of its own, so an argument the fold
    # ignores is written `_note` as in any function, with no warning either side.
    [state | values] = Macro.generate_arguments(length(args), __MODULE__)
    ctx = Macro.var(:ctx, __MODULE__)

    quote do
      @flezha_changes unquote(name)

      @doc false
      def __flezha_apply_change__(unquote(state_arg), {unquote(name), unquote_splicing(rest)}),
        do: unquote(body)

      def unquote(name)(unquote(state), unquote(ctx), unquote_splicing(values)) do
        Flezha.SDK.Ctx.apply(
          unquote(ctx),
          unquote(state),
          {unquote(name), unquote_splicing(values)}
        )
      end
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    fields = Module.get_attribute(env.module, :flezha_state_fields, []) |> Enum.reverse()
    changes = Module.get_attribute(env.module, :flezha_changes, [])
    block? = Module.get_attribute(env.module, :flezha_state_block, false)
    # A short-form relay module carries the block for the tasklet the
    # relay generates, so there the callbacks implement no behaviour.
    impl = Flezha.SDK.Tasklet in Module.get_attribute(env.module, :behaviour, [])

    refuse_hand_written(env.module, block?)

    quote do
      unquote(if block?, do: derived(fields, impl))
      unquote(if changes != [], do: fold(impl))
    end
  end

  # `defchange` is the one way to write a change here, and the block owns
  # `state_from_json/1`. The short form's generated tasklet delegates to the
  # `apply_change/2` its author's `defchange` wrote, so it is not refused.
  defp refuse_hand_written(module, block?) do
    if Module.defines?(module, {:apply_change, 2}) and
         not Module.get_attribute(module, :flezha_delegates_changes, false) do
      raise CompileError,
        description:
          "#{inspect(module)} defines `apply_change/2`; in the `use` form each change is " <>
            "written once with `defchange`, which generates it. A hand-written " <>
            "`apply_change/2` is the reference form: `@behaviour Flezha.SDK.Tasklet` with " <>
            "every callback by hand"
    end

    if block? and Module.defines?(module, {:state_from_json, 1}) do
      raise CompileError,
        description:
          "#{inspect(module)} defines `state_from_json/1` beside a `state` block, which " <>
            "generates it; write the fields or the callback, not both"
    end
  end

  # What the block generates: the two callbacks the state shape decides.
  defp derived(fields, impl) do
    defaults = {:%{}, [], Enum.map(fields, fn {name, _type, default} -> {name, default} end)}

    from_json =
      {:%{}, [],
       Enum.map(fields, fn {name, _type, default} ->
         {name, quote(do: Map.get(json, unquote(Atom.to_string(name)), unquote(default)))}
       end)}

    quote do
      unquote(init_state(impl))

      @impl unquote(impl)
      def state_from_json(json) when is_map(json), do: unquote(from_json)

      unquote(defaults_doc(defaults))
    end
  end

  # The one `apply_change/2` the behaviour asks for, over every clause
  # `defchange` generated.
  defp fold(impl) do
    quote do
      @impl unquote(impl)
      def apply_change(state, change), do: __flezha_apply_change__(state, change)
    end
  end

  # `init_state/1` is overridable, which is Elixir's spelling of Rust's
  # `init = f`: an author who needs the cell key writes their own.
  defp init_state(impl) do
    quote do
      @impl unquote(impl)
      def init_state(_cell_key), do: __flezha_state_defaults__()

      defoverridable init_state: 1
    end
  end

  defp defaults_doc(defaults) do
    quote do
      @doc false
      def __flezha_state_defaults__, do: unquote(defaults)
    end
  end
end
