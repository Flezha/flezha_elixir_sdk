defmodule Flezha.SDK.CounterTable do
  @moduledoc """
  A process names its counter SLOTS and the numbering is done once.

      use Flezha.SDK.CounterTable,
        table: :counter,
        backend: :atomics,
        new_opts: [signed: false],
        slots: [
          [name: :dispatches, kind: :count],
          [name: :timers_armed, kind: :set]
        ]

  One `use` declares one table; a module owning two tables writes two.

  Each declared table `t` gains `t_rows/0`, `t_slots/0`, `t_size/0`,
  `t_index/1`, `t_new/0`, `t_bump/2`, `t_add/3`, `t_set/3`, `t_get/2` and
  `t_snapshot/2` — every one derived from the row list, so a slot's position
  exists in exactly one place and appending a row is the whole edit.

  A row is a keyword list carrying at least `name:` and `kind:`. The kind is
  what a reader needs to reduce the slot across the processes that hold it:

    * `:count` — a monotonic count. Summing two of them is meaningful.
    * `:set` — a gauge written whole. Summing two readings of one fact
      multiplies it, so the reader takes one.
    * `:instant` — a stamp. Summing two clocks means nothing, so the reader
      keeps the later one. A stamp's zero is ABSENT, not earliest: a holder
      that never wrote the slot still holds 0, and a monotonic reading is
      negative on the BEAM, so a plain maximum hands the answer to the holder
      with no reading at all. A reader that folds a stamp over holders where
      only some write it must skip the zeros.

  A row may carry any further fields its owner needs; they ride through
  `t_rows/0` untouched, which is what lets one table declare a slot's storage
  and the surface that reads it in the same row.

  `defaults:` carries the fields every row of a table shares — the scope of an
  operator table, the kind of a table that is all counts — so a row states only
  what is true of IT. A row that names a field wins over the default.

  `blocks:` reserves fixed-size regions after the counters for slots addressed
  by hash rather than by name — a start-time table, say. Each block's base is
  `t_block_base/1`, derived from the counters declared before it, so adding a
  counter moves the blocks rather than letting a new slot land inside one.

  Backends are `:counters` (the default) and `:atomics`, and `new_opts:` is
  passed to the backend's `new/2`. Both are lock-free and add no behaviour: a
  table here is instrumentation. A counter whose values are read as policy and
  persisted per mutation is a durable ledger and is not one of these tables.
  """

  @typedoc "A slot row: at least `name:` and `kind:`, plus whatever its owner declares."
  @type row :: keyword()
  @typedoc "How a slot is reduced across the processes that hold it."
  @type kind :: :count | :set | :instant

  @kinds [:count, :set, :instant]

  @doc """
  The reduction a kind implies, unless a row overrides it with `aggregate:`.

  A count sums. A gauge and a stamp are ONE fact read from many holders, so a
  reader takes the larger rather than adding readings of the same thing.

  This is the reduction across holders that all carry a reading. It is not the
  rule for folding a slot only some holders write: a stamp's 0 is absent, so a
  maximum against a holder that never wrote it answers 0. Skip the zeros there.
  """
  @spec default_aggregate(kind()) :: :sum | :max
  def default_aggregate(:count), do: :sum
  def default_aggregate(:set), do: :max
  def default_aggregate(:instant), do: :max

  @doc "The `aggregate:` a row declares, or the one its kind implies."
  @spec aggregate(row()) :: :sum | :max
  def aggregate(row) do
    Keyword.get_lazy(row, :aggregate, fn -> default_aggregate(Keyword.fetch!(row, :kind)) end)
  end

  @doc """
  The measurement key a metric row is emitted under.

  This decides the Prometheus TYPE — `:count` renders a counter, `:value` a
  gauge — so it is a deliberate choice a row states rather than one derived
  from the slot's kind. A row that says nothing takes its kind's natural key.
  """
  @spec measurement(row()) :: atom()
  def measurement(row) do
    Keyword.get_lazy(row, :measurement, fn ->
      case Keyword.fetch!(row, :kind) do
        :count -> :count
        _ -> :value
      end
    end)
  end

  # Runs while the using module compiles, so a malformed row fails the build of
  # the file that wrote it rather than the first bump at run time.
  @doc false
  @spec validate!(module(), atom(), [row()]) :: [row()]
  def validate!(module, table, rows) do
    Enum.each(rows, fn row ->
      unless Keyword.keyword?(row) and Keyword.has_key?(row, :name) do
        raise ArgumentError,
              "#{inspect(module)} table #{inspect(table)}: a slot row is a keyword list " <>
                "carrying at least `name:` and `kind:`, got #{inspect(row)}"
      end

      kind = Keyword.get(row, :kind)

      unless kind in @kinds do
        raise ArgumentError,
              "#{inspect(module)} slot #{inspect(Keyword.fetch!(row, :name))}: `kind:` is one " <>
                "of #{inspect(@kinds)}, got #{inspect(kind)} — the kind is what decides how " <>
                "the slot is reduced across the processes that hold it"
      end
    end)

    names = Enum.map(rows, &Keyword.fetch!(&1, :name))

    case names -- Enum.uniq(names) do
      [] ->
        rows

      dupes ->
        raise ArgumentError, "#{inspect(module)} #{inspect(table)} repeats #{inspect(dupes)}"
    end
  end

  # The rows are written INLINE in the `use`. A module attribute cannot carry
  # them: a macro is handed the attribute's AST, and its value is not yet
  # readable while `use` expands, so `@rows` would quietly build a table with no
  # slots at all. The macro republishes the rows as `@<table>_rows` instead, so
  # the module can still derive from them further down its own body.
  defp slot_rows(rows, _module, _table) when is_list(rows), do: rows

  defp slot_rows(other, module, table) do
    raise ArgumentError,
          "#{inspect(module)} table #{inspect(table)}: `slots:` takes the rows themselves, " <>
            "got #{Macro.to_string(other)}. A module attribute is not readable while `use` " <>
            "expands; write the rows here and read them back from @#{table}_rows."
  end

  defmacro __using__(opts) do
    table = Keyword.fetch!(opts, :table)
    caller = __CALLER__.module

    rows =
      opts
      |> Keyword.fetch!(:slots)
      |> slot_rows(caller, table)
      |> Enum.map(&Keyword.merge(Keyword.get(opts, :defaults, []), &1))
      |> then(&validate!(caller, table, &1))

    blocks = Keyword.get(opts, :blocks, [])
    backend = Keyword.get(opts, :backend, :counters)
    new_opts = Keyword.get(opts, :new_opts, [:atomics])

    names = Enum.map(rows, &Keyword.fetch!(&1, :name))
    counters = length(names)
    size = counters + Enum.sum(Keyword.values(blocks))
    indexed = Enum.with_index(names, 1)

    {bases, _} =
      Enum.map_reduce(blocks, counters, fn {name, len}, base -> {{name, base}, base + len} end)

    quote do
      # Republished so the module can derive from its own rows below the
      # `use` — the collapse a reader needs, a key list, a surface index.
      Module.put_attribute(__MODULE__, unquote(:"#{table}_rows"), unquote(Macro.escape(rows)))

      @doc false
      def unquote(:"#{table}_rows")(), do: unquote(Macro.escape(rows))

      @doc false
      def unquote(:"#{table}_slots")(), do: unquote(names)

      @doc false
      def unquote(:"#{table}_size")(), do: unquote(size)

      @doc false
      def unquote(:"#{table}_new")(),
        do: unquote(backend).new(unquote(size), unquote(new_opts))

      # One clause per slot: the cost of a hand-written number with none of
      # the drift, because the clauses ARE the row list.
      unquote(
        for {name, idx} <- indexed do
          quote do
            def unquote(:"#{table}_index")(unquote(name)), do: unquote(idx)
          end
        end
      )

      unquote(
        for {name, base} <- bases do
          quote do
            def unquote(:"#{table}_block_base")(unquote(name)), do: unquote(base)
          end
        end
      )

      # The cross-holder reduction, one clause per row, derived from the row's
      # `aggregate:` or from its kind. A reader that reduces two readings of
      # one slot calls this rather than choosing `+` or `max` for itself.
      unquote(
        for r <- rows do
          name = Keyword.fetch!(r, :name)

          body =
            case Flezha.SDK.CounterTable.aggregate(r) do
              :sum -> quote(do: a + b)
              :max -> quote(do: max(a, b))
            end

          quote do
            def unquote(:"#{table}_reduce")(unquote(name), a, b), do: unquote(body)
          end
        end
      )

      @doc false
      def unquote(:"#{table}_bump")(ref, slot),
        do: unquote(backend).add(ref, unquote(:"#{table}_index")(slot), 1)

      @doc false
      def unquote(:"#{table}_add")(ref, slot, delta),
        do: unquote(backend).add(ref, unquote(:"#{table}_index")(slot), delta)

      @doc false
      def unquote(:"#{table}_sub")(ref, slot, delta),
        do: unquote(backend).sub(ref, unquote(:"#{table}_index")(slot), delta)

      @doc false
      def unquote(:"#{table}_set")(ref, slot, value),
        do: unquote(backend).put(ref, unquote(:"#{table}_index")(slot), value)

      @doc false
      def unquote(:"#{table}_get")(ref, slot),
        do: unquote(backend).get(ref, unquote(:"#{table}_index")(slot))

      # Read every slot into `base`, keyed by the layout's own atoms.
      @doc false
      def unquote(:"#{table}_snapshot")(ref, base \\ %{}) do
        Enum.reduce(unquote(indexed), base, fn {slot, idx}, acc ->
          Map.put(acc, slot, unquote(backend).get(ref, idx))
        end)
      end
    end
  end
end
