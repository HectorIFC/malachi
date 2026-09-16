# The decisions benchmark/single_node_scale.exs makes about its sweep, apart from the measuring: which
# ladder it runs, what a cell's efficiency is measured against, and at which N a batch size crossed the
# target rate. They live here because the script starts sweeping as soon as it is loaded, so nothing in
# it can be required by a test, and a runner cannot reach the target rate to exercise them for real.
#
#   Code.require_file("support/scale_sweep.exs", __DIR__)

defmodule Malachi.Bench.ScaleSweep do
  @doc """
  The ladder `name` asks for: `default` when `value` is nil, otherwise `value` parsed as distinct
  positive integers separated by whitespace, in the order given. `{:error, message}` for anything else,
  naming the variable, because a sweep over values nobody asked for is worse than none.
  """
  def ladder(_name, nil, default), do: {:ok, default}

  def ladder(name, value, _default) when is_binary(value) do
    case parse_ladder(value) do
      {:ok, ladder} -> {:ok, ladder}
      {:error, reason} -> {:error, "#{name}=#{inspect(value)} #{reason}; expected distinct positive integers"}
    end
  end

  defp parse_ladder(value) do
    with {:ok, items} <- non_empty(String.split(value)),
         {:ok, ints} <- positive_integers(items) do
      if Enum.uniq(ints) == ints, do: {:ok, ints}, else: {:error, "repeats a value"}
    end
  end

  defp non_empty([]), do: {:error, "is empty"}
  defp non_empty(items), do: {:ok, items}

  defp positive_integers(items) do
    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      case Integer.parse(item) do
        {int, ""} when int > 0 -> {:cont, {:ok, acc ++ [int]}}
        _ -> {:halt, {:error, "has #{inspect(item)}, not a positive integer"}}
      end
    end)
  end

  @doc """
  Each cell of one batch size with `:eff` set: its per-pipeline rate as a percentage of the rate at the
  smallest N swept, whatever order the cells ran in.
  """
  def with_efficiency([_ | _] = cells) do
    base = Enum.min_by(cells, & &1.n).per
    Enum.map(cells, fn cell -> %{cell | eff: Float.round(cell.per / base * 100, 0)} end)
  end

  @doc """
  The cell with the smallest N whose aggregate rate reached `target`, or nil. The smallest, not the
  first run: the question the sweep answers is how few pipelines it takes, and the ladder may be given
  in any order.
  """
  def crossing(cells, target) do
    cells
    |> Enum.filter(&(&1.agg >= target))
    |> Enum.min_by(& &1.n, fn -> nil end)
  end
end
