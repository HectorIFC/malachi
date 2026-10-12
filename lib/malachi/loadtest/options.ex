defmodule Malachi.Loadtest.Options do
  @moduledoc """
  The integer options of the load generator and its clients, checked where they are read: a zero or a
  negative count would fail late and confusingly (an ArithmeticError after the whole run, or an instantly
  empty measurement), so a bad one raises up front, naming the option.
  """

  @doc "The positive integer option `key` of `opts`, `default` without one; anything else raises."
  @spec positive!(keyword(), atom(), pos_integer()) :: pos_integer()
  def positive!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      value -> raise ArgumentError, "#{key} must be a positive integer, got: #{inspect(value)}"
    end
  end

  @doc "The non-negative integer option `key` of `opts`, `default` without one; anything else raises."
  @spec non_negative!(keyword(), atom(), non_neg_integer()) :: non_neg_integer()
  def non_negative!(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 -> value
      value -> raise ArgumentError, "#{key} must be a non-negative integer, got: #{inspect(value)}"
    end
  end
end
