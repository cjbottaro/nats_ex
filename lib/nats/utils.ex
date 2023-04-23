defmodule Nats.Utils do
  @moduledoc false

  def default_opts(opts, defaults) do
    {opts, other} = Keyword.split(opts, Keyword.keys(defaults))
    opts = Keyword.merge(defaults, opts)
    {opts, other}
  end

  def chomp(s) do
    String.replace_suffix(s, "\r\n", "")
  end

  def new_uid(bytes \\ 4) do
    :crypto.strong_rand_bytes(bytes)
    |> Base.encode16(case: :lower)
  end

  def to_nanosecond(value, unit \\ :millisecond) do
    case unit do
      :second      -> value * 1_000_000_000
      :millisecond -> value * 1_000_000
      :microsecond -> value * 1_000

      :minute -> to_nanosecond(value*60, :second)
      :hour -> to_nanosecond(value*60, :minute)
    end
  end

  def format_usec(usec) do
    cond do
      usec < 1_000 ->
        "#{usec}μs"

      true ->
        ms = usec / 1_000 |> round()
        "#{ms}ms"
    end
  end

  @spec format_duration(integer, atom) :: binary

  def format_duration(quantity, unit) do
    usec =
      case unit do
        :microsecond -> quantity
        :millisecond -> quantity * 1000
        :second -> quantity * 1000 * 1000
      end

    cond do
      usec < 1_000 ->
        "#{usec}μs"

      usec < 1_000_000 ->
        ms = (usec / 1_000) |> round()
        "#{ms}ms"

      usec < 60_000_000 ->
        s = div(usec, 1_000_000)

        case div(usec - s * 1_000_000, 1_000) do
          0 -> "#{s}s"
          ms -> "#{s}s#{ms}ms"
        end

      true ->
        s = (usec / 1_000_000) |> round()
        m = div(s, 60)

        case rem(s, 60) do
          0 -> "#{m}m"
          s -> "#{m}m#{s}s"
        end
    end
  end

end
