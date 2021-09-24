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

end
