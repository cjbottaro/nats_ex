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
