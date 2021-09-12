defmodule Nats.Protocol.Unsub do
  @moduledoc false
  defstruct [:sid, :count]

  @type t :: %__MODULE__{}

  def new(sid, count) do
    %__MODULE__{
      sid: sid,
      count: to_string(count)
    }
  end

  @spec to_iodata(unsub :: t()) :: list
  def to_iodata(unsub) when is_struct(unsub, __MODULE__) do
    %{
      sid: sid,
      count: count
    } = unsub

    msg = ["UNSUB", " ", sid]

    msg = if count do
      [msg, " ", count]
    else
      msg
    end

    [msg, "\r\n"]
  end

end
