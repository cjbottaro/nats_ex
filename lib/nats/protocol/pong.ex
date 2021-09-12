defmodule Nats.Protocol.Pong do
  @moduledoc false
  defstruct []

  def new do
    %__MODULE__{}
  end

  def to_iodata(pong) when is_struct(pong, __MODULE__) do
    ["PONG", "\r\n"]
  end

end
