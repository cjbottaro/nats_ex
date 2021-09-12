defmodule Nats.Protocol.Connect do
  @moduledoc false
  defstruct [:payload]

  def new(payload \\ []) do
    %__MODULE__{payload: Map.new(payload)}
  end

  def to_iodata(connect) when is_struct(connect, __MODULE__) do
    ["CONNECT", " ", Jason.encode!(connect.payload), "\r\n"]
  end

end
