defmodule Nats.Protocol.Msg do
  defstruct [:subject, :sid, :reply_to, :bytes, :payload]
  require Logger

  @type t :: %__MODULE__{
    bytes: integer,
    payload: binary,
    reply_to: binary | nil,
    sid: binary,
    subject: binary
  }

  @doc false
  def recv(socket, data) do
    {subject, sid, reply_to, bytes} = case String.split(data, " ") do
      [subject, sid, reply_to, bytes] -> {subject, sid, reply_to, bytes}
      [subject, sid, bytes] -> {subject, sid, nil, bytes}
    end

    bytes = String.to_integer(bytes)

    :ok = :inet.setopts(socket, packet: :raw)

    payload = case bytes do
      0 ->
        {:ok, "\r\n"} = :gen_tcp.recv(socket, 2)
        nil

      n ->
        {:ok, payload} = :gen_tcp.recv(socket, n)
        {:ok, "\r\n"} = :gen_tcp.recv(socket, 2)
        line = payload <> "\r\n"
        Logger.debug("<<- #{inspect line}")
        payload
    end

    :ok = :inet.setopts(socket, packet: :line)

    %__MODULE__{
      subject: subject,
      sid: sid,
      reply_to: reply_to,
      bytes: bytes,
      payload: payload
    }
  end
end
