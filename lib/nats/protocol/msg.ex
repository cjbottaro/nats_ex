defmodule Nats.Protocol.Msg do
  defstruct [:subject, :sid, :reply_to, :bytes, :headers, :payload]
  require Logger

  @type t :: %__MODULE__{
    bytes: integer,
    headers: [binary] | [],
    payload: binary | map | nil,
    reply_to: binary | nil,
    sid: binary,
    subject: binary
  }

  @doc false
  def recv(socket, data, with_headers \\ false) do
    {subject, sid, reply_to, header_size, payload_size} = if with_headers do
      case String.split(data, " ") do
        [subject, sid, reply_to, header_size, payload_size] ->
          {subject, sid, reply_to, header_size, payload_size}

        [subject, sid, header_size, payload_size] ->
          {subject, sid, nil, header_size, payload_size}
      end
    else
      case String.split(data, " ") do
        [subject, sid, reply_to, payload_size] -> {subject, sid, reply_to, "0", payload_size}
        [subject, sid, payload_size] -> {subject, sid, nil, "0", payload_size}
      end
    end

    header_size = String.to_integer(header_size)
    payload_size = String.to_integer(payload_size) - header_size

    :ok = :inet.setopts(socket, packet: :raw)

    headers = case header_size do
      0 -> []

      n ->
        {:ok, data} = :gen_tcp.recv(socket, n)
        Logger.debug("<<- #{inspect data}")
        String.replace_trailing(data, "\r\n", "")
        |> String.split("\r\n")
    end

    payload = case payload_size do
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
      bytes: header_size + payload_size,
      headers: headers,
      payload: payload
    }
  end
end
