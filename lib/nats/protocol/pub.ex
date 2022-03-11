defmodule Nats.Protocol.Pub do
  @moduledoc false
  defstruct [:subject, :reply_to, :headers, :payload, :bytes]

  def new(subject, opts \\ []) do
    {bytes, payload} = process_payload(opts[:payload])

    %__MODULE__{
      subject: subject,
      reply_to: opts[:reply_to],
      headers: opts[:headers],
      payload: payload,
      bytes: bytes
    }
  end

  def to_iodata(%__MODULE__{headers: nil} = pub) do
    %{subject: subject, reply_to: reply_to, payload: payload, bytes: bytes} = pub

    msg = ["PUB", " ", subject]

    msg = if reply_to do
      [msg, " ", reply_to]
    else
      msg
    end

    if payload do
      [msg, " ", to_string(bytes), "\r\n", payload, "\r\n"]
    else
      [msg, " ", "0", "\r\n", "\r\n"]
    end
  end

  def to_iodata(%__MODULE__{} = pub) do
    %{
      subject: subject,
      reply_to: reply_to,
      headers: headers,
      payload: payload,
      bytes: bytes
    } = pub

    reply_to = if reply_to do
      [" ", reply_to]
    else
      ""
    end

    headers = Enum.map(headers, fn {k, v} ->
      [k, ": ", v, "\r\n"]
    end)

    header_size = IO.iodata_length(headers) + 12
    bytes = header_size + bytes

    ["HPUB ", subject, reply_to, " ", to_string(header_size), " ", to_string(bytes), "\r\n", "NATS/1.0\r\n", headers, "\r\n", payload, "\r\n"]
  end

  defp process_payload(nil), do: {0, ""}
  defp process_payload(s) when is_binary(s), do: {byte_size(s), s}
  defp process_payload(m) when is_map(m), do: process_payload(Jason.encode!(m))
  defp process_payload(l) when is_list(l), do: process_payload(Map.new(l))

end
