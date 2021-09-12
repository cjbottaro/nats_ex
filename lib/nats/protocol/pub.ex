defmodule Nats.Protocol.Pub do
  @moduledoc false
  defstruct [:subject, :reply_to, :payload, :bytes]

  def new(subject, opts \\ []) do
    {bytes, payload} = process_payload(opts[:payload])

    %__MODULE__{
      subject: subject,
      reply_to: opts[:reply_to],
      payload: payload,
      bytes: bytes
    }
  end

  def to_iodata(pub) when is_struct(pub, __MODULE__) do
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

  defp process_payload(nil), do: {0, nil}
  defp process_payload(s) when is_binary(s), do: {byte_size(s), s}
  defp process_payload(m) when is_map(m), do: process_payload(Jason.encode!(m))
  defp process_payload(l) when is_list(l), do: process_payload(Map.new(l))

end
