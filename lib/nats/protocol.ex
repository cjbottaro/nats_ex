defmodule Nats.Protocol do
  @moduledoc false
  require Logger
  alias Nats.Protocol

  defdelegate pong(), to: Protocol.Pong, as: :new
  defdelegate connect(opts \\ []), to: Protocol.Connect, as: :new
  defdelegate sub(subject, sid), to: Protocol.Sub, as: :new
  defdelegate pub(subject, opts \\ []), to: Protocol.Pub, as: :new
  defdelegate unsub(sid, count \\ nil), to: Protocol.Unsub, as: :new

  def recv(_socket, "INFO", payload) do
    %Protocol.Info{payload: Jason.decode!(payload)}
  end

  def recv(_socket, "PING", nil) do
    %Protocol.Ping{}
  end

  def recv(socket, "MSG", payload) do
    Protocol.Msg.recv(socket, payload, false)
  end

  def recv(socket, "HMSG", payload) do
    Protocol.Msg.recv(socket, payload, true)
  end

end
