defmodule Jetstream.Consumer.Fetcher do
  @moduledoc false
  use GenStage
  require Logger

  def child_spec(config) do
    %{
      id: id(config),
      start: {__MODULE__, :start_link, [config]}
    }
  end

  def start_link(config) do
    GenStage.start_link(__MODULE__, config, name: name(config))
  end

  defp id(config) do
    {__MODULE__, config[:module]}
  end

  def name(config) do
    {:global, id(config)}
  end

  def init(config) do
    Logger.info "Fetcher stage #{inspect self()} starting up -- #{config[:stream]} / #{config[:consumer]}"

    {:ok, conn} = Keyword.put(config, :msg_handler, self())
    |> Nats.Client.start_link()

    inbox = "_CONS." <> Nats.Utils.new_uid()
    {:ok, inbox_sid} = Nats.Client.sub(conn, inbox)

    stream = config[:stream]
    consumer = config[:consumer]
    next_msg_subject = "$JS.API.CONSUMER.MSG.NEXT.#{stream}.#{consumer}"

    state = %{
      config: config,
      conn: conn,
      inbox: inbox,
      inbox_sid: inbox_sid,
      next_msg_subject: next_msg_subject
    }

    {:producer, state}
  end

  def handle_call(:shutdown, _from, state) do
    Logger.info "Fetcher stage #{inspect self()} shutting down -- unsubbing #{state.inbox}"
    resp = Nats.Client.unsub(state.conn, state.inbox_sid)
    {:reply, resp, [], state}
  end

  def handle_demand(1, state) do
    %{conn: conn, next_msg_subject: next_msg_subject, inbox: inbox} = state
    payload = %{batch: 1} |> Jason.encode!()
    :ok = Nats.Client.pub(conn, next_msg_subject, reply_to: inbox, payload: payload)
    {:noreply, [], state}
  end

  def handle_info(%Nats.Protocol.Msg{} = message, state) do
    {:noreply, [message], state}
  end

end
