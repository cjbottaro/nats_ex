defmodule Jetstream.Consumer.Fetcher do
  @moduledoc false
  use GenStage
  require Logger

  def child_spec({config, i}) do
    %{
      id: id(config, i),
      start: {__MODULE__, :start_link, [config, i]}
    }
  end

  def start_link(config, i) do
    GenStage.start_link(__MODULE__, {config, i}, name: name(config, i))
  end

  defp id(config, i) do
    {__MODULE__, {config[:module], i}}
  end

  def name(config, i) do
    {:global, id(config, i)}
  end

  def init({config, i}) do
    Logger.info "#{inspect self()} fetcher-#{i} starting up -- #{config[:stream]} / #{config[:consumer]}"

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
      next_msg_subject: next_msg_subject,
      demand: 0
    }

    {:producer, state}
  end

  def handle_call(:shutdown, _from, state) do
    Logger.info "Fetcher stage #{inspect self()} shutting down -- unsubbing #{state.inbox}"
    resp = Nats.Client.unsub(state.conn, state.inbox_sid)
    {:reply, resp, [], state}
  end

  def handle_demand(1, state) do
    %{demand: demand} = state

    if demand == 0 do
      %{conn: conn, next_msg_subject: next_msg_subject, inbox: inbox} = state
      payload = %{batch: 10} |> Jason.encode!()
      :ok = Nats.Client.pub(conn, next_msg_subject, reply_to: inbox, payload: payload)
    end

    {:noreply, [], %{state | demand: state.demand + 1}}
  end

  def handle_info(%Nats.Protocol.Msg{} = message, state) do
    %{demand: demand} = state

    if demand > 0 do
      %{conn: conn, next_msg_subject: next_msg_subject, inbox: inbox} = state
      payload = %{batch: 10} |> Jason.encode!()
      :ok = Nats.Client.pub(conn, next_msg_subject, reply_to: inbox, payload: payload)
    end

    {:noreply, [message], %{state | demand: state.demand - 1}}
  end

end
