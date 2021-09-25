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

  def debug(module, i) do
    GenServer.call(name([module: module], i), :debug)
  end

  def init({config, i}) do
    Logger.info "#{inspect self()} fetcher-#{i} starting up -- #{config[:stream]} / #{config[:consumer]}"

    {:ok, conn} = Keyword.put(config, :notify, self())
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
      connected: false,
      demand: 0,
      batch: 0,
    }

    Process.send_after(self(), :report, 1_000)

    {:producer, state}
  end

  def handle_call(:debug, _from, state) do
    {:reply, state, [], state}
  end

  def handle_call(:shutdown, _from, state) do
    Logger.info "Fetcher stage #{inspect self()} shutting down -- unsubbing #{state.inbox}"
    resp = Nats.Client.unsub(state.conn, state.inbox_sid)
    {:reply, resp, [], state}
  end

  def handle_demand(1, state) do
    state = %{state | demand: state.demand + 1}
    {:noreply, [], next_batch(state)}
  end

  def handle_info({:nats_client_connect, _pid}, state) do
    state = %{state | connected: true, batch: 0}
    {:noreply, [], next_batch(state)}
  end

  def handle_info({:nats_client_disconnect, _pid}, state) do
    {:noreply, [], %{state | connected: false}}
  end

  def handle_info(%Nats.Protocol.Msg{} = message, state) do
    state = %{ state |
      demand: state.demand - 1,
      batch: state.batch - 1
    }

    {:noreply, [message], next_batch(state)}
  end

  def handle_info(:report, state) do
    %{demand: demand, batch: batch} = state

    Logger.info("Fetcher demand: #{demand}")
    Logger.info("Fetcher  batch: #{batch}")

    Process.send_after(self(), :report, 1_000)

    {:noreply, [], state}
  end

  defp next_batch(state) when not state.connected, do: state
  defp next_batch(state) when state.batch > 0, do: state
  defp next_batch(state) do
    %{conn: conn, next_msg_subject: next_msg_subject, inbox: inbox} = state

    # Determine batch size.
    batch = max(state.config[:min_batch], state.demand)
    batch = case state.config[:max_batch] do
      nil -> batch
      max -> min(batch, max)
    end

    :telemetry.execute([:nats, :jetstream, :consumer, :next_batch], %{batch_size: batch})
    Logger.info("Fetcher next_batch: #{batch}")

    payload = %{batch: batch} |> Jason.encode!()
    :ok = Nats.Client.pub(conn, next_msg_subject, reply_to: inbox, payload: payload)

    %{state | batch: batch}
  end

end
