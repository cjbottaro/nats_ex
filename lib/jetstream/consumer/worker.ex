defmodule Jetstream.Consumer.Worker do
  @moduledoc false
  use GenStage
  require Logger

  def child_spec(config) do
    %{
      id: id(config),
      start: {__MODULE__, :start_link, [config]},
      shutdown: config[:shutdown_grace_period]
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
    Logger.info "Worker stage #{inspect self()} starting up -- #{config[:concurrency]} concurrency"

    # For shutdown grace period.
    Process.flag(:trap_exit, true)

    {:ok, conn} = Nats.Client.start_link(config)
    {:ok, info} = Jetstream.consumer_info(conn, config[:stream], config[:consumer])
    ack_wait = info.payload["config"]["ack_wait"] / 1_000_000 |> trunc()

    state = %{ config: config, tasks: %{}, ack_wait: ack_wait, conn: conn, producer: nil }
    producer = Jetstream.Consumer.Fetcher.name(config)
    options = [min_demand: 0, max_demand: 1]

    {:consumer, state, subscribe_to: [{producer, options}]}
  end

  def handle_subscribe(:producer, _opts, from, state) do
    Enum.each(1..state.config[:concurrency], fn _ ->
      :ok = GenStage.ask(from, 1)
    end)
    {:manual, %{state | producer: from}}
  end

  def handle_events([message], _from, state) do
    %{config: config, tasks: tasks, ack_wait: ack_wait} = state

    start_time = System.monotonic_time(:millisecond)
    task = Task.async(config[:module], :handle_message, [message])
    timer = Process.send_after(self(), {:ack_timeout, task.ref}, ack_wait)

    # Annotate the task some and store it.
    task = Map.merge(task, %{start_time: start_time, timer: timer, message: message})
    tasks = Map.put(tasks, task.ref, task)

    {:noreply, [], %{state | tasks: tasks}}
  end

  # Task.async both links and monitors, so we can ignore the :EXIT messages from
  # the link because we're already handling the :DOWN messages from the monitor.
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, [], state}
  end

  # Tasks report their return value which we can ignore.
  def handle_info({ref, _value}, state) when is_map_key(state.tasks, ref) do
    {:noreply, [], state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} -> {:noreply, [], state}
      {task, tasks} ->
        Process.cancel_timer(task.timer)
        :ok = ack_or_nak(state, task, reason)
        :ok = GenStage.ask(state.producer, 1)
        {:noreply, [], put_in(state.tasks, tasks)}
    end
  end

  # We don't need to nak because Jetstream will automatically retry on messages
  # on timeout.
  def handle_info({:ack_timeout, ref}, state) do
    {task, tasks} = Map.pop(state.tasks, ref)

    # If the task doesn't exist or shutdown doesn't return nil, that means the
    # task has ended and will be (was) handled by :DOWN in handle_info.
    if task && Task.shutdown(task, :brutal_kill) == nil do
      elapsed = (System.monotonic_time(:millisecond) - task.start_time) / 1000 |> round()
      Logger.warn("Timeout while processing #{task.message.reply_to} #{elapsed}s")
      :ok = GenStage.ask(state.producer, 1) # Don't forget this.
    end

    {:noreply, [], %{state | tasks: tasks}}
  end

  def terminate(_reason, state) do
    count = map_size(state.tasks)
    Logger.info "Worker stage #{inspect self()} shutting down -- #{count} tasks running"

    :ok = Jetstream.Consumer.Fetcher.name(state.config)
    |> GenStage.call(:shutdown)

    Map.values(state.tasks)
    |> Task.yield_many(state.config[:shutdown_grace_period] - 1000)
    |> Enum.each(fn
      {task, {:ok, _value}} -> ack_or_nak(state, task, :normal)
      {task, {:exit, error}} -> ack_or_nak(state, task, error)
      {task, nil} -> case Task.shutdown(task, :brutal_kill) do
        {:ok, _value} -> ack_or_nak(state, task, :normal)
        {:exit, error} -> ack_or_nak(state, task, error)
        nil -> ack_or_nak(state, task, :shutdown)
      end
    end)
  end

  @spec ack_or_nak(map, Task.t, :normal | term) :: :ok
  defp ack_or_nak(state, task, reason) do
    {log, payload} = case reason do
      :normal -> {"ACK 🥂", "+ACK"}
      _error  -> {"NAK 💥", "-NAK"}
    end

    conn = state.conn
    message = task.message

    Logger.info("#{log} #{message.reply_to}")
    Nats.Client.pub(conn, message.reply_to, payload: payload)
  end

end
