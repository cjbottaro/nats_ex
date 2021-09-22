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
    Logger.info "#{inspect self()} starting up -- #{config[:concurrency]} concurrency"

    # For shutdown grace period.
    Process.flag(:trap_exit, true)

    conns = Enum.map(1..config[:ack_pool], fn _ ->
      {:ok, conn} = Nats.Client.start_link(config)
      conn
    end)

    {:ok, info} = Enum.random(conns)
    |> Jetstream.consumer_info(config[:stream], config[:consumer])

    ack_wait = info.payload["config"]["ack_wait"] / 1_000_000 |> trunc()

    {:ok, handler_state} = config[:module].init(config)

    state = %{
      config: config,
      handler_state: handler_state,
      tasks: %{},
      ack_wait: ack_wait,
      conns: conns,
      producers: []
    }

    producers = Enum.map(1..config[:fetch_pool], fn i ->
      {
        Jetstream.Consumer.Fetcher.name(config, i),
        [min_demand: 0, max_demand: 1]
      }
    end)

    GenStage.cast(self(), :start_asking)

    {:consumer, state, subscribe_to: producers}
  end

  def handle_subscribe(:producer, _opts, from, state) do
    {:manual, %{state | producers: [from | state.producers]}}
  end

  def handle_cast(:start_asking, state) do
    Enum.each(1..state.config[:concurrency], fn _ ->
      ask(state, 1)
    end)

    {:noreply, [], state}
  end

  def handle_events([message], _from, state) do
    %{config: config, tasks: tasks, ack_wait: ack_wait, handler_state: handler_state} = state

    start_time = System.monotonic_time(:millisecond)
    task = Task.async(config[:module], :handle_message, [message, handler_state])
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
  def handle_info({ref, value}, state) when is_map_key(state.tasks, ref) do
    {:ok, handler_state} = value
    {:noreply, [], %{state | handler_state: handler_state}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} -> {:noreply, [], state}
      {task, tasks} ->
        Process.cancel_timer(task.timer)
        :ok = ack_or_nak(state, task, reason)
        :ok = ask(state, 1)
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
      :ok = ask(state, 1) # Don't forget this.
    end

    {:noreply, [], %{state | tasks: tasks}}
  end

  def terminate(_reason, state) do
    count = map_size(state.tasks)
    Logger.info "Worker stage #{inspect self()} shutting down -- #{count} tasks running"

    Enum.each(1..state.config[:fetch_pool], fn i ->
      :ok = Jetstream.Consumer.Fetcher.name(state.config, i)
      |> GenStage.call(:shutdown)
    end)

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

    conns = state.conns
    message = task.message

    Logger.info("#{log} #{message.reply_to}")

    Enum.random(conns)
    |> Nats.Client.pub(message.reply_to, payload: payload)
  end

  defp ask(state, n) do
    :ok = Enum.random(state.producers)
    |> GenStage.ask(n)
  end

end
