defmodule Jetstream.PushConsumer do
  use GenServer
  alias Nats.Protocol.Msg
  require Logger

  @housekeeping_interval 1_000

  defmodule Job do
    defstruct [:task, :start_at, :timer]
  end

  def start_link(config \\ []) do
    GenServer.start_link(__MODULE__, config)
  end

  def init(config) do
    deliver_subject = "_CONS.74976e46"

    IO.inspect(config)

    {:ok, conn} = Nats.Client.start_link(config)
    {:ok, _sid} = Nats.Client.sub(conn, deliver_subject)
    {:ok, %{payload: info}} = Jetstream.create_consumer(
      conn, config[:stream], config[:consumer],
      Keyword.put(config, :deliver_subject, deliver_subject)
    )

    ack_wait = case info["ack_wait"] do
      nil -> nil
      wait -> trunc(wait/1_000_000)
    end

    {:ok, handler_state} = config[:module].init(config)

    # For graceful shutdown.
    Process.flag(:trap_exit, true)

    state = %{
      config: config,
      nats: conn,
      module: config[:module],
      handler_state: handler_state,
      deliver_subject: deliver_subject,
      info: info,
      ack_wait: ack_wait,
      jobs: %{},
    }

    Process.send_after(self(), :housekeeping, @housekeeping_interval)

    {:ok, state}
  end

  def handle_info(:housekeeping, state) do
    case map_size(state.jobs) do
      0 -> nil
      n -> Logger.info("Running jobs: #{n}")
    end

    Process.send_after(self(), :housekeeping, @housekeeping_interval)

    {:noreply, state}
  end

  def handle_info(%Msg{} = msg, state) do
    %{module: module, ack_wait: ack_wait, jobs: jobs, handler_state: handler_state} = state

    start_at = System.monotonic_time(:microsecond)

    task = Task.async(module, :handle_message, [msg, handler_state])

    timer = case ack_wait do
      nil -> nil
      ack_wait -> Process.send_after(self(), {:ack_timeout, task.ref}, ack_wait)
    end

    job = %Job{start_at: start_at, task: task, timer: timer}
    jobs = Map.put(jobs, task.ref, job)

    {:noreply, %{state | jobs: jobs}}
  end

  # Tasks report their return value which we can ignore.
  def handle_info({ref, _value}, state) when is_map_key(state.jobs, ref) do
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    %{jobs: jobs} = state

    case Map.pop(jobs, ref) do
      {nil, _jobs} -> {:noreply, state}
      {_job, jobs} -> {:noreply, %{state | jobs: jobs}}
    end
  end

  # Task.async both links and monitors, so we can ignore the :EXIT messages from
  # the link because we're already handling the :DOWN messages from the monitor.
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  defmacro __using__(config \\ []) do
    quote location: :keep do
      @config unquote(config)

      def child_spec(config) do
        %{
          id: config[:module],
          start: {__MODULE__, :start_link, [config]}
        }
      end

      def start_link(config \\ []) do
        config = Keyword.merge(@config, config)
        |> Keyword.put(:module, __MODULE__)

        Jetstream.PushConsumer.start_link(config)
      end

      def init(_config), do: {:ok, nil}
      defoverridable(init: 1)

    end
  end

end
