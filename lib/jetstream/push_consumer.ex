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
    deliver_subject = config[:module]
    |> Macro.underscore()
    |> String.replace("/", "_")

    {:ok, conn} = Nats.Client.start_link(config)
    {:ok, _sid} = Nats.Client.sub(conn, deliver_subject, queue_group: deliver_subject)
    {:ok, %{payload: info}} = Jetstream.create_consumer(
      conn, config[:stream], config[:consumer],
      Keyword.merge(config, [deliver_subject: deliver_subject, deliver_group: deliver_subject])
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
      flow_control: nil,
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
      n -> Logger.info("Running jobs(#{state[:config][:i]}): #{n}")
    end

    Process.send_after(self(), :housekeeping, @housekeeping_interval)

    {:noreply, state}
  end

  def handle_info(%Msg{payload: nil, headers: headers} = msg, state) do
    cond do
      "NATS/1.0 100 FlowControl Request" in headers ->
        {:noreply, %{state | flow_control: msg.reply_to}, {:continue, :flow_control}}

      "NATS/1.0 100 Idle Heartbeat" in headers ->
        flow_control = Enum.find_value(headers, fn header ->
          case String.split(header, ":") do
            ["Nats-Consumer-Stalled", subject] -> String.trim(subject)
            _ -> false
          end
        end)
        if flow_control do
          {:noreply, %{state | flow_control: flow_control}, {:continue, :flow_control}}
        else
          {:noreply, state}
        end

      true ->
        Logger.error "Unexpected msg: #{inspect msg}"
    end
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

    {:noreply, %{state | jobs: jobs}, {:continue, :flow_control}}
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

  def handle_continue(:flow_control, %{flow_control: flow_control, jobs: jobs} = state)
  when flow_control != nil and map_size(jobs) == 0 do
    %{nats: nats, flow_control: flow_control} = state

    Logger.info("Flow control(#{state[:config][:i]}): #{flow_control}")
    Nats.Client.pub(nats, flow_control)

    {:noreply, %{state | flow_control: nil}}
  end

  def handle_continue(:flow_control, state) do
    {:noreply, state}
  end

  defmacro __using__(config \\ []) do
    quote location: :keep do
      @config unquote(config)

      def child_spec(i) do
        %{
          id: {Jetstream.Consumer, {__MODULE__, i}},
          start: {__MODULE__, :start_link, [[i: i]]}
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
