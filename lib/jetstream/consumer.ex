defmodule Jetstream.Consumer do
  @moduledoc """
  Create a Jetstream consumer.

  ## Quickstart

      defmodule MyConsumer do
        use Jetstream.Consumer, stream: "change-stream"

        def handle_msg(msg) do
          # Do something with msg
          :ack
        end
      end

  ## Configuration

  * `:nats` - `t:Nats.Client.t/0` - Nats client process`
  * `:stream` - `binary` - Jetstream stream name
  * `:name` - `binary` - Jetstream consumer name, default `inspect(__MODULE__)`
  * `:filter_subject` - `binary|nil` - Can accept wildcards, default `nil`
  * `:deliver_subject` - `binary` - Default `inspect(__MODULE__)`
  * `:deliver_group` - `binary|nil` - Default `inspect(__MODULE__)`
  * `:ack_policy` - `binary` - Default `"explicit"`
  * `:ack_wait` - `integer` - Default `duration(2, :second, in: :nanosecond)`
  * `:max_ack_pending` - `integer` - Default `1000`
  * `:max_concurrency` - `integer` - Default `20`
  * `:pull` - `boolean` - Make a pull based consumer. Default `false`.

  If a consumer is push based, the following options are ignored:

      :max_concurrency

  If a consumer is pull based, the following options are ignored:

      :delivery_subject
      :delivery_group

  `use` options will be merged with `Config` with the latter taking precedence.

       import Config
       config :my_app, MyConsumer, stream: "foobar"

       iex> MyConsumer.config()
       [
         nats: Jetstream,
         stream: "foobar",
         name: "MyConsumer",
         filter_subject: nil,
         delivery_subject: "MyConsumer",
         delivery_group: "MyConsumer",
         ack_policy: "explicit",
         ack_wait: 2_000_000_000,
         max_ack_pending: 1000,
         max_concurrency: 20
         pull: false
       ]

  ## Error handling

  If `c:handle_msg/1` raises an exception, then an error handling callback is invoked.
  The default implemention noops.

  This can be used for retries with exponential backoff, reporting to Sentry, etc.

  See `c:handle_error/3` for more info.
  """

  @type msg :: %Nats.Protocol.Msg{}

  @doc """
  Get consumer configuration.
  """
  @callback config :: Keyword.t

  @doc """
  Create consumer.

  Calls `Jetstream.consumer_create/4` with args from `c:config/0`.
  """
  @callback create :: {:ok, msg} | {:error, term}

  @doc """
  Delete consumer.

  Calls `Jetstream.consumer_delete/3` with args from `c:config/0`.
  """
  @callback delete :: {:ok, msg} | {:error, term}

  @doc """
  Get consumer info.

  Calls `Jetstream.consumer_info/3` with args from `c:config/0`.
  """
  @callback info :: {:ok, msg} | {:error, term}

  @doc """
  Callback to handle stream messages.
  """
  @callback handle_msg(msg) :: :ack | :nak | :term | {:nak, Keyword.t} | {:term, Keyword.t}

  @doc """
  Callback to handle errors.

  If `c:handle_msg/1` raises an exception, this callback will be invoked.

  If this callback raises an exception, it will be rescued and logged.

  The return value is ignored.

  ## Examples

  Retry with exponential backoff...

      def handle_error(msg, _e, _trace) do
        info = Jetstream.parse_ack(msg)
        time = :math.pow(2, info.delivered) * 1_000 |> round()
        Jetstream.ack(NatsConn, :nak, delay: time)
      end

  Report to Sentry...

      def handle_error(msg, e, trace) do
        Sentry.capture_exception(e,
          result: :none
          stacktrace: trace,
          tags: %{foo: "bar"},
          extra: %{biz: "baz}
        )
      end

  """
  @callback handle_error(msg, Exception.t, Exception.stacktrace) :: any

  defmacro __using__(config \\ []) do
    quote location: :keep do
      @behaviour Jetstream.Consumer

      defaults = [
        nats: Jetstream,
        stream: nil,
        name: inspect(__MODULE__),
        filter_subject: nil,
        ack_policy: "explicit",
        ack_wait: 2_000_000_000_000,
        max_ack_pending: 1000,
        deliver_subject: inspect(__MODULE__),
        deliver_group: inspect(__MODULE__),
        max_concurrency: 20,
        pull: false,
      ]

      @config Keyword.merge(defaults, unquote(config))

      def config do
        config = Application.get_application(__MODULE__)
        |> Application.get_env(__MODULE__, [])

        config = Keyword.merge(@config, config)

        # We can't have dots in consumer name, delivery subject, etc.
        config
        |> Keyword.update!(:name, &String.replace(&1, ".", "-"))
        |> Keyword.update!(:deliver_subject, &String.replace(&1, ".", "/"))
        |> Keyword.update!(:deliver_group, & &1 && String.replace(&1, ".", "/"))
      end

      def info do
        config = config()
        Jetstream.consumer_info(
          config[:nats],
          config[:stream],
          config[:name]
        )
      end

      def create do
        config = config()

        consumer_config = if config[:pull] do
          Keyword.drop(config, [:deliver_subject, :deliver_group])
        else
          config
        end

        Jetstream.consumer_create(
          config[:nats],
          config[:stream],
          config[:name],
          consumer_config
        )
      end

      def delete do
        config = config()
        Jetstream.consumer_delete(
          config[:nats],
          config[:stream],
          config[:name]
        )
      end

      def handle_error(_msg, _e, _trace), do: nil
      defoverridable(handle_error: 3)

      use GenServer
      require Logger

      def start_link(config \\ []) do
        GenServer.start_link(__MODULE__, Keyword.merge(config(), config))
      end

      def init(config) do
        {:ok, _msg} = create()

        if config[:pull] do
          send(self(), :request_work)
        else
          {:ok, _sid} = Nats.Client.sub(
            config[:nats], config[:deliver_subject],
            queue_group: config[:deliver_group]
          )
        end

        state = %{
          config: config,
          jobs: %{},
          ack_wait: round(config[:ack_wait] / 1_000_000),
          max_concurrency: config[:max_concurrency], # Not used if push based.
        }

        {:ok, state}
      end

      # In a push consumer, we don't ask for work.
      def handle_info(%Nats.Protocol.Msg{} = msg, state) do
        {:noreply, start_job(msg, state)}
      end

      # In a pull consumer, we limit ourselves to max_concurrency.
      def handle_info(:request_work, %{jobs: jobs, max_concurrency: max_concurrency} = state)
      when map_size(jobs) == max_concurrency, do: {:noreply, state}

      # In a pull consumer, we have to ask for work.
      def handle_info(:request_work, state) do
        %{config: config} = state

        state = case Jetstream.consumer_msg_next(config[:nats], config[:stream], config[:name]) do
          {:ok, %{headers: ["NATS/1.0 408 Request Timeout"]}} -> state

          {:ok, %{headers: ["NATS/1.0 409 Consumer is push based"]}} ->
            Logger.critical("Consumer #{config[:stream]}/#{config[:name]} is push based")
            Process.exit(self(), :kill)

          {:ok, msg} -> start_job(msg, state)

          _error -> state
        end

        send(self(), :request_work)

        {:noreply, state}
      end

      # Task is taking longer than ack_wait.
      def handle_info({:ack_wait_exceeded, ref}, state) do
        if job = state.jobs[ref] do
          Logger.warn("Ack wait expired for #{job.msg.reply_to}")
          Task.Supervisor.terminate_child(Task.Supervisor, job.task.pid)
        end

        {:noreply, state}
      end

      # Task reported a value, but we just noop and let the :DOWN message handle
      # things to stay dry.
      def handle_info({ref, _result}, state) when is_reference(ref) do
        {:noreply, state}
      end

      # Task ended, update jobs and request more work if pull based.
      def handle_info({:DOWN, ref, :process, _pid, _how}, state) do
        %{jobs: jobs, config: config} = state

        {job, jobs} = Map.pop(jobs, ref)

        if job do
          Process.cancel_timer(job.timer)
        end

        if config[:pull] do
          send(self(), :request_work)
        end

        {:noreply, %{state | jobs: jobs}}
      end

      defp start_job(msg, state) do
        task = Task.Supervisor.async_nolink(Task.Supervisor, fn -> handle_msg(msg, state) end)
        timer = Process.send_after(self(), {:ack_wait_exceeded, task.ref}, state.ack_wait)
        jobs = Map.put(state.jobs, task.ref, %{task: task, timer: timer, msg: msg})
        %{state | jobs: jobs}
      end

      defp handle_msg(msg, state) do
        jid = String.split(msg.reply_to, ".")
        |> Enum.drop(2)
        |> Enum.join(".")

        Logger.info("🚀 #{jid} started")

        {time, result} = :timer.tc fn ->
          try do
            case handle_msg(msg) do
              how when how in [:ack, :nak, :term] -> {how, []}
              {how, opts} when how in [:nak, :term] -> {how, opts}
            end
          rescue
            e -> {:exception, e, __STACKTRACE__}
          end
        end

        time = Nats.Utils.format_duration(time, :microsecond)

        case result do
          {:exception, e, trace} ->
            err_message = Exception.format(:error, e, trace)
            Logger.info("💥 #{jid} raised in #{time}")
            Logger.error("#{jid}\n#{err_message}")
            try do
              handle_error(msg, e, trace)
            rescue
              e ->
                err_message = Exception.format(:error, e, __STACKTRACE__)
                Logger.info("🥴 #{jid} raised in error handler")
                Logger.error("#{jid} (error handler)\n#{err_message}")
            end

          {how, opts} ->
            case Jetstream.consumer_msg_ack(state.config[:nats], msg, how, opts) do
              {:ok, _msg} -> case how do
                :ack  -> Logger.info("🥂 #{jid} succeeded in #{time}")
                :nak  -> Logger.info("🚫 #{jid} failed in #{time}")
                :term -> Logger.info("💀 #{jid} aborted in #{time}")
              end
              _error -> Logger.warn("⚡️ #{jid} server unavailable")
            end
        end

        :ok
      end

    end
  end

end
