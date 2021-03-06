defmodule Jetstream.Consumer do
  @moduledoc """
  Define a Jetstream consumer, similar to a Faktory worker + job.

  ```
  defmodule MyConsumer do
    use Jetstream.Consumer,
      stream: "foo",
      consumer: "bar"

    def handle_message(msg) do
      IO.inspect(msg)
    end
  ```

  Additional options passed to `use Jetstream.Consumer` will be passed to
  `Jetstream.consumer_create/4` when the consumer is created on the Jetstream
  server. For example:
  ```
  use Jetstream.Consumer,
      stream: "foo",
      consumer: "bar",
      max_delivery: 10,
      ack_wait: 10_000_000_000, # 10 seconds
      filter_subject: ["change-stream.postgres"]
  ```

  `msg` in `c:handle_message/1` is a `Nats.Protocol.Msg` struct. If the function
  finishes without error, then the message is `+ACK`ed, otherwise it is `-NAK`ed
  and Jetstream will redeliver the message until `:max_deliver` is hit (which
  defaults to unlimited).

  If `c:handle_message/1` exceeds the configured `:ack_wait` time, then the
  process the function is running in will be terminated, and Jetstream will
  redeliver the message (again, up until `:max_delivery` times).

  ## Configuration

  Compile time configuration can be down via `use Jetstream.Consumer` or via
  `Config`.
  ```
  config :my_app, MyConsumer,
    host: "jetstream.company.com",
    port: 4222,
    stream: "foo",
    consumer: "bar"
  ```

  Runtime configuration can be done via the `c:init/1` callback.
  ```
  defmodule MyConsumer do
    use Jetstream.Consumer

    def init(config) do
      Keyword.merge(config,
        host: "jetstream.company.com",
        port: 4222,
        stream: "foo",
        consumer: "bar"
      )
    end
  ```

  All config methods are merged together to make the final config.
  """

  @callback handle_message(msg :: Nats.Protocol.Msg.t) :: any | none
  @callback init(config :: Keyword.t) :: Keyword.t

  defmacro __using__(opts \\ []) do
    quote location: :keep do
      @opts unquote(opts)

      if !@opts[:stream] do
        raise ArgumentError, "option :stream is required"
      end

      def init(config), do: {:ok, config}
      defoverridable(init: 1)

      def handle_error(error), do: nil
      defoverridable(handle_error: 1)

      @defaults [
        ack_wait: {30, :second},
        concurrency: 20,
        fetch_pool: 1,
        work_pool: 1,
        ack_pool: 1,
        max_batch: nil,
        shutdown_grace_period: 25_000,
      ]
      def config do
        mix_config = Application.get_application(__MODULE__)
        |> Application.get_env(__MODULE__, [])

        @defaults
        |> Keyword.merge(mix_config)
        |> Keyword.merge(@opts)
        |> Keyword.put(:module, __MODULE__)
      end

      def child_spec(config) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [config]}
        }
      end

      def start_link(opts \\ []) do
        config = Keyword.merge(config(), opts)
        |> Keyword.put_new(:consumer, Macro.underscore(__MODULE__))

        Jetstream.Consumer.sanity_checks!(config)

        children = [
          {Jetstream.Consumer.Creator, config},
          Enum.map(1..config[:fetch_pool], fn i -> {Jetstream.Consumer.Fetcher, {config, i}} end),
          Enum.map(1..config[:work_pool], fn i -> {Jetstream.Consumer.Worker, {config, i}} end),
        ]
        |> List.flatten()

        Supervisor.start_link(children, strategy: :one_for_one)
      end

    end
  end

  @doc false
  def sanity_checks!(config) do
    if config[:shutdown_grace_period] < 1_000 do
      raise ArgumentError, ":shutdown_grace_period cannot be less than 1000 ms"
    end
  end

end
