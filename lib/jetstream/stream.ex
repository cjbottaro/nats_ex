defmodule Jetstream.Stream do
  @moduledoc """
  Create and use a Jetstream stream.

  ```
  defmodule MyStream do
    use Jetstream.Stream,
      host: "foo.bar.com",
      port: 4222,
      name: "my-stream",
      subjects: ["my-stream.*"]
  end

  :ok = MyStream.pub("my-stream.foo", "Hello, world!")
  ```

  ## Configuration

  Compile time configuration via `use Jetstream.Stream` options or `Config`.
  ```
  defmodule MyStream do
    use Jetstream.Stream, otp_app: :my_app
  end

  config :my_app, MyStream,
    host: "foo.bar.com",
    port: 4222,
    name: "my-stream",
    subjects: ["my-stream.*"]
  ```

  Runtime configuration via `runtime.exs` or the `c:init/1` callback.
  ```
  defmodule MyStream do
    use Jetstream.Stream, otp_app: :my_app

    def init(config) do
      Keyword.merge(config,
        host: "foo.bar.com",
        port: 4222,
        name: "my-stream",
        subjects: ["my-stream.*"]
      )
    end
  end
  ```
  """

  @defaults [
    host: "localhost",
    port: 4222,
  ]

  @doc """
  Default configuration

  ```
  #{inspect @defaults, pretty: true, width: 0}
  ```
  """
  def defaults, do: @defaults

  @doc """
  Configuration.

  Merges `Config` with `defaults/0`.
  """
  def config do
    Keyword.merge(
      @defaults,
      Application.get_application(__MODULE__)
      |> Application.get_env(__MODULE__, [])
    )
  end

  @callback init(config :: Keyword.t) :: Keyword.t
  @callback pub(subject :: binary, payload :: binary, opts :: Keyword.t) :: :ok | {:error, binary}
  @callback config() :: Keyword.t

  defmacro __using__(opts \\ []) do
    base = __MODULE__

    quote do
      @base unquote(base)
      @opts unquote(opts)

      def config do
        mix_config = Application.get_application(__MODULE__)
        |> Application.get_env(__MODULE__, [])

        @base.config()
        |> Keyword.merge(mix_config)
        |> Keyword.merge(@opts)
        |> Keyword.put(:module, __MODULE__)
      end

      def child_spec(config \\ []) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [config]}
        }
      end

      def start_link(config \\ []) do
        config = Keyword.merge(config(), config)

        # Jetstream.Stream.sanity_checks!(config)

        children = [
          %{
            id: {@base, __MODULE__},
            start: {Nats.Client, :start_link, [Keyword.put(config, :name, __MODULE__)]}
          },
          {Jetstream.Stream.Creator, config},
        ]

        Supervisor.start_link(children, strategy: :one_for_one)
      end

      def publish(subject, payload, opts \\ []) do
        Jetstream.publish(__MODULE__, subject, payload, opts)
      end

      def info do
        Jetstream.stream_info(__MODULE__, config()[:name])
      end

    end
  end
end
