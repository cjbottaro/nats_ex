defmodule Nats.Rpc.Client do

  defmacro __using__(opts \\ []) do
    base = __MODULE__

    quote do
      @opts unquote(opts)
      @base unquote(base)

      def child_spec(config \\ []) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [config]}
        }
      end

      def start_link(config \\ [], opts \\ []) do
        require Logger
        Logger.info("Starting RPC client #{inspect __MODULE__}")
        Nats.Client.start_link(config, Keyword.put(opts, :name, __MODULE__))
      end

      def config do
        Application.get_env(@opts[:otp_app], __MODULE__, [])
        |> Keyword.merge(@opts)
      end

      def call(subject, args, opts \\ []) do
        opts = opts
        |> Keyword.put(:process_response, {__MODULE__, :process_response, []})
        |> Keyword.put(:process_request, {__MODULE__, :process_request, []})

        @base.call(__MODULE__, subject, args, opts)
      end

      def call!(subject, args, opts \\ []) do
        opts = opts
        |> Keyword.put(:process_response, {__MODULE__, :process_response, []})
        |> Keyword.put(:process_request, {__MODULE__, :process_request, []})

        @base.call!(__MODULE__, subject, args, opts)
      end

      def process_request(payload), do: payload
      defoverridable(process_request: 1)

      def process_response(payload), do: payload
      defoverridable(process_response: 1)
    end
  end

  def call(pid, topic, args, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    payload = %{args: args}

    payload = case opts[:process_request] do
      {mod, name, args} -> apply(mod, name, [payload | args])
      f when is_function(f) -> f.(payload)
      nil -> payload
    end

    payload = Cyanide.encode!(payload)

    case Nats.Client.request(pid, topic, payload: payload, timeout: timeout) do
      {:ok, msg} -> handle_response(msg.payload, opts)
      error -> error
    end
  end

  defmodule Error, do: defexception [:message]
  defmodule TimeoutError, do: defexception [:message]

  def call!(pid, topic, args, opts \\ []) do
    case call(pid, topic, args, opts) do
      {:ok, value} -> value
      {:error, :timeout} -> raise(TimeoutError, "request timed out")
      {:error, error} ->
        {e, message} = error_to_exception(error)
        raise(e, message)
    end
  end

  defp handle_response(json, opts) do
    payload = Cyanide.decode!(json)

    payload = case opts[:process_response] do
      nil -> payload
      {m, f, a} -> apply(m, f, [payload | a])
      f when is_function(f) -> f.(payload)
    end

    case payload do
      %{"error" => [type, message]} -> {:error, {type, message}}
      %{"value" => value} -> {:ok, value}
    end
  end

  defp error_to_exception({type, message}) do
    {Module.safe_concat([type]), message}
  rescue
    ArgumentError -> {Error, "#{type}: #{message}"}
  end

end
