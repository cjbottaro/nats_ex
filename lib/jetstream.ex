defmodule Jetstream do
  @moduledoc """
  Helper functions for the Jetstream API.
  """
  import Nats.Utils

  @defaults [
    subjects: [],
    max_age: 0,
    max_bytes: -1,
    max_msg_size: -1,
    max_msgs: -1,
    max_consumers: -1,
    retention: :limits,
    discard: :old,
    storage: :file,
    num_replicas: 1,
    duplicate_window: nil,
  ]

  @doc """
  Create a stream.

  `opts` defaults to
  ```
  #{inspect @defaults, pretty: true, width: 0}
  ```
  ## Examples
  ```
  {:ok, _msg} = create_stream(conn, "foo")
  {:ok, _msg} = create_stream(conn, "foo", subjects: ["foo.*"])
  ```
  """
  @spec create_stream(Nats.Client.t, binary, Keyword.t) :: {:ok, Nats.Protocol.Msg.t} | {:error, binary}
  def create_stream(pid, name, opts \\ []) do
    {opts, _trash} = default_opts(opts, @defaults)

    payload = Keyword.put(opts, :name, name) |> Map.new()

    Nats.Client.request(pid, "$JS.API.STREAM.CREATE.#{name}", payload: payload)
    |> decode_response()
  end

  def update_stream(pid, name, config \\ []) do
    with {:ok, %{payload: %{"config" => old_config}}} <- stream_info(pid, name) do
      new_config = Map.new(config, fn {k, v} -> {to_string(k), v} end)
      config = Map.merge(old_config, new_config)
      Nats.Client.request(pid, "$JS.API.STREAM.UPDATE.#{name}", payload: config)
      |> decode_response()
    end
  end

  def list_streams(pid) do
    Nats.Client.request(pid, "$JS.API.STREAM.NAMES")
    |> decode_response()
  end

  def stream_info(pid, name) do
    Nats.Client.request(pid, "$JS.API.STREAM.INFO.#{name}")
    |> decode_response()
  end

  def delete_stream(pid, name) do
    Nats.Client.request(pid, "$JS.API.STREAM.DELETE.#{name}")
    |> decode_response()
  end

  def get_message(pid, name, seq) do
    payload = %{seq: seq}
    Nats.Client.request(pid, "$JS.API.STREAM.MSG.GET.#{name}", payload: payload)
    |> decode_response()
  end

  @defaults [async: false]
  def publish(pid, subject, payload, opts \\ []) do
    opts = Keyword.merge(@defaults, opts)
    if opts[:async] do
      Nats.Client.pub(pid, subject, payload: payload)
    else
      case Nats.Client.request(pid, subject, payload: payload) do
        {:ok, %{bytes: 0}} -> :ok
        {:ok, %{payload: json}} -> case Jason.decode(json) do
          {:ok, %{"error" => %{"description" => reason}}} -> {:error, reason}
          {:ok, _payload} -> :ok
          {:error, reason} -> {:error, reason}
        end
        error -> error
      end
    end
  end

  @defaults [
    ack_policy: :explicit,
    ack_wait: nil,
    max_deliver: -1,
    replay_policy: :instant,
    durable: true,
  ]
  def create_consumer(pid, stream_name, name, opts \\ []) do
    {opts, _trash} = default_opts(opts, @defaults)

    {endpoint, opts} = case Keyword.pop(opts, :durable) do
      {true, opts} -> {
        "$JS.API.CONSUMER.DURABLE.CREATE",
        Keyword.put(opts, :durable_name, name)
      }

      {_, opts} -> {"$JS.API.CONSUMER.CREATE", opts}
    end

    payload = %{
      stream_name: stream_name,
      config: Map.new(opts)
    }

    Nats.Client.request(pid, "#{endpoint}.#{stream_name}.#{name}", payload: payload)
    |> decode_response()
  end

  def delete_consumer(pid, stream_name, name) do
    Nats.Client.request(pid, "$JS.API.CONSUMER.DELETE.#{stream_name}.#{name}")
    |> decode_response()
  end

  def consumer_info(pid, stream_name, name) do
    Nats.Client.request(pid, "$JS.API.CONSUMER.INFO.#{stream_name}.#{name}")
    |> decode_response()
  end

  def list_consumers(pid, stream_name) do
    Nats.Client.request(pid, "$JS.API.CONSUMER.LIST.#{stream_name}")
    |> decode_response()
  end

  defp decode_response(resp) do
    with {:ok, msg} <- resp do
      payload = Jason.decode!(msg.payload)
      {:ok, %{msg | payload: payload}}
    end
  end

end
