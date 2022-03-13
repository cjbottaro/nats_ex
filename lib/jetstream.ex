defmodule Jetstream do
  @moduledoc """
  Helper functions for the Jetstream API.
  """
  import Nats.Utils

  @type reason :: binary | atom

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
  {:ok, _msg} = stream_create(conn, "foo")
  {:ok, _msg} = stream_create(conn, "foo", subjects: ["foo.*"])
  ```
  """
  @spec stream_create(Nats.Client.t, binary, Keyword.t) :: {:ok, map} | {:error, term}
  def stream_create(pid, name, opts \\ []) do
    {opts, _trash} = default_opts(opts, @defaults)

    payload = Keyword.put(opts, :name, name) |> Map.new()

    Nats.Client.request(pid, "$JS.API.STREAM.CREATE.#{name}", payload: payload)
    |> decode_response()
  end

  def stream_update(pid, name, config \\ []) do
    with {:ok, %{payload: %{"config" => old_config}}} <- stream_info(pid, name) do
      new_config = Map.new(config, fn {k, v} -> {to_string(k), v} end)
      config = Map.merge(old_config, new_config)
      Nats.Client.request(pid, "$JS.API.STREAM.UPDATE.#{name}", payload: config)
      |> decode_response()
    end
  end

  def stream_list(pid) do
    Nats.Client.request(pid, "$JS.API.STREAM.NAMES")
    |> decode_response()
  end

  def stream_info(pid, name) do
    Nats.Client.request(pid, "$JS.API.STREAM.INFO.#{name}")
    |> decode_response()
  end

  def stream_delete(pid, name) do
    Nats.Client.request(pid, "$JS.API.STREAM.DELETE.#{name}")
    |> decode_response()
  end

  def stream_msg_get(pid, name, seq) do
    payload = %{seq: seq}
    Nats.Client.request(pid, "$JS.API.STREAM.MSG.GET.#{name}", payload: payload)
    |> decode_response()
  end

  def stream_msg_delete(pid, name, seq) do
    payload = %{seq: seq}
    Nats.Client.request(pid, "$JS.API.STREAM.MSG.DELETE.#{name}", payload: payload)
    |> decode_response()
  end

  def publish(pid, subject, payload, opts \\ []) do
    {async, opts} = Keyword.pop(opts, :async, false)
    opts = Keyword.put(opts, :payload, payload)

    if async do
      Nats.Client.pub(pid, subject, opts)
    else
      case Nats.Client.request(pid, subject, opts) do
        {:ok, %{bytes: 0}} -> :ok
        {:ok, %{payload: json}} -> case Jason.decode(json) do
          {:ok, %{"error" => %{"description" => reason}}} -> {:error, reason}
          {:ok, payload} -> {:ok, payload}
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
  def consumer_create(pid, stream_name, name, opts \\ []) do
    opts = Keyword.merge(@defaults, opts)

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

  def consumer_delete(pid, stream_name, name) do
    Nats.Client.request(pid, "$JS.API.CONSUMER.DELETE.#{stream_name}.#{name}")
    |> decode_response()
  end

  def consumer_info(pid, stream_name, name) do
    Nats.Client.request(pid, "$JS.API.CONSUMER.INFO.#{stream_name}.#{name}")
    |> decode_response()
  end

  def consumer_list(pid, stream_name) do
    Nats.Client.request(pid, "$JS.API.CONSUMER.LIST.#{stream_name}")
    |> decode_response()
  end

  @defaults [batch: 1, no_wait: false, expires: 5000]
  def consumer_msg_next(pid, stream_name, consumer_name, opts \\ []) do
    {expires, opts} = Keyword.merge(@defaults, opts)
    |> Keyword.get_and_update!(:expires, &{&1, &1 * 1_000_000})

    payload = Map.new(opts) |> Jason.encode!()
    subject = "$JS.API.CONSUMER.MSG.NEXT.#{stream_name}.#{consumer_name}"

    Nats.Client.request(pid, subject, payload: payload, timeout: expires + 200, v: 1)
  end

  @spec consumer_msg_ack(Nats.Client.t, Nats.Protocol.Msg.t | binary, atom, Keyword.t) :: {:ok, Nats.Protocol.Msg.t} | {:error, reason}
  def consumer_msg_ack(client, msg_or_subject, type, opts \\ [])

  def consumer_msg_ack(pid, msg, type, opts) when is_struct(msg) do
    consumer_msg_ack(pid, msg.reply_to, type, opts)
  end

  def consumer_msg_ack(pid, subject, type, opts) when is_binary(subject) do
    payload = case type do
      :ack -> "+ACK"
      :nak -> case opts[:delay] do
        nil -> "-NAK"
        delay -> "-NAK " <> Jason.encode!(%{delay: delay * 1_000_000})
      end
      :term -> "+TERM"
    end
    Nats.Client.request(pid, subject, Keyword.put(opts, :payload, payload))
  end

  defp decode_response(resp) do
    with {:ok, msg} <- resp do
      payload = Jason.decode!(msg.payload)
      {:ok, %{msg | payload: payload}}
    end
  end

end
