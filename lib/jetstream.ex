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

  def stream_msg_delete(pid, name, seq, opts \\ []) do
    no_erase = Keyword.get(opts, :no_erase, false)
    payload = %{seq: seq, no_erase: no_erase}
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

  @doc """
  Ack, nak, or terminate a Jetstream message.

  ## Options

    * `:delay` - `(integer)` - Instruct Jetstream exactly when (in milliseconds)
    to redeliver the message. This only applies when `type` is `:nak`. Default
    `nil` (messages will be redelivered immediately).

  ## Examples

  Simple ack...

      iex> consumer_msg_ack(client, msg, :ack)
      {:ok, %Nats.Protocol.Msg{...}}

  You can use the `:reply_to` field directly...

      iex> consumer_msg_ack(client, msg.reply_to, :ack)
      {:ok, %Nats.Protocol.Msg{...}}

  Nak with immediate redelivery...

      iex> consumer_msg_ack(client, msg, :nak)
      {:ok, %Nats.Protocol.Msg{...}}

  Message will be redelivered 5 seconds from now...

      iex> consumer_msg_ack(client, msg, :nak, delay: 5000)
      {:ok, %Nats.Protocol.Msg{...}}

  Terminate message (no redelivery)...

      iex> consumer_msg_ack(client, msg, :term)
      {:ok, %Nats.Protocol.Msg{...}}

  """
  @spec consumer_msg_ack(Nats.Client.t, Nats.Protocol.Msg.t | binary, :ack | :nak | :term, Keyword.t) :: {:ok, Nats.Protocol.Msg.t} | {:error, reason}
  def consumer_msg_ack(client, msg_or_js_ack, type, opts \\ [])

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

  @spec parse_ack(Nats.Protocol.Msg.t | binary) :: %{
    stream: binary,
    consumer: binary,
    delivered: non_neg_integer,
    stream_seq: non_neg_integer,
    consumer_seq: non_neg_integer,
    timestamp: DateTime.t,
    pending: non_neg_integer,
    domain: nil | binary,
    account: nil | binary
  }
  def parse_ack(msg_or_reply_to)
  def parse_ack(%Nats.Protocol.Msg{reply_to: reply_to}), do: parse_ack(reply_to)
  def parse_ack(reply_to) do
    case String.split(reply_to, ".") do
      ["$JS", "ACK", stream, consumer, delivered, stream_seq, consumer_seq, timestamp, pending] ->
        %{domain: nil, account: nil, stream: stream, consumer: consumer, delivered: delivered, stream_seq: stream_seq, consumer_seq: consumer_seq, timestamp: timestamp, pending: pending}
      ["$JS", "ACK", domain, account, stream, consumer, delivered, stream_seq, consumer_seq, timestamp, pending] ->
        %{domain: domain, account: account, stream: stream, consumer: consumer, delivered: delivered, stream_seq: stream_seq, consumer_seq: consumer_seq, timestamp: timestamp, pending: pending}
    end
    |> Map.update!(:delivered, &String.to_integer/1)
    |> Map.update!(:stream_seq, &String.to_integer/1)
    |> Map.update!(:consumer_seq, &String.to_integer/1)
    |> Map.update!(:pending, &String.to_integer/1)
    |> Map.update!(:timestamp, fn timestamp ->
      String.to_integer(timestamp)
      |> DateTime.from_unix!(:nanosecond)
    end)
  end

  defp decode_response(resp) do
    with {:ok, msg} <- resp do
      payload = Jason.decode!(msg.payload)
      {:ok, %{msg | payload: payload}}
    end
  end

end
