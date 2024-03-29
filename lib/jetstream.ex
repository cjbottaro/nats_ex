defmodule Jetstream do
  @moduledoc """
  Helper functions for the Jetstream API.
  """

  @type name :: binary
  @type stream_name :: name
  @type consumer_name :: name

  @type reason :: binary | atom

  @type msg :: Nats.Protocol.Msg.t
  @type opts :: Keyword.t
  @type config :: Keyword.t | map

  @typedoc """
  A key-value related error.

  If `{:error, Nats.Protol.Msg.t}`, there was something semantically wrong with
  the request, and the msg should hold information about why

  If `{:error, term}`, there was probably a connection error.
  """
  @type kv_error :: {:error, Nats.Protocol.Msg.t()} | {:error, term}

  @typedoc """
  Stream sequence id.
  """
  @type seq :: pos_integer()

  @typedoc """
  KV entry revision.
  """
  @type revision :: seq

  @type stream_create_opt ::
    {:allow_direct, boolean}
    | {:allow_rollup_hdrs, boolean}
    | {:deny_delete, boolean}
    | {:deny_purge, boolean}
    | {:discard, :old | :new}
    | {:duplicate_window, non_neg_integer}
    | {:max_age, non_neg_integer}
    | {:max_bytes, non_neg_integer | -1}
    | {:max_consumers, non_neg_integer | -1}
    | {:max_msg_size, non_neg_integer | -1}
    | {:max_msgs, non_neg_integer | -1}
    | {:max_msgs_per_subject, non_neg_integer | -1}
    | {:mirror_direct, boolean}
    | {:num_replicas, non_neg_integer}
    | {:retention, :limits | :interest | :workqueue}
    | {:sealed, boolean}
    | {:storage, :file | :memory}
    | {:subject, [binary]}

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
  @spec stream_create(Nats.Client.t, binary, [stream_create_opt]) :: {:ok, map} | {:error, term}

  def stream_create(pid, name, opts \\ []) do
    opts = Keyword.merge(@defaults, opts)

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

  @spec stream_msg_get(Nats.Client.t, binary, integer) :: {:ok, Nats.Msg.t} | {:error, term}

  def stream_msg_get(pid, name, seq) when is_integer(seq) do
    stream_msg_get(pid, name, seq: seq)
  end

  @spec stream_msg_get(Nats.Client.t, binary, Keyword.t) :: {:ok, Nats.Msg.t} | {:error, term}

  def stream_msg_get(pid, name, opts) do
    Nats.Client.request(pid, "$JS.API.STREAM.MSG.GET.#{name}", payload: Map.new(opts))
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
          {:ok, %{"error" => error}} -> {:error, error}
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

  def consumer_update(pid, stream_name, name, config) do
    with {:ok, %{payload: %{"config" => old_config}}} <- consumer_info(pid, stream_name, name) do
      old_config = Keyword.new(old_config, fn {k, v} -> {String.to_atom(k), v} end)
      config = Keyword.merge(old_config, config)
      consumer_create(pid, stream_name, name, config)
    end
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
      if payload["error"] do
        {:error, %{msg | payload: payload}}
      else
        {:ok, %{msg | payload: payload}}
      end
    end
  end

  @callback stream_create(name, config) :: {:ok, msg} | {:error, reason}
  @callback stream_update(name, config) :: {:ok, msg} | {:error, reason}
  @callback stream_delete(name) :: {:ok, msg} | {:error, reason}
  @callback stream_info(name) :: {:ok, msg} | {:error, reason}
  @callback stream_list() :: {:ok, msg} | {:error, reason}
  @callback stream_msg_get(name, seq) :: {:ok, msg} | {:error, reason}
  @callback stream_msg_delete(name, seq, opts) :: {:ok, msg} | {:error, reason}

  @callback consumer_create(stream_name, name, config) :: {:ok, msg} | {:error, reason}
  @callback consumer_update(stream_name, name, config) :: {:ok, msg} | {:error, reason}
  @callback consumer_delete(stream_name, name) :: {:ok, msg} | {:error, reason}
  @callback consumer_info(stream_name, name) :: {:ok, msg} | {:error, reason}
  @callback consumer_list(stream_name) :: {:ok, msg} | {:error, reason}
  @callback consumer_msg_next(stream_name, name, opts) :: {:ok, msg} | {:error, reason}
  @callback consumer_msg_ack(msg | binary, :ack | :nak | :term, opts) :: {:ok, msg} | {:error, reason}

  defmacro __using__(opts \\ []) do
    nats = opts[:nats]
    if !nats do
      raise ArgumentError, ":nats option is required"
    end

    quote do
      @behaviour Jetstream
      @nats unquote(nats)

      def stream_create(name, config \\ []), do: Jetstream.stream_create(@nats, name, config)
      def stream_update(name, config), do: Jetstream.stream_update(@nats, name, config)
      def stream_delete(name), do: Jetstream.stream_delete(@nats, name)
      def stream_info(name), do: Jetstream.stream_info(@nats, name)
      def stream_list(), do: Jetstream.stream_list(@nats)
      def stream_msg_get(name, seq), do: Jetstream.stream_msg_get(@nats, name, seq)
      def stream_msg_delete(name, seq, opts), do: Jetstream.stream_msg_delete(@nats, name, seq, opts)

      def consumer_create(stream_name, name, config \\ []), do: Jetstream.consumer_create(@nats, stream_name, name, config)
      def consumer_update(stream_name, name, config), do: Jetstream.consumer_update(@nats, stream_name, name, config)
      def consumer_delete(stream_name, name), do: Jetstream.consumer_delete(@nats, stream_name, name)
      def consumer_info(stream_name, name), do: Jetstream.consumer_info(@nats, stream_name, name)
      def consumer_list(stream_name), do: Jetstream.consumer_list(@nats, stream_name)
      def consumer_msg_next(stream_name, name, opts \\ []), do: Jetstream.consumer_msg_next(@nats, stream_name, name, opts)
      def consumer_msg_ack(msg_or_js_ack, type, opts \\ []), do: Jetstream.consumer_msg_ack(@nats, msg_or_js_ack, type, opts)
    end
  end

  alias Jetstream.{Bucket, Entry}

  @doc """
  Create a KV bucket.

  ## Options

    * `history` - How much history to keep for keys. Default `1`.
    * `ttl` - Time to live in milliseconds for keys. Default `nil` (no ttl).
    * `max_size` - Total size limit in bytes of the bucket. Default `nil` (no limit).
    * `max_value_size` - Max size of a single key value. Default `nil` (use Nats server default, which is typically 1048576 bytes).
    * `num_replicas` - Replica count of the bucket. Default `1`.

  ## Examples

      # History 1, replication factor 1.
      bucket_create(client, "my-bucket")

      # History 5, replication factor 3.
      bucket_create(client, "my-bucket", history: 5, num_replicas: 3)

  """
  @spec bucket_create(Nats.Client.t, binary, Keyword.t) :: {:ok, Bucket.t} | kv_error
  defdelegate bucket_create(client, name, opts \\ []), to: Jetstream.Bucket, as: :create

  @doc """
  Update a KV bucket.

  This takes the same options as `create/3`.
  """
  @spec bucket_update(Nats.Client.t, binary, Keyword.t) :: {:ok, Bucket.t} | kv_error
  defdelegate bucket_update(client, name, opts), to: Jetstream.Bucket, as: :update

  @doc """
  Delete a KV bucket.
  """
  @spec bucket_delete(Nats.Client.t, binary) :: :ok | kv_error
  defdelegate bucket_delete(client, name), to: Jetstream.Bucket, as: :delete

  @doc """
  Get info on a KV bucket.
  """
  @spec bucket_info(Nats.Client.t, binary) :: {:ok, Bucket.t} | kv_error
  defdelegate bucket_info(client, name), to: Jetstream.Bucket, as: :info

  @doc """
  Get a list of all KV buckets.
  """
  @spec bucket_list(Nats.Client.t) :: {:ok, [binary]} | kv_error
  defdelegate bucket_list(client), to: Jetstream.Bucket, as: :list

  @doc """
  Create or update a KV entry.

  If `value` is a map, it will be automatically serialized as JSON and
  deserialized by `entry_fetch/3` and `entry_value/4`.
  """
  @spec entry_put(Nats.Client.t, binary, binary, Entry.value, Keyword.t) :: {:ok, revision} | kv_error
  defdelegate entry_put(client, bucket, key, value, opts \\ []), to: Jetstream.Entry, as: :put

  @doc """
  Create a KV entry or error.

  This will error if the key already exists.
  """
  @spec entry_create(Nats.Client.t, binary, binary, binary) :: {:ok, revision} | kv_error
  defdelegate entry_create(client, bucket, key, value), to: Jetstream.Entry, as: :create

  @doc """
  Fetch a KV entry.
  """
  @spec entry_fetch(Nats.Client.t, binary, binary) :: {:ok, Entry.t} | {:error, :not_found} | kv_error
  defdelegate entry_fetch(client, bucket, key), to: Jetstream.Entry, as: :fetch

  @doc """
  Get a KV entry's value.

  If the key does not exist, returns `default` (which defaults to `nil`).

  ## Example

      iex> entry_value(client, "foo", "bar")
      {:ok, nil}

      iex> entry_put(client, "foo", "bar", "baz")
      {:ok, _revision}

      iex> entry_value(client, "foo", "bar")
      {:ok, "baz"}

  """
  @spec entry_value(Nats.Client.t, binary, binary, term) :: {:ok, binary | term} | kv_error
  defdelegate entry_value(client, bucket, key, default \\ nil), to: Jetstream.Entry, as: :value

  @doc """
  Delete a KV key.

  Mark a key as deleted. Previous entries will still show up in history.

  ## Example

      iex> entry_put(client, "my-bucket", "my-key", "foobar")
      {:ok, 1}

      iex> entry_delete(client, "my-bucket", "my-key")
      {:ok, 2}

      iex> entry_fetch(client, "my-bucket", "my-key")
      {:error, :not_found}

      iex> {:ok, [e2, e1]} = entry_history(client, "my-bucket", "my-key")
      ...

      iex> e2
      %Jetstream.Entry{
        delete: true,
        value: nil,
        revision: 2,
        ...
      }

      iex> e1
      %Jetstream.Entry{
        delete: false,
        value: "foobar",
        revision: 1,
        ...
      }

  """
  @spec entry_delete(Nats.Client.t, binary, binary) :: {:ok, revision} | kv_error
  defdelegate entry_delete(client, bucket, key), to: Jetstream.Entry, as: :delete

  @doc """
  Purge a KV key.

  Like deleting a key, but history will only show the purge entry, all previous
  entries will be removed.

  ## Example

      iex> entry_put(client, "my-bucket", "my-key", "foobar")
      {:ok, 1}

      iex> entry_delete(client, "my-bucket", "my-key")
      {:ok, 2}

      iex> entry_fetch(client, "my-bucket", "my-key")
      {:error, :not_found}

      iex> {:ok, [e2]} = entry_history(client, "my-bucket", "my-key")
      ...

      iex> e2
      %Jetstream.Entry{
        operation: :purge,
        value: nil,
        revision: 2,
        ...
      }

  """
  @spec entry_purge(Nats.Client.t, binary, binary) :: {:ok, revision} | kv_error
  defdelegate entry_purge(client, bucket, key), to: Jetstream.Entry, as: :purge

  @doc """
  Get the history of a KV key.

  The returned list _should_ be sorted in descending order of revision (newest revision first).
  """
  @spec entry_history(Nats.Client.t, binary, binary) :: {:ok, [Entry.t]} | kv_error
  defdelegate entry_history(client, bucket, key), to: Jetstream.Entry, as: :history

end
