defmodule Jetstream.Entry do
  @moduledoc """
  Represents an entry when using Jetstream as a KV store.
  """

  defstruct [:bucket, :key, :value, :created_at, :revision, :operation]

  @type t :: %__MODULE__{
    bucket: binary,
    key: binary,
    value: value,
    created_at: DateTime.t,
    revision: pos_integer,
    operation: :put | :delete | :purge
  }

  @type value :: binary | map

  @spec put(Nats.Client.t, binary, binary, value) :: {:ok, integer} | Jetstream.kv_error
  @spec put(Nats.Client.t, binary, binary, value, Keyword.t) :: {:ok, integer} | Jetstream.kv_error
  @doc false

  def put(client, bucket, key, value, opts \\ []) do
    headers = Keyword.get(opts, :headers, [])

    {value, headers} = case value do
      b when is_binary(b) -> {b, headers}
      m when is_map(m) -> {Jason.encode!(value), [{"Content-Type", "application/json"} | headers]}
    end

    headers = case opts[:revision] do
      nil -> headers
      n -> [{"Nats-Expected-Last-Subject-Sequence", n} | headers]
    end

    headers = case opts[:operation] do
      :delete -> [
        {"KV-Operation", "DEL"}
        | headers
      ]

      :purge ->[
        {"KV-Operation", "PURGE"},
        {"Nats-Rollup", "sub"}
        | headers
      ]

      _operation -> headers
    end

    case Jetstream.publish(client, "$KV.#{bucket}.#{key}", value, headers: headers) do
      {:ok, %{"seq" => seq}} -> {:ok, seq}
      {:ok, msg} -> {:error, msg}
      error -> error
    end
  end

  @spec create(Nats.Client.t, binary, binary, binary) :: {:ok, pos_integer} | Jetstream.kv_error
  @doc false

  def create(client, bucket, key, value) do
    put(client, bucket, key, value, revision: 0)
  end

  @spec fetch(Nats.Client.t, binary, binary) :: {:ok, t} | {:error, :not_found} | Jetstream.kv_error
  @doc false

  def fetch(client, bucket, key) do
    case Nats.Client.request(client, "$JS.API.DIRECT.GET.KV_#{bucket}.$KV.#{bucket}.#{key}") do
      {:ok, %{headers: ["NATS/1.0 404 Message Not Found"]}} -> {:error, :not_found}

      {:ok, msg} ->
        entry = %__MODULE__{
          bucket: bucket,
          key: key,
          value: msg.payload,
          operation: :put
        }

        entry = Enum.reduce(msg.headers, entry, fn

          <<"Nats-Sequence: ", n::binary>>, entry ->
            revision = String.to_integer(n)
            %{entry | revision: revision}

          <<"Nats-Time-Stamp: ", dt::binary>>, entry ->
            {:ok, created_at, 0} = DateTime.from_iso8601(dt)
            %{entry | created_at: created_at}

          "KV-Operation: DEL", entry ->
            %{entry | operation: :delete}

          "KV-Operation: PURGE", entry ->
            %{entry | operation: :purge}

          "Content-Type: application/json", entry ->
            %{entry | value: Jason.decode!(entry.value)}

          _header, entry -> entry

        end)

        if entry.operation == :put do
          {:ok, entry}
        else
          {:error, :not_found}
        end

      error -> error
    end
  end

  @spec fetch_value(Nats.Client.t, binary, binary) :: {:ok, binary} | Jetstream.kv_error
  @doc false

  def fetch_value(client, bucket, key) do
    case fetch(client, bucket, key) do
      {:ok, entry} -> {:ok, entry.value}
      error -> error
    end
  end

  @spec value(Nats.Client.t, binary, binary) :: {:ok, binary} | Jetstream.kv_error
  @spec value(Nats.Client.t, binary, binary, term) :: {:ok, term} | Jetstream.kv_error
  @doc false

  def value(client, bucket, key, default \\ nil) do
    case fetch(client, bucket, key) do
      {:error, :not_found} -> {:ok, default}
      {:ok, entry} -> {:ok, entry.value}
    end
  end

  def delete(client, bucket, key) do
    put(client, bucket, key, "", operation: :delete)
  end

  def purge(client, bucket, key) do
    put(client, bucket, key, "", operation: :purge)
  end

  @spec history(Nats.Client.t, binary, binary) :: {:ok, [t]} | Jetstream.kv_error
  @doc false

  def history(client, bucket, key) do
    stream = "KV_#{bucket}"
    tmp_subject = Nats.Utils.new_uid()
    tmp_consumer = Nats.Utils.new_uid()

    with {:ok, sid} <- Nats.Client.sub(client, tmp_subject) do
      try do
        get_history(client, bucket, key, stream, tmp_subject, tmp_consumer)
      after
        :ok = Nats.Client.unsub(client, sid)
      end
    end
  end

  defp get_history(client, bucket, key, stream, tmp_subject, tmp_consumer) do
    resp = Jetstream.consumer_create(client, stream, tmp_consumer,
      durable: false,
      ack_policy: :none,
      deliver_policy: :all,
      deliver_subject: tmp_subject,
      filter_subject: "$KV.#{bucket}.#{key}"
    )

    with {:ok, _msg} <- resp do
      entries = Stream.repeatedly(fn ->
        receive do
          %Nats.Protocol.Msg{} = m -> m
        end
      end)
      |> Enum.reduce_while([], fn msg, entries ->
        %{
          pending: pending,
          stream_seq: seq,
          timestamp: timestamp
        } = Jetstream.parse_ack(msg)

        [_, _, key] = String.split(msg.subject, ".")

        operation = Enum.find_value(msg.headers, :put, fn
          "KV-Operation: DEL" -> :delete
          "KV-Operation: PURGE" -> :purge
          _header -> false
        end)

        entry = %__MODULE__{
          bucket: bucket,
          key: key,
          value: msg.payload,
          created_at: timestamp,
          revision: seq,
          operation: operation
        }

        entries = [entry | entries]

        if pending == 0 do
          {:halt, entries}
        else
          {:cont, entries}
        end
      end)

      # This is best effort since the Nats server will clean up ephemeral consumers.
      Jetstream.consumer_delete(client, stream, tmp_consumer)

      {:ok, entries}
    end
  end
end
