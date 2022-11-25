defmodule Nats.Kv.Bucket do
  defstruct [:name, :history, :ttl, :max_size, :max_value_size, :num_replicas]

  @type t :: %__MODULE__{}

  @opts_schema NimbleOptions.new!(
    history: [
      type: :integer,
      default: 1,
      doc: "Amount of history to keep for each key"
    ],
    ttl: [
      type: :integer,
      doc: "TTL for keys in milliseconds"
    ],
    max_size: [
      type: :integer,
      doc: "Max size of the bucket in bytes"
    ],
    max_value_size: [
      type: :integer,
      doc: "Max key value size in bytes"
    ],
    num_replicas: [
      type: :integer,
      default: 1,
      doc: "Number of replicas"
    ]
  )

  @create_type "io.nats.jetstream.api.v1.stream_create_response"
  @delete_type "io.nats.jetstream.api.v1.stream_delete_response"
  @info_type   "io.nats.jetstream.api.v1.stream_info_response"
  @list_type   "io.nats.jetstream.api.v1.stream_names_response"

  @spec create(Nats.Client.t, binary, Keyword.t) :: {:ok, t} | {:error, Nats.Msg.t} | {:error, :term}

  def create(client, name, opts \\ []) do
    with {:ok, opts} <- NimbleOptions.validate(opts, @opts_schema) do
      max_age = opts[:max_age] && (opts[:max_age] * 1_000_000)
      resp = Jetstream.stream_create(client, "KV_#{name}",
        discard: :new,
        allow_rollup_hdrs: true,
        deny_delete: true,
        allow_direct: true,
        subjects: ["$KV.#{name}.>"],
        max_msgs_per_subject: opts[:history],
        max_age: max_age,
        duplicate_window: max_age,
        max_bytes: opts[:max_size],
        max_msg_size: opts[:max_value_size],
        num_replicas: opts[:num_replicas]
      )

      case resp do
        {:ok, %{payload: %{"type" => @create_type, "config" => config}}} -> {:ok, config_to_bucket(config)}
        {:ok, msg} -> {:error, msg}
        error -> error
      end
    end
  end

  @spec delete(Nats.Client.t, binary) :: :ok | {:error, Nats.Msg.t} | {:error, term}

  def delete(client, name) do
    case Jetstream.stream_delete(client, "KV_#{name}") do
      {:ok, %{payload: %{"type" => @delete_type, "success" => true}}} -> :ok
      {:ok, %{payload: %{"type" => @delete_type, "error" => %{"code" => 404}}}} -> :ok
      {:ok, msg} -> {:error, msg}
      error -> error
    end
  end

  def info(client, name) do
    case Jetstream.stream_info(client, "KV_#{name}") do
      {:ok, %{payload: %{"type" => @info_type, "config" => config}}} -> {:ok, config_to_bucket(config)}
      {:ok, msg} -> {:error, msg}
      error -> error
    end
  end

  @spec list(Nats.Client.t) :: {:ok, [binary]} | {:error, Nats.Msg.t} | {:error, term}

  def list(client) do
    case Jetstream.stream_list(client) do
      {:ok, %{payload: %{"type" => @list_type, "streams" => streams}}} ->
        buckets = Enum.reduce(streams, [], fn
          <<"KV_", name::binary>>, acc -> [name | acc]
          _stream, acc -> acc
        end)
        {:ok, buckets}

      {:ok, msg} -> {:error, msg}
      error -> error
    end
  end

  defp config_to_bucket(config) do
    name = String.replace_prefix(config["name"], "KV_", "")

    ttl = case config["max_age"] do
      0 -> nil
      max_age -> max_age / 1_000_000
    end

    max_size = case config["max_bytes"] do
      -1 -> nil
      size -> size
    end

    max_value_size = case config["max_msg_size"] do
      -1 -> nil
      size -> size
    end

    %__MODULE__{
      name: name,
      ttl: ttl,
      max_size: max_size,
      max_value_size: max_value_size,
      history: config["max_msgs_per_subject"],
      num_replicas: config["num_replicas"]
    }
  end
end
