defmodule Jetstream.Bucket do
  @moduledoc """
  Represents an bucket when using Jetstream as a KV store.
  """

  defstruct [:name, :history, :ttl, :max_size, :max_value_size, :num_replicas]

  @type t :: %__MODULE__{
    name: binary,
    history: 1..64,
    ttl: nil | pos_integer,
    max_size: nil | pos_integer,
    max_value_size: nil | pos_integer,
    num_replicas: 1..5
  }

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
  @update_type "io.nats.jetstream.api.v1.stream_update_response"
  @delete_type "io.nats.jetstream.api.v1.stream_delete_response"
  @info_type   "io.nats.jetstream.api.v1.stream_info_response"
  @list_type   "io.nats.jetstream.api.v1.stream_names_response"

  @spec create(Nats.Client.t, binary, Keyword.t) :: {:ok, t} | Jetstream.kv_error
  @doc false

  def create(client, name, opts \\ []) do
    with {:ok, opts} <- NimbleOptions.validate(opts, @opts_schema) do
      config = to_stream_config(name, opts)
      resp = Jetstream.stream_create(client, "KV_#{name}", config)

      case resp do
        {:ok, %{payload: %{"type" => @create_type, "config" => config}}} -> {:ok, config_to_bucket(config)}
        {:ok, msg} -> {:error, msg}
        error -> error
      end
    end
  end

  @doc false
  def update(client, name, opts) do
    with {:ok, opts} <- NimbleOptions.validate(opts, @opts_schema) do
      config = to_stream_config(name, opts)

      case Jetstream.stream_update(client, "KV_#{name}", config) do
        {:ok, %{payload: %{"type" => @update_type, "config" => config}}} -> {:ok, config_to_bucket(config)}
        {:ok, msg} -> {:error, msg}
        error -> error
      end
    end
  end

  @spec delete(Nats.Client.t, binary) :: :ok | Jetstream.kv_error
  @doc false

  def delete(client, name) do
    case Jetstream.stream_delete(client, "KV_#{name}") do
      {:ok, %{payload: %{"type" => @delete_type, "success" => true}}} -> :ok
      {:ok, %{payload: %{"type" => @delete_type, "error" => %{"code" => 404}}}} -> :ok
      {:ok, msg} -> {:error, msg}
      error -> error
    end
  end

  @spec info(Nats.Client.t, binary) :: {:ok, t} | Jetstream.kv_error
  @doc false

  def info(client, name) do
    case Jetstream.stream_info(client, "KV_#{name}") do
      {:ok, %{payload: %{"type" => @info_type, "config" => config}}} -> {:ok, config_to_bucket(config)}
      {:ok, msg} -> {:error, msg}
      error -> error
    end
  end

  @spec list(Nats.Client.t) :: {:ok, [binary]} | Jetstream.kv_error
  @doc false

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

  defp to_stream_config(name, opts) do
    max_age = opts[:ttl] && (opts[:ttl] * 1_000_000)

    [
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
    ]
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
