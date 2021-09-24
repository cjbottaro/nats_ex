defmodule Jetstream.Consumer.Creator do
  @moduledoc false
  use Task, restart: :transient

  def start_link(config) do
    Task.start_link(__MODULE__, :run, [config])
  end

  def run(config) do
    config = Keyword.update!(config, :ack_wait, fn
      {value, unit} -> Nats.Utils.to_nanosecond(value, unit)
      value -> Nats.Utils.to_nanosecond(value, :millisecond)
    end)

    {:ok, conn} = Nats.Client.start_link(config)
    with {:ok, %{payload: %{"error" => %{"code" => 404}}}} <- get_consumer(conn, config) do
      {:ok, %{payload: %{"created" => _}}} = Jetstream.create_consumer(conn, config[:stream], config[:consumer], config)
    end
    :ok = GenServer.stop(conn)
  end

  defp get_consumer(conn, config) do
    Jetstream.consumer_info(conn, config[:stream], config[:consumer])
  end
end
