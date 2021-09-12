defmodule Jetstream.Consumer.Creator do
  @moduledoc false
  use Task, restart: :transient

  def start_link(config) do
    Task.start_link(__MODULE__, :run, [config])
  end

  def run(config) do
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
