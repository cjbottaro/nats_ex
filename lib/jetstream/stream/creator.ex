defmodule Jetstream.Stream.Creator do
  @moduledoc false
  use Task, restart: :transient

  def start_link(config) do
    Task.start_link(__MODULE__, :run, [config])
  end

  def run(config) do
    conn = config[:module]
    case get_stream(conn, config) do
      {:ok, %{payload: %{"error" => %{"code" => 404}}}} ->
        {:ok, %{payload: %{"created" => _}}} = Jetstream.stream_update(conn, config[:name], config)
      {:ok, %{payload: %{"created" => _}}} ->
        {:ok, %{payload: %{"type" => "io.nats.jetstream.api.v1.stream_update_response"}}} =
          Jetstream.stream_update(conn, config[:name], config)
    end
  end

  defp get_stream(conn, config) do
    Jetstream.stream_info(conn, config[:name])
  end
end
