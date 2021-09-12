defmodule Nats.Rpc.Server.Logger do
  @moduledoc false
  require Logger

  def init do
    config = Nats.Rpc.Server.config() |> Map.new()

    :telemetry.attach_many(
      inspect(__MODULE__),
      [
        [:nats, :rpc, :server, :ok],
        [:nats, :rpc, :server, :error],
      ],
      &__MODULE__.log/4,
      config
    )
  end

  def log([:nats, :rpc, :server, :ok], time, meta, config) when config.log == true do
    time = Nats.Utils.format_usec(time.usec)
    Logger.info("🥂 #{meta.subject} call in #{time}")
  end

  def log([:nats, :rpc, :server, :error], time, meta, config) when config.log == true do
    time = Nats.Utils.format_usec(time.usec)
    Logger.info("💥 #{meta.subject} error in #{time}")
  end

end
