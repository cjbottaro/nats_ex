defmodule Nats.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    :ok = Nats.Rpc.Server.Logger.init()

    children = [
      # Starts a worker by calling: Nats.Worker.start_link(arg)
      # {Nats.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Nats.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
