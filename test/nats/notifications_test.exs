defmodule Nats.NotificationsTest do
  use ExUnit.Case

  setup do
    :ok = :telemetry.attach_many(:notification_test,
      [
        [:nats, :client, :connect],
        [:nats, :client, :disconnect]
      ],
      &handler/4,
      self()
    )

    on_exit fn ->
      :ok = :telemetry.detach(:notification_test)
    end

    :ok
  end

  test "connect" do
    {:ok, conn} = Nats.Client.start_link()
    assert_receive {:connect, ^conn}
  end

  @tag capture_log: true
  test "reconnect" do
    {:ok, conn} = Nats.Client.start_link()

    socket = Nats.Client.debug(conn).socket
    send(conn, {:tcp_closed, socket})

    assert_receive {:connect, ^conn}
    assert_receive {:disconnect, ^conn}
    assert_receive {:connect, ^conn}
  end

  def handler([:nats, :client, what], %{}, meta, pid) do
    send(pid, {what, meta.client})
  end

end
