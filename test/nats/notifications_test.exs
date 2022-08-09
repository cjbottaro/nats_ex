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

  # @tag capture_log: true
  # test "disconnect on monitor" do
  #   {:ok, conn} = Nats.Client.start_link()
  #   :ok = Nats.Client.monitor(conn)

  #   socket = Nats.Client.debug(conn).socket
  #   send(conn, {:tcp_closed, socket})

  #   assert_receive {:nats_client_disconnect, ^conn}
  # end

  # test "cleans up monitors (start_link)" do
  #   pid = spawn(fn -> Process.sleep(10_000) end)
  #   {:ok, conn} = Nats.Client.start_link(monitor: pid)
  #   Process.exit(pid, :kill)

  #   monitors = Stream.repeatedly(fn -> Nats.Client.debug(conn).notify end)
  #   |> Stream.with_index()
  #   |> Enum.reduce_while(nil, fn
  #     {_monitors, 10}, acc -> {:halt, acc}
  #     {[], _}, _acc -> {:halt, []}
  #     {monitors, _}, _acc ->
  #       Process.sleep(10)
  #       {:cont, monitors}
  #   end)

  #   assert [] == monitors
  # end

  # test "cleans up monitors (monitor)" do
  #   {:ok, conn} = Nats.Client.start_link()

  #   Task.async(fn -> Nats.Client.monitor(conn) end)
  #   |> Task.await()

  #   assert [] == Nats.Client.debug(conn).notify
  # end

end
