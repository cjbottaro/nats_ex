defmodule Nats.ResubTest do
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

  @tag capture_log: true
  test "resubs on disconnect" do
    {:ok, conn} = Nats.Client.start_link()
    assert_receive {:connect, ^conn}

    {:ok, _sid} = Nats.Client.sub(conn, "foo")

    # Simulate a disconnect.
    socket = Nats.Client.debug(conn).socket
    send(conn, {:tcp_closed, socket})
    assert_receive {:disconnect, ^conn}
    assert_receive {:connect, ^conn}

    Nats.Client.pub(conn, "foo", payload: "bar")

    assert_receive %Nats.Protocol.Msg{subject: "foo", payload: "bar"}
  end

  @tag capture_log: true
  test "does not resub on disconnect" do
    {:ok, conn} = Nats.Client.start_link(resub: false)
    assert_receive {:connect, ^conn}

    {:ok, _sid} = Nats.Client.sub(conn, "foo")

    # Simulate a disconnect.
    socket = Nats.Client.debug(conn).socket
    send(conn, {:tcp_closed, socket})
    assert_receive {:disconnect, ^conn}
    assert_receive {:connect, ^conn}

    Nats.Client.pub(conn, "foo", payload: "bar")

    refute_receive %Nats.Protocol.Msg{subject: "foo", payload: "bar"}
  end

  def handler([:nats, :client, what], %{}, meta, pid) do
    send(pid, {what, meta.client})
  end

end
