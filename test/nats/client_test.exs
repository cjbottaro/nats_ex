defmodule Nats.ClientTest do
  use ExUnit.Case
  alias Nats.Protocol.Msg

  setup_all do
    {:ok, client} = Nats.Client.start_link()
    {:ok, %{client: client}}
  end

  setup %{client: client} do
    {:ok, subs} = Nats.Client.subs(client)
    Enum.each(subs, &Nats.Client.unsub(client, &1.sid))
  end

  test "basic pub/sub", %{client: client} do
    :ok = Nats.Client.pub(client, "foo", payload: "bar")
    refute_receive _, 100

    {:ok, _sid} = Nats.Client.sub(client, "foo")
    :ok = Nats.Client.pub(client, "foo", payload: "bar")
    assert_receive %Msg{payload: "bar"}, 100
  end

  test "unsub", %{client: client} do
    {:ok, sid} = Nats.Client.sub(client, "foo")
    :ok = Nats.Client.pub(client, "foo", payload: "bar")
    assert_receive %Msg{payload: "bar"}, 100

    :ok = Nats.Client.unsub(client, sid)
    :ok = Nats.Client.pub(client, "foo", payload: "bar")
    refute_receive _, 100

    {:ok, subs} = Nats.Client.subs(client)
    assert Enum.count(subs) == 0
  end

  test "unsub with count", %{client: client} do
    {:ok, sid} = Nats.Client.sub(client, "foo")
    :ok = Nats.Client.unsub(client, sid, 2)

    :ok = Nats.Client.pub(client, "foo", payload: "bar")
    :ok = Nats.Client.pub(client, "foo", payload: "bar")
    :ok = Nats.Client.pub(client, "foo", payload: "bar")

    assert_receive %Msg{payload: "bar"}, 100
    assert_receive %Msg{payload: "bar"}, 100
    refute_receive _, 100
  end

  test "automatically unsubs if receiver goes down", %{client: client} do
    Task.async(fn ->
      {:ok, _sid} = Nats.Client.sub(client, "foo")
      {:ok, subs} = Nats.Client.subs(client)
      assert Enum.count(subs) == 1
    end)
    |> Task.await()

    {:ok, subs} = Nats.Client.subs(client)
    assert Enum.count(subs) == 0

    :ok = Nats.Client.pub(client, "foo", payload: "bar")
    refute_receive _, 100
  end

end
