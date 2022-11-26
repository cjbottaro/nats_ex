defmodule Nats.KvTest do
  use ExUnit.Case

  setup_all do
    {:ok, client} = Nats.Client.start_link()
    {:ok, %{client: client}}
  end

  setup %{client: client} do
    {:ok, _} = Jetstream.bucket_create(client, "kv-test", history: 3)
    on_exit(fn -> Jetstream.bucket_delete(client, "kv-test") end)

    {:ok, subs} = Nats.Client.subs(client)
    Enum.each(subs, &Nats.Client.unsub(client, &1.sid))

    :ok
  end

  test "ttl", %{client: client} do
    {:ok, _} = Jetstream.bucket_update(client, "kv-test", ttl: 100)

    Jetstream.entry_put(client, "kv-test", "k1", "v1")
    {:ok, "v1"} = Jetstream.entry_value(client, "kv-test", "k1")

    Process.sleep(100)

    {:error, :not_found} = Jetstream.entry_fetch(client, "kv-test", "k1")
  end

  test "delete", %{client: client} do
    Jetstream.entry_put(client, "kv-test", "k1", "v1")
    Jetstream.entry_delete(client, "kv-test", "k1")

    {:error, :not_found} = Jetstream.entry_fetch(client, "kv-test", "k1")

    {:ok, [e2, e1]} = Jetstream.entry_history(client, "kv-test", "k1")

    assert e2.operation == :delete
    assert e2.revision == 2

    assert e1.operation == :put
    assert e1.revision == 1
    assert e1.value == "v1"
  end

  test "purge", %{client: client} do
    Jetstream.entry_put(client, "kv-test", "k1", "v1")
    Jetstream.entry_purge(client, "kv-test", "k1")

    {:error, :not_found} = Jetstream.entry_fetch(client, "kv-test", "k1")

    {:ok, [e2]} = Jetstream.entry_history(client, "kv-test", "k1")

    assert e2.operation == :purge
    assert e2.revision == 2
  end

  test "history", %{client: client} do
    Jetstream.entry_put(client, "kv-test", "k1", "v1")
    Jetstream.entry_put(client, "kv-test", "k1", "v2")
    Jetstream.entry_put(client, "kv-test", "k1", "v3")
    Jetstream.entry_put(client, "kv-test", "k1", "v4")

    resp = Jetstream.entry_value(client, "kv-test", "k1")
    assert resp == {:ok, "v4"}

    {:ok, [e3, e2, e1]} = Jetstream.entry_history(client, "kv-test", "k1")

    # Might be testing implementation here... relying on stream sequence numbers
    # starting at 1 for newly created streams.

    assert e1.key == "k1"
    assert e1.bucket == "kv-test"
    assert e1.value == "v2"
    assert e1.revision == 2

    assert e2.key == "k1"
    assert e2.bucket == "kv-test"
    assert e2.value == "v3"
    assert e2.revision == 3

    assert e3.key == "k1"
    assert e3.bucket == "kv-test"
    assert e3.value == "v4"
    assert e3.revision == 4

    {:ok, e} = Jetstream.entry_fetch(client, "kv-test", "k1")

    assert e.bucket == e3.bucket
    assert e.key == e3.key
    assert e.value == e3.value
    assert e.revision == e3.revision
    assert e.created_at == e3.created_at
  end

end
