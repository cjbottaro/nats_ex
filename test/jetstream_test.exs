defmodule Js do
  use Jetstream, nats: JetstreamClient
end

defmodule JetstreamTest do
  use ExUnit.Case

  setup_all do
    {:ok, client} = Nats.Client.start_link(name: JetstreamClient)
    {:ok, %{client: client}}
  end

  setup do
    on_exit(fn ->
      Js.stream_delete("foo")
    end)

    :ok
  end

  test "parse_ack" do
    info = Jetstream.parse_ack("$JS.ACK.change-stream.ChangeLogger/Consumer.1.104.2.1660858232357389126.0")
    assert info == %{
      domain: nil,
      account: nil,
      stream: "change-stream",
      consumer: "ChangeLogger/Consumer",
      delivered: 1,
      stream_seq: 104,
      consumer_seq: 2,
      timestamp: ~U[2022-08-18 21:30:32.357389Z],
      pending: 0
    }
  end

  test "stream api" do
    {:ok, resp} = Js.stream_list()
    assert resp.payload["streams"] == nil

    {:error, resp} = Js.stream_info("foo")
    assert resp.payload["error"]["code"] == 404

    {:ok, _} = Js.stream_create("foo")

    {:ok, resp} = Js.stream_list()
    assert resp.payload["streams"] == ["foo"]

    {:ok, resp} = Js.stream_info("foo")
    assert resp.payload["config"]["subjects"] == ["foo"]

    {:ok, _} = Js.stream_update("foo", subjects: ["foo", "bar"])

    {:ok, resp} = Js.stream_info("foo")
    assert resp.payload["config"]["subjects"] == ["foo", "bar"]

    {:ok, _} = Js.stream_delete("foo")

    {:ok, resp} = Js.stream_list()
    assert resp.payload["streams"] == nil

    {:error, resp} = Js.stream_info("foo")
    assert resp.payload["error"]["code"] == 404
  end

  test "consumer api" do
    {:ok, _} = Js.stream_create("foo")

    {:ok, resp} = Js.consumer_list("foo")
    assert resp.payload["consumers"] == []

    {:error, resp} = Js.consumer_info("foo", "bar")
    assert resp.payload["error"]["code"] == 404

    {:ok, _} = Js.consumer_create("foo", "bar", filter_subjects: ["foo"])

    {:ok, resp} = Js.consumer_list("foo")
    assert resp.payload["consumers"] != []

    {:ok, resp} = Js.consumer_info("foo", "bar")
    assert resp.payload["config"]["filter_subjects"] == ["foo"]

    {:ok, _} = Js.consumer_update("foo", "bar", filter_subjects: ["bar", "foo"])

    {:ok, resp} = Js.consumer_info("foo", "bar")
    assert resp.payload["config"]["filter_subjects"] == ["bar", "foo"]

    {:ok, _} = Js.consumer_delete("foo", "bar")

    {:ok, resp} = Js.consumer_list("foo")
    assert resp.payload["consumers"] == []

    {:error, resp} = Js.consumer_info("foo", "bar")
    assert resp.payload["error"]["code"] == 404
  end

end
