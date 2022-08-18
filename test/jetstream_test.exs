defmodule JetstreamTest do
  use ExUnit.Case

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

end
