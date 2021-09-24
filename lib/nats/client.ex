defmodule Nats.Client do
  @moduledoc """
  Basic client connection to a Nats server.

  ```
  {:ok, conn} = Nats.Client.start_link()
  {:ok, _sid} = Nats.Client.sub(conn, "some.subject")
  :ok = Nats.Client.pub(conn, "some.subject", payload: "foobar")
  "foobar" = receive do
    %Nats.Protocol.Msg{} = msg -> msg.payload
  end
  ```
  """

  @type t :: pid
  @type sid :: binary

  use Connection
  import Nats.Utils
  alias Nats.Protocol
  require Logger

  @doc false
  def debug(conn) do
    Connection.call(conn, :debug)
  end

  @doc """
  Subscribe to a subject.

  The process that calls this function will be the reciever of pub messages. This
  allows a single Nats connection to be shared among many processes. Processes
  are monitored and subscriptions automatically cancelled when they go down.

  ## Options
    * `:queue_group` - Use a queue group for the subscription. Default `nil`.

  ## Examples

      sub(client, "foo.bar")
      sub(client, "foo.bar", queue_group: "foo")

  """
  @spec sub(t, binary, Keyword.t) :: {:ok, sid} | {:error, binary}
  def sub(conn, subject, opts \\ []) do
    sub = Protocol.Sub.new(subject, nil, opts[:queue_group])
    Connection.call(conn, {:sub, sub})
  end

  @doc """
  Publish to a subject.

  `opts` can take a `payload: binary` and also `reply_to: subject`.

  ```
  :ok = pub(conn, "foo.bar")
  :ok = pub(conn, "foo.bar", payload: "hello world!")
  :ok = pub(conn, "foo.bar", payload: "echo", reply_to: "some.subject")
  ```
  """
  @spec pub(t, binary, Keyword.t) :: :ok | {:error, binary}
  def pub(conn, subject, opts \\ []) do
    pub = Protocol.Pub.new(subject, opts)
    Connection.call(conn, {:pub, pub})
  end

  @doc """
  New style Nats request/response.

  Every client connection immediately makes a subscription to
  `_INBOX.<unique-inbox-id>`. When a `request/3` call is made, the `:reply_to`
  is automatically set to `_INBOX.<unique-inbox-id>.<unique-request-id>`. The
  end result is you get a synchronous request/response cycle.

  `opts` can take a `timeout: millisecond` option which defaults to `5000`.

  Note that the client connection keeps track of all requests and properly
  handles responses that come in after the timeout, thus no process mailboxes
  will fill up with unhandled/late messages.

  ```
  {:ok, %{payload: "hello"}} = request(conn, "echo-server", payload: "hello")
  ```
  """
  @spec request(t, binary, Keyword.t) :: {:ok, Nats.Protocol.Msg.t} | {:error, :timeout} | {:error, binary}
  def request(conn, subject, opts \\ []) do
    {timeout, opts} = Keyword.pop(opts, :timeout, 5000)

    if not is_integer(timeout) or timeout <= 0 do
      raise ArgumentError, ":timeout must be a positive integer in milliseconds"
    end

    pub = Protocol.Pub.new(subject, opts)
    Connection.call(conn, {:request, pub, timeout}, :infinity)
  end

  @doc """
  Unsubscribe from a topic.

  If `count` is given, then the subscription will be automatically cancelled
  after that many messages have been recieved on that subscription.

  ```
  :ok = unsub(conn, "123")    # Unsub immediately.
  :ok = unsub(conn, "123", 3) # Unsub after receiving 3 messages.
  ```
  """
  @spec unsub(t, binary | integer) :: :ok | {:error, term}
  @spec unsub(t, binary | integer, integer | nil) :: :ok | {:error, term}
  def unsub(conn, sid, count \\ nil)

  def unsub(conn, sid, count) when is_integer(sid) do
    unsub(conn, to_string(sid), count)
  end

  def unsub(conn, sid, count) when is_binary(sid) do
    Connection.call(conn, {:unsub, sid, count})
  end

  @doc """
  Send a raw string to Nats server.

  ```
  :ok = send_raw(conn, "PONG\\r\\n")
  ```
  """
  @spec send_raw(t, binary) :: :ok | {:error, binary}
  def send_raw(conn, message) do
    Connection.call(conn, {:send_raw, message})
  end

  @doc """
  Get info about the connection.
  """
  @spec info(t) :: {:ok, map} | {:error, binary}
  def info(conn) do
    Connection.call(conn, :info)
  end

  @doc """
  Get all subscriptions for the connection.
  """
  @spec subs(t) :: {:ok, [Nats.Protocol.Sub.t()]} | {:error, binary}
  def subs(conn) do
    Connection.call(conn, :subs)
  end

  @defaults [
    host: "localhost",
    port: 4222,
    headers: true,
    subs: [],
    notify: [],
  ]

  @doc """
  Default options used for the first argument to `start_link/2`

  ```
  #{inspect @defaults, pretty: true, width: 0}
  ```

  They can be overwritten (merged) via `Config`...
  ```
  import Config
  config :nats, #{inspect __MODULE__}, host: "foo.bar.com"

  #{inspect __MODULE__}.defaults()
  #{inspect Keyword.merge(@defaults, host: "foo.bar.com"), pretty: true, width: 0}
  ```
  """
  def defaults do
    otp_app = Application.get_application(__MODULE__)

    Keyword.merge(
      @defaults,
      Application.get_env(otp_app, __MODULE__, [])
    )
  end

  @doc """
  Start a client connection.

  `opts` is merged with `defaults/0`.

  `subs` is a list of subscriptions to automatically subscribe to when the
  client connection starts up.

  ## Examples

  ```
  # Connect to specific host and port.
  {:ok, conn} = Nats.Client.start_link(host: "foo.bar.com", port: 1234)

  # Automatically subscribe
  {:ok, conn} = Nats.Client.start_link(subs: ["foo", "bar.*"])
  ```

  """
  @spec start_link(Keyword.t) :: {:ok, t} | {:error, reason :: binary}
  def start_link(opts \\ [], gen_opts \\ []) do
    {opts, other_opts} = default_opts(opts, defaults())
    gen_opts = Keyword.merge(other_opts, gen_opts)

    Connection.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Close the client connection.
  """
  def stop(conn) do
    GenServer.stop(conn)
  end

  defmacro __using__(use_config \\ []) do
    quote location: :keep do

      def debug do
        Nats.Client.debug(__MODULE__)
      end

      def info do
        Nats.Client.info(__MODULE__)
      end

      def pub(subject, opts \\ []) do
        Nats.Client.pub(__MODULE__, subject, opts)
      end

      def sub(subject, opts \\ []) do
        Nats.Client.sub(__MODULE__, subject, opts)
      end

      def unsub(sid, count \\ nil) do
        Nats.Client.unsub(__MODULE__, sid, count)
      end

      def subs do
        Nats.Client.subs(__MODULE__)
      end

      def request(subject, opts \\ []) do
        Nats.Client.request(__MODULE__, subject, opts)
      end

      def send_raw(message) do
        Nats.Client.send_raw(__MODULE__, message)
      end

      def child_spec(config \\ []) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [config]}
        }
      end

      def start_link(config \\ []) do
        mix_config = Application.get_application(__MODULE__)
        |> Application.get_env(__MODULE__, [])

        config = Nats.Client.defaults()
        |> Keyword.merge(mix_config)
        |> Keyword.merge(unquote(use_config))
        |> Keyword.merge(config)
        |> Keyword.put(:name, __MODULE__)

        Nats.Client.start_link(config)
      end

      def stop do
        Nats.Client.stop(__MODULE__)
      end

    end
  end

  @doc false
  def init(opts) do
    opts = Keyword.update!(opts, :notify, &List.wrap/1)

    state = %{
      opts: opts,
      socket: nil,
      closed_at: nil,
      next_sid: nil,
      subs: %{},
      request_inbox: nil,
      requests: %{},
      info: nil,
      max_payload: nil,
    }

    # Setup our request inbox.
    request_inbox = "_INBOX." <> new_uid()

    # Setup any configured subs (plus our request inbox).
    subs = ["#{request_inbox}.*" | opts[:subs]]
    |> Enum.with_index(1)
    |> Enum.map(fn
      {{subject, queue_group}, sid} -> Protocol.Sub.new(subject, sid, queue_group)
      {subject, sid} -> Protocol.Sub.new(subject, sid)
    end)
    |> Map.new(fn sub -> {sub.sid, sub} end)

    state = %{state |
      request_inbox: request_inbox,
      subs: subs,
      next_sid: 1 + map_size(subs),
    }

    case connect_with_handshake(state) do
      {:ok, socket, info} ->
        notify(:connect, state)
        {:ok, %{state | socket: socket, info: info, max_payload: info["max_payload"]}}

      {:error, reason} ->
        host = state.opts[:host]
        port = state.opts[:port]
        Logger.error("Connection failed to #{host}:#{port} -- #{reason}")
        {:backoff, 1000, %{state | closed_at: DateTime.utc_now()}}
    end
  end

  def connect(:backoff, state) do
    host = state.opts[:host]
    port = state.opts[:port]

    case connect_with_handshake(state) do
      {:ok, socket, info} ->
        notify(:connect, state)
        time = DateTime.diff(DateTime.utc_now(), state.closed_at)
        Logger.info("Connection (re)established to #{host}:#{port} after #{time}s")
        {:ok, %{state | socket: socket, info: info, max_payload: info["max_payload"], closed_at: nil}}

      {:error, reason} ->
        Logger.error("Connection failed to #{host}:#{port} -- #{reason}")
        {:backoff, 1000, state}
    end
  end

  def disconnect(info, state) do
    # Sure why not.
    :ok = :gen_tcp.close(state.socket)

    # Log an error.
    host = state.opts[:host]
    port = state.opts[:port]
    Logger.error("Connection closed to #{host}:#{port} -- #{info}")

    # Notify anyone interested.
    notify(:disconnect, state)

    # Update our state.
    state = %{state | socket: nil, closed_at: DateTime.utc_now()}

    # Try to reconnect immediately.
    {:connect, :backoff, state}
  end

  def handle_call(_, _, state) when state.socket == nil do
    {:reply, {:error, "not connected"}, state}
  end

  def handle_call(:debug, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:send_raw, message}, _from, state) do
    {:reply, send_message(message, state), state}
  end

  def handle_call({:request, pub, _timeout}, _from, state) when pub.bytes > state.max_payload do
    {:reply, {:error, "Maximum Payload Violation"}, state}
  end

  def handle_call({:request, pub, timeout}, from, state) do
    %{request_inbox: request_inbox, requests: requests} = state

    reply_to = "#{request_inbox}.#{new_uid()}"
    pub = %{pub | reply_to: reply_to}

    case send_message(pub, state) do
      :ok ->
        timer = Process.send_after(self(), {:cancel_request, reply_to}, timeout)
        requests = Map.put(requests, reply_to, {from, timer})
        state = %{state | requests: requests}
        {:noreply, state}

      error -> {:reply, error, state}
    end
  end

  def handle_call({:pub, pub}, _from, state) when pub.bytes > state.max_payload do
    {:reply, {:error, "Maximum Payload Violation"}, state}
  end

  def handle_call({:pub, pub}, _from, state) do
    {:reply, send_message(pub, state), state}
  end

  def handle_call({:sub, sub}, {pid, _ref}, state) do
    {sub, state} = set_sid(sub, state)

    case send_message(sub, state) do
      :ok ->
        sub = %{sub | receiver: pid, monitor: Process.monitor(pid)}
        subs = Map.put(state.subs, sub.sid, sub)
        state = %{state | subs: subs}
        {:reply, {:ok, sub.sid}, state}
      error -> {:reply, error, state}
    end
  end

  # TODO is there anyway to get rid our internally tracked sub?
  def handle_call({:unsub, sid, count}, _from, state) when count != nil do
    resp = Protocol.unsub(sid, count)
    |> send_message(state)

    {:reply, resp, state}
  end

  def handle_call({:unsub, sid, count}, _from, state) do
    {sub, subs} = Map.pop(state.subs, sid)

    if sub do
      Process.demonitor(sub.monitor, [:flush])
    else
      Logger.warn("sid (#{sid}) not found")
    end

    resp = Protocol.unsub(sid, count)
    |> send_message(state)

    {:reply, resp, %{state | subs: subs}}
  end

  def handle_call(:info, _from, state) do
    {:reply, {:ok, state.info}, state}
  end

  def handle_call(:subs, _from, state) do
    subs = Map.delete(state.subs, "1") |> Map.values()
    {:reply, {:ok, subs}, state}
  end

  def handle_info({:tcp_closed, socket}, state) when state.socket == socket do
    {:disconnect, :tcp_closed, state}
  end

  def handle_info({:tcp, _port, line}, state) do
    %{socket: socket} = state

    if line != "PING\r\n" do
      Logger.debug "<<- #{inspect line}"
    end

    {command, payload} = case String.split(line, " ", parts: 2) do
      [command, payload] -> {command, chomp(payload)}
      [command] -> {chomp(command), nil}
    end

    {:ok, state} = Nats.Protocol.recv(socket, command, payload)
    |> handle_message(state)

    :inet.setopts(socket, active: :once)

    {:noreply, state}
  end

  def handle_info({:cancel_request, request_key}, state) do
    %{requests: requests} = state

    {request, requests} = Map.pop(requests, request_key)

    if request do
      {from, _timer} = request
      Connection.reply(from, {:error, :timeout})
    end

    {:noreply, %{state | requests: requests}}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    subs = Enum.reduce(state.subs, %{}, fn {sid, sub}, acc ->
      if sub.receiver == pid do
        Protocol.unsub(sid, nil) |> send_message(state)
        acc
      else
        Map.put(acc, sid, sub)
      end
    end)

    {:noreply, %{state | subs: subs}}
  end

  @doc false
  def handle_message(%Protocol.Ping{}, state) do
    Protocol.Pong.new()
    |> send_message(state)

    {:ok, state}
  end

  def handle_message(%Protocol.Msg{} = msg, state) do
    if String.starts_with?(msg.subject, state.request_inbox) do
      reply_to_request(msg, state)
    else
      case state.subs[msg.sid] do
        %{receiver: {pid, _ref}} -> send(pid, msg)
        %{receiver: pid} when is_pid(pid) -> send(pid, msg)
        nil -> Logger.warn("Unexpected #{inspect msg}")
      end
      {:ok, state}
    end
  end

  defp notify(what, state) do
    msg = case what do
      :connect -> {:nats_client_connect, self()}
      :disconnect -> {:nats_client_disconnect, self()}
    end

    Enum.each(state.opts[:notify], &send(&1, msg))
  end

  defp resubscribe(socket, subs) do
    Enum.each(subs, fn {_sid, sub} ->
      :ok = send_message(sub, socket)
    end)

    :ok
  end

  defp reply_to_request(msg, state) do
    %{requests: requests} = state

    case Map.pop(requests, msg.subject) do

      {nil, _requests} ->
        Logger.warn("Discarding request response due to timeout: #{inspect msg}")
        {:ok, state}

      {request, requests} ->
        {from, timer} = request
        case Process.cancel_timer(timer) do
          false -> Logger.warn("Discarding request response due to timeout: #{inspect msg}")
          _time_left -> :ok = Connection.reply(from, {:ok, msg})
        end
        {:ok, %{state | requests: requests}}

    end
  end

  defp send_message(message, state) when is_map(state) do
    send_message(message, state.socket)
  end

  defp send_message(message, socket) do
    iodata = message.__struct__.to_iodata(message)
    case message do
      %Protocol.Pong{} -> nil
      _ -> Logger.debug("->> #{inspect IO.iodata_to_binary(iodata)}")
    end
    :gen_tcp.send(socket, iodata)
  end

  defp set_sid(sub, state) do
    %{next_sid: next_sid} = state
    sub = %{sub | sid: to_string(next_sid)}
    state = %{state | next_sid: next_sid + 1}
    {sub, state}
  end

  defp connect_with_handshake(state) do
    host = state.opts[:host] |> String.to_charlist()
    port = state.opts[:port]
    opts = [:binary, active: false, packet: :line]

    with {:ok, socket} <- :gen_tcp.connect(host, port, opts),
      {:ok, line} <- :gen_tcp.recv(socket, 0),
      Logger.debug("<<- #{inspect line}"),
      connect = Protocol.Connect.new(lang: "elixir", verbose: false, headers: state.opts[:headers]),
      :ok <- send_message(connect, socket),
      :ok <- resubscribe(socket, state.subs),
      :ok <- :inet.setopts(socket, active: :once)
    do
      [_, info] = String.split(line, " ", parts: 2)
      {:ok, socket, Jason.decode!(info)}
    end
  end

end
