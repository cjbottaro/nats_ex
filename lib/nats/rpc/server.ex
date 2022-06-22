defmodule Nats.Rpc.Server do
  @moduledoc ~S"""
  Simple JSON based RPC server.

  You can use this with plain `Nats.Client.request/3`, but it's better paired
  with `Nats.Rpc.call/3`.

  ```
  defmodule MyServer do
    use Nats.Rpc.Server

    def echo(s) do
      "Echo: #{s}"
    end

    def echo(s1, s2) do
      "Echo: #{s1} and #{s2}"
    end

    def json_echo(s) do
      %{echo: s}
    end
  end

  {:ok, _pid} = MyServer.start_link()
  {:ok, conn} = Nats.Client.start_link()

  {:ok, "Echo: hello"} = Nats.Rpc.call(conn, "my_server.echo", ["hello"])
  {:ok, "Echo: hi and bye"} = Nats.Rpc.call(conn, "my_server.echo", ["hi", "bye"])
  {:ok, %{"echo" => "hello"}} = Nats.Rpc.call(conn, "my_server.json_echo", ["hello"])
  ```

  ## Protocol

  Requests have JSON payload with key `"args"`:
  ```
  payload = %{ args: ["hi", "bye"] }
  Nats.Client.request(conn, "foo.echo.multi-arg", payload: payload)
  ```

  Responses also have a JSON payload, but key depends on success or failure:

  On success:
  ```
  {:ok, resp} = Nats.Client.request(conn, "add-integers", payload: %{args: [1, 2]})
  %{payload: %{"value" => 3}} = resp
  ```

  On Error
  ```
  {:ok, resp} = Nats.Client.request(conn, "add-integers", payload: %{args: ["1", 2]})
  %{payload: %{"error" => ["ArgumentError", "\"1\" is not an integer"]}} = resp
  ```

  You can extend or modify this protocol by overriding the callbacks `c:process_request/2`
  and `c:process_response/2`.

  ## Routing

  Routing is inferred by the module basename but can be overridden.

      defmodule Foo.Bar do
        use Nats.Rpc.Server

        # "bar.my_func"
        def my_func, do: nil
      end

      defmodule Foo.Bar do
        use Nats.Rpc.Server, base_route: "baz"

        # "baz.my_func"
        def my_func, do: nil
      end

      defmodule Foo.Bar do
        use Nats.Rpc.Server

        # "some.thing"
        @route "some.thing"
        def my_func, do: nil
      end

  You can see all the routes with `Foo.Bar.routes`.

  """

  @defaults [
    shutdown_grace_period: 25_000,
    log: true,
  ]
  @doc """
  Default configuration options.

  ```
  #{inspect @defaults, pretty: true, width: 0}
  ```
  """
  @spec defaults :: Keyword.t
  def defaults, do: @defaults

  def config do
    config = Application.get_application(__MODULE__)
    |> Application.get_env(__MODULE__, [])

    Keyword.merge(defaults(), config)
  end


  @doc """
  Process request payload before sending args to handler function.

  See the Protocol section for what a request payload should look like.

  ```
  def process_request("example.off-by-one-add", payload) do
    update_in payload["args"], fn [n1, n2] ->
      [n1, n2+1]
    end
  end
  ```
  """
  @callback process_request(subject :: binary, payload :: map) :: map

  @doc """
  Process response payload before sending return value to the caller.

  See the Protocol section for what a response payload should look like.

  ```
  def process_response("example.off-by-one-add", payload) do
    update_in payload["value"], & &1+1
  end
  ```
  """
  @callback process_response(subject :: binary, payload :: map) :: map

  defmacro __using__(opts \\ []) do
    base = __MODULE__

    quote do
      @opts unquote(opts)
      @base unquote(base)
      @routes %{}
      @behaviour @base
      @on_definition @base
      @before_compile @base

      def config do
        mix_config = Application.get_env(@opts[:otp_app], __MODULE__, [])

        @base.defaults()
        |> Keyword.merge(mix_config)
        |> Keyword.merge(@opts)
        |> Keyword.put(:module, __MODULE__)
      end

      def child_spec(config) do
        config = Keyword.merge(config(), config)
        %{
          id: {@base, __MODULE__},
          start: {@base, :start_link, [config, [name: __MODULE__]]},
          shutdown: config[:shutdown_grace_period]
        }
      end

      def start_link(config \\ []) do
        children = [{__MODULE__, Keyword.merge(config(), config)}]
        Supervisor.start_link(children, strategy: :one_for_one, name: __MODULE__)
      end

      def stop do
        Supervisor.stop(__MODULE__)
      end

      def process_request(_subject, payload), do: payload
      defoverridable(process_request: 2)

      def process_response(_subject, payload), do: payload
      defoverridable(process_response: 2)

      def process_error(_subject, _payload, _error), do: nil
      defoverridable(process_error: 3)

      def process_max_size(_subject, payload, _json), do: payload
      defoverridable(process_max_size: 3)
    end
  end

  defmodule CompileError do
    defexception [:message]
  end

  def routable_function?(<<"process_", _::binary>>), do: false
  def routable_function?("start_link"), do: false
  def routable_function?("stop"), do: false
  def routable_function?("child_spec"), do: false
  def routable_function?("config"), do: false
  def routable_function?(_), do: true

  def __on_definition__(env, :def, name, _args, _guards, _body) do
    if routable_function?(to_string(name)) do
      route = Module.get_attribute(env.module, :route)
      routes = Module.get_attribute(env.module, :routes)

      dispatch_to = routes[route]

      if route && dispatch_to && dispatch_to != name do
        raise CompileError, "\"#{route}\" is already routed to #{inspect dispatch_to}"
      end

      route = route || to_string(name)

      Module.put_attribute(env.module, :routes, Map.put(routes, route, name))
      Module.delete_attribute(env.module, :route)
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      base_route = @opts[:base_route] || (
        inspect(__MODULE__)
        |> String.split(".")
        |> List.last()
        |> Macro.underscore()
      )
      @routes Map.new(@routes, fn {k, v} ->
        {
          Enum.join([base_route, k], "."),
          v
        }
      end)

      def routes, do: @routes
    end
  end

  use GenServer
  require Logger

  def start_link(config, opts \\ []) do
    GenServer.start_link(__MODULE__, config, opts)
  end

  def init(config) do
    Process.flag(:trap_exit, true)

    subjects = config[:module].routes
    |> Map.keys()
    |> Enum.map(fn subject ->
      [head | rest] = String.split(subject, ".")
      rest = Enum.map(rest, fn _ -> "*" end)
      Enum.join([head | rest], ".")
    end)
    |> Enum.uniq()

    queue_group = Macro.underscore(config[:module])
    subs = Enum.map(subjects, fn subject ->
      Nats.Sub.new(subject, queue_group: queue_group)
    end)

    name = inspect(config[:module])
    Logger.info "#{name} starting up -- #{inspect subjects}"

    {:ok, conn} = Keyword.take(config, [:host, :port])
    |> Keyword.put(:subs, subs)
    |> Nats.Client.start_link()

    {:ok, info} = Nats.Client.info(conn)

    state = %{
      conn: conn,
      config: config,
      tasks: %{},
      name: name,
      max_payload: info["max_payload"]
    }

    {:ok, state}
  end

  def handle_info(%Nats.Protocol.Msg{} = msg, state) do
    module = state.config[:module]
    msg = %{msg | payload: Cyanide.decode!(msg.payload)}
    fn_name = module.routes[msg.subject]

    # Kick off the task.
    task = Task.async(fn ->
      payload = module.process_request(msg.subject, msg.payload)
      value = apply(module, fn_name, payload["args"])
      payload = module.process_response(msg.subject, %{"value" => value})
      resolve_max_payload(payload, msg.subject, state)
    end)

    # Annotate it.
    task = Map.merge(task, %{
      start_at: System.monotonic_time(:microsecond),
      msg: msg
    })

    # Add to tasks.
    tasks = Map.put(state.tasks, task.ref, task)

    {:noreply, %{state | tasks: tasks}}
  end

  # Task.async both links and monitors, so we can ignore the :EXIT messages from
  # the link because we're already handling the :DOWN messages from the monitor.
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  # Task completed successfully.
  def handle_info({ref, value}, state) when is_map_key(state.tasks, ref) do
    %{tasks: tasks} = state

    # Stop :DOWN message from being sent. According to the docs, this is very
    # efficient; more efficient than handling and ignoring the :DOWN message.
    Process.demonitor(ref, [:flush])

    {task, tasks} = Map.pop(tasks, ref)
    :ok = respond_success(task, value, state)

    {:noreply, %{state | tasks: tasks}}
  end

  # Task failed with error/exit.
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) when is_map_key(state.tasks, ref) do
    %{tasks: tasks} = state

    {task, tasks} = Map.pop(tasks, ref)
    :ok = respond_failure(task, reason, state)

    {:noreply, %{state | tasks: tasks}}
  end

  def terminate(reason, state) do
    %{tasks: tasks, name: name, conn: conn} = state

    count = map_size(tasks)

    Logger.info "#{name} shutting down gracefully -- #{count} pending request(s)"

    # Unsub so we don't try to process anymore requests while shutting down. We
    # can't just close the connection because we need it to reply to any pending
    # requests.
    {:ok, subs} = Nats.Client.subs(conn)
    Enum.each(subs, fn sub -> :ok = Nats.Client.unsub(conn, sub.sid) end)

    finished = Map.values(tasks)
    |> Task.yield_many(state.config[:shutdown_grace_period] - 1000)
    |> Enum.reduce(0, fn
      {task, {:ok, value}}, acc ->
        :ok = respond_success(task, value, state)
        acc + 1

      {task, {:exit, error}}, acc ->
        :ok = respond_failure(task, error, state)
        acc

      {task, nil}, acc -> case Task.shutdown(task, :brutal_kill) do
        {:ok, value} ->
          :ok = respond_success(task, value, state)
          acc + 1

        {:exit, error} ->
          :ok = respond_failure(task, error, state)
          acc

        nil ->
          :ok = respond_failure(task, reason, state)
          acc
      end
    end)

    Logger.info("#{name} shutdown -- #{count - finished} pending request(s)")

    state
  end

  defp respond_success(task, payload, state) do
    %{conn: conn} = state

    :telemetry.execute(
      [:nats, :rpc, :server, :ok],
      %{usec: System.monotonic_time(:microsecond) - task.start_at},
      %{
        subject: task.msg.subject,
        payload: task.msg.payload,
        config: state.config
      }
    )

    Nats.Client.pub(conn, task.msg.reply_to, payload: payload)
  end

  defp respond_failure(task, error, state) do
    %{config: config, conn: conn} = state

    {e, trace} = exit_to_exception(error)

    :telemetry.execute(
      [:nats, :rpc, :server, :error],
      %{usec: System.monotonic_time(:microsecond) - task.start_at},
      %{
        subject: task.msg.subject,
        payload: task.msg.payload,
        config: state.config,
        exception: e,
        stacktrace: trace,
        error: error
      }
    )

    # TODO This needs to be done async in case it fails.
    config[:module].process_error(task.msg.subject, task.msg.payload, e, trace)

    payload = %{error: [inspect(e.__struct__), Exception.message(e)]}
    |> Cyanide.encode!()

    Nats.Client.pub(conn, task.msg.reply_to, payload: payload)
  end

  defmodule MaxResponseSizeError, do: defexception [:message]

  defp resolve_max_payload(payload, subject, state) do
    %{max_payload: max_payload, config: config} = state

    json = Cyanide.encode!(payload)
    size = byte_size(json)

    if size > max_payload do

      :telemetry.execute(
        [:nats, :rpc, :server, :max_payload],
        %{},
        %{
          config: state.config,
          size: size,
          subject: subject,
          payload: Map.drop(payload, [:value])
        }
      )

      new_payload = config[:module].process_max_size(subject, payload, json)
      if new_payload == payload do
        raise MaxResponseSizeError, "response exceeds #{max_payload} bytes"
      else
        new_json = Cyanide.encode!(new_payload)
        if byte_size(new_json) > max_payload do
          raise MaxResponseSizeError, "response exceeds #{max_payload} bytes"
        else
          new_json
        end
      end
    else
      json
    end
  end

  defmodule TaskExit, do: defexception [:message]

  defp exit_to_exception(error) do
    case error do
      {reason, trace} ->
        e = Exception.normalize(:error, reason, trace)
        {e, trace}

      reason when is_atom(reason) ->
        e = %__MODULE__.TaskExit{message: to_string(reason)}
        {e, []}
    end
  end

end
