# WIP

# defmodule Nats.Subscription do
#   use GenServer
#   import Nats.Utils
#   alias Nats.Protocol

#   @defaults [
#     subs: [],
#     msg_handler: nil
#   ]

#   def start_link(opts \\ []) do
#     {opts, gen_opts} = default_opts(opts, @defaults)
#     GenServer.start_link(__MODULE__, opts, gen_opts)
#   end

#   def init(opts) do
#     state = %{
#       opts: opts,
#       nats: new_nats_client(opts)
#     }

#     {:ok, state}
#   end

#   def handle_info(%Protocol.Msg{} = msg, state) do
#     apply(state.opts[:msg_handler], :handle_msg, [state.nats, msg])
#     {:noreply, state}
#   end

#   defp new_nats_client(opts) do
#     opts = Keyword.merge(opts, msg_handler: self())
#     {:ok, pid} = Nats.Client.start_link(opts)
#     pid
#   end

# end
