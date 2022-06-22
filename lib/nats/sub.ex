defmodule Nats.Sub do
  @enforce_keys [:subject, :receiver]
  defstruct [:subject, :queue_group, :receiver]

  def new(subject, opts \\ []) do
    %__MODULE__{
      subject: subject,
      queue_group: opts[:queue_group],
      receiver: opts[:reciever] || self()
    }
  end
end
