defmodule Nats.Protocol.Sub do
  @moduledoc """
  Represents a Nats subscription.
  """

  defstruct [:subject, :sid, :queue_group, :receiver, :monitor]

  @typedoc """
  Nats subscription struct.
  """
  @type t :: %__MODULE__{}

  @doc """
  Convenience to construct a `t:#{inspect __MODULE__}.t/0`.
  """
  @spec new(atom | binary, atom | binary | integer, atom | binary | nil) :: t()
  def new(subject, sid, queue_group \\ nil) do

    # Normalize subject.
    subject = case subject do
      nil -> raise ArgumentError, "subject cannot be nil"
      subject when is_binary(subject) -> subject
      subject when is_atom(subject) -> to_string(subject)
    end

    # Normalize sid.
    sid = case sid do
      nil -> nil
      sid -> to_string(sid)
    end

    # Normalize queue_group.
    queue_group = case queue_group do
      nil -> nil
      queue_group when is_binary(queue_group) -> queue_group
      queue_group when is_atom(queue_group) -> to_string(queue_group)
    end

    %__MODULE__{
      subject: subject,
      sid: sid,
      queue_group: queue_group
    }
  end

  @doc """
  Convert to iodata for use with Nats wire protocol.
  """
  @spec to_iodata(t()) :: list
  def to_iodata(sub) when is_struct(sub, __MODULE__) do
    %{
      subject: subject,
      sid: sid,
      queue_group: queue_group
    } = sub

    msg = ["SUB", " ", subject]

    msg = if queue_group do
      [msg, " ", queue_group]
    else
      msg
    end

    [msg, " ", to_string(sid), "\r\n"]
  end

end
