defmodule Flezha.SDK.LocalStreamTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.{LocalStream, Transport}

  defp take_owner(handle, pid) do
    :ok = Transport.controlling_process(handle, pid)
    :ok = Transport.set_active(handle)
  end

  test "a frame sent on one side reaches the other side's owner by term identity" do
    {sdk, ctrl} = LocalStream.open()
    take_owner(ctrl, self())

    # message is any term, never assumed a binary
    msg = %{not: "a binary", n: 7}
    :ok = Transport.send_frame(sdk, "op-1", msg, %{"channel" => "in"})

    id = Transport.stream_id(ctrl)
    assert_receive {:flz_data, ^id, "op-1", ^msg, %{"channel" => "in"}}
    # both handles report the same logical stream id
    assert Transport.stream_id(sdk) == id
  end

  test "frames buffered while a side is inactive flush in order on set_active" do
    {sdk, ctrl} = LocalStream.open()

    :ok = Transport.send_frame(sdk, "op", 1, %{})
    :ok = Transport.send_frame(sdk, "op", 2, %{})
    :ok = Transport.send_frame(sdk, "op", 3, %{})

    # nothing delivered yet — no active owner on the controller side
    refute_received {:flz_data, _, _, _, _}

    take_owner(ctrl, self())
    id = Transport.stream_id(ctrl)
    assert_receive {:flz_data, ^id, "op", 1, %{}}
    assert_receive {:flz_data, ^id, "op", 2, %{}}
    assert_receive {:flz_data, ^id, "op", 3, %{}}
  end

  test "ownership transfer redirects delivery to the new owner" do
    {sdk, ctrl} = LocalStream.open()
    take_owner(ctrl, self())

    parent = self()
    other = spawn_link(fn -> receive do: (m -> send(parent, {:other_got, m})) end)
    :ok = Transport.controlling_process(ctrl, other)

    id = Transport.stream_id(ctrl)
    :ok = Transport.send_frame(sdk, "op", "x", %{})
    assert_receive {:other_got, {:flz_data, ^id, "op", "x", %{}}}
    refute_received {:flz_data, _, _, _, _}
  end

  test "owner death synthesizes a binary-reason close to the survivor" do
    {sdk, ctrl} = LocalStream.open()
    take_owner(sdk, self())

    id = Transport.stream_id(ctrl)
    dead = spawn(fn -> :ok end)
    ref = Process.monitor(dead)
    assert_receive {:DOWN, ^ref, :process, ^dead, _}

    :ok = Transport.controlling_process(ctrl, dead)
    assert_receive {:flz_closed, ^id, reason}
    assert is_binary(reason)
  end

  test "close/1 delivers a binary-reason close to the other side and stops the stream" do
    {sdk, ctrl} = LocalStream.open()
    take_owner(sdk, self())
    id = Transport.stream_id(sdk)

    :ok = Transport.close(ctrl)
    assert_receive {:flz_closed, ^id, "closed"}

    # the stream process is gone; further ops answer cleanly, never crash
    assert {:error, :closed} = Transport.send_frame(sdk, "op", "x", %{})
  end
end
