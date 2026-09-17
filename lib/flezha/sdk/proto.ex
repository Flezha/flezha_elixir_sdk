defmodule Flezha.SDK.Proto do
  @moduledoc """
  The `RunnerCommands` protobuf modules, generated into this package's own
  namespace from the schema in the contract set, so a controller that compiles
  the same schema into its own modules shares a VM with this one. Only
  `Flezha.SDK.Codec` names them.
  """

  use Protox,
    files: [Flezha.SDK.Contract.path!("protobufs/runner_commands.proto")],
    namespace: Flezha.SDK.Proto
end
