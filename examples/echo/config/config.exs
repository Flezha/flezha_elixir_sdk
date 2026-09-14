import Config

# The flezha package is the controller and the SDK in one. This application is
# a runner, so the controller half stays off and nothing but the SDK starts.
config :flezha, controller_enabled: false, testnet_enabled: false

# `:kafka_ex` is a dependency of flezha and starts a default worker when its
# application starts. With no brokers configured that worker raises and takes
# the boot with it, and a runner never talks to Kafka.
config :kafka_ex, disable_default_worker: true, kafka_version: "kayrock"
