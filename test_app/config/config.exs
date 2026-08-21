import Config

# Runner-only boot: depend on flezha as a library without becoming a controller.
config :flezha, controller_enabled: false
config :flezha, testnet_enabled: false

# A runner node does not talk to Kafka; keep KafkaEx from dialing a broker at
# boot (the controller — not the runner — owns plugin providers).
config :kafka_ex, disable_default_worker: true, kafka_version: "kayrock"
