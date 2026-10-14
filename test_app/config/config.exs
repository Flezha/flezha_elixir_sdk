import Config

# The runner-exit test builds this app while the SDK's own suite has the native
# library loaded, and a path dependency's priv is the SDK's own directory: a
# build extracting the library there would rewrite the file that VM has mapped.
# That build needs no library.
if config_env() == :watcher, do: config(:flezha_sdk, socket_nif: false)
