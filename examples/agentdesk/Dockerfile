# The runner's image: the release `MIX_ENV=prod mix release` builds, on a Debian 13 runtime.
FROM debian:trixie-slim
COPY _build/prod/rel/agentdesk /agentdesk
# The runner is not an Erlang cluster node, and writes its temporary files where it may.
ENV LANG=C.UTF-8 RELEASE_DISTRIBUTION=none RELEASE_TMP=/tmp
USER 1000
ENTRYPOINT ["/agentdesk/bin/agentdesk", "start"]
