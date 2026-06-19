FROM elixir:1.19-otp-28-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git build-base ca-certificates

ENV MIX_ENV=prod
ENV ERL_FLAGS="+JPperf true"

COPY mix.exs mix.lock ./
RUN mix local.hex --force && \
    mix local.rebar --force && \
    mix deps.get --only prod && \
    mix deps.compile

COPY config config
COPY lib lib
COPY rel rel

RUN mix compile && \
    mix release

FROM alpine:3.21 AS runner

ARG APP_VERSION

LABEL org.opencontainers.image.title="Malachi" \
      org.opencontainers.image.description="High-performance message system" \
      org.opencontainers.image.version="${APP_VERSION}" \
      org.opencontainers.image.source="https://github.com/HectorIFC/malachi" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.vendor="HectorIFC"

RUN apk add --no-cache \
        libstdc++ \
        libgcc \
        ncurses-libs \
        ca-certificates \
        curl

# Copy OpenSSL libraries from builder to ensure binary compatibility with crypto.so
# The elixir:1.19-otp-28-alpine image has crypto.so compiled against a specific OpenSSL version
# that may not match Alpine 3.21's package version
COPY --from=builder /usr/lib/libssl.so.3 /usr/lib/
COPY --from=builder /usr/lib/libcrypto.so.3 /usr/lib/

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

WORKDIR /app

RUN addgroup -g 1000 malachi && \
    adduser -u 1000 -G malachi -s /bin/sh -D malachi

RUN mkdir -p /app/data/mnesia && \
    chown -R malachi:malachi /app/data

COPY --from=builder --chown=malachi:malachi /app/_build/prod/rel/malachi ./

USER malachi

ENV MALACHIMQ_TCP_PORT=4040
ENV MALACHIMQ_DASHBOARD_PORT=4041
ENV MALACHI_LOCALE=en_US

EXPOSE 4040 4041

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD curl -sf http://localhost:4041/login > /dev/null || exit 1

# Security hardening recommendations:
#   docker run --security-opt=no-new-privileges:true \
#     --read-only --tmpfs /tmp:rw,noexec,nosuid \
#     hectorcardoso/malachi:0.5.0

CMD ["bin/malachi", "start"]
