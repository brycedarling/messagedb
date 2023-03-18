# syntax=docker/dockerfile:1.3.0-labs
FROM postgres:14.1-alpine3.15

ENV MESSAGEDB_VERSION=v1.2.6
ENV MESSAGEDB_REPO=https://github.com/message-db/message-db
ENV BUILD_DIR=/build

WORKDIR /build

RUN --mount=type=cache,target=/var/cache/apk \
    apk add bash && \
    apk add git && \
    git clone -b ${MESSAGEDB_VERSION} ${MESSAGEDB_REPO}

RUN <<XX cat > /docker-entrypoint-initdb.d/init-messagedb.sh
#!/usr/bin/env bash

cd ${BUILD_DIR}/message-db/database
./install.sh
XX

RUN chmod 555 /docker-entrypoint-initdb.d/init-messagedb.sh && \
    chown -R postgres:postgres /docker-entrypoint-initdb.d/init-messagedb.sh && \
    chown -R postgres:postgres /build/message-db