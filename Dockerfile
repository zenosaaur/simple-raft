FROM rust:latest
RUN apt-get update && \
    apt-get install -y protobuf-compiler --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/app
COPY . .
