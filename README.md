# Raft + Mini Key‑Value Store

A lightweight Raft consensus implementation with a minimal key‑value database.

## Run

Make sure Rust is installed, then configure each node with a YAML file like:

```yaml
host: "0.0.0.0"
port: 8080
domain: "localhost"
log_file: "application_1.log"
state_file: "state_1.json"

peers:
  - id: "node-2"
    address: "127.0.0.1:8081"
  - id: "node-3"
    address: "127.0.0.1:8082"
```

### Start a node

```bash
cargo run --bin server path_to_config.yaml
```

### Run a client

```bash
cargo run --bin client <url-of-node>
```

## Commands

```
SET <table> <pk_value> <column> <value>
GET <table> <pk_value> <column>
```

Example:

```
SET User alice username alice
SET User alice password s3cr3t
GET User alice username
```

Currently available table: **User** (fields: `username`, `password`).
