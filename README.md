# xk6-influxdbv2

k6 output extension for **InfluxDB 3 Core / Enterprise**, using the native [`POST /api/v3/write_lp`](https://docs.influxdata.com/influxdb3/core/write-data/http-api/v3-write-lp/) endpoint. The repository and Go module retain their existing name; the output now targets InfluxDB 3 only.

## Build

Install Go (see `go.mod`), Git, and [xk6](https://github.com/grafana/xk6). To build from this checkout:

```bash
xk6 build --with github.com/li-zhixin/xk6-influxdbv2=.
```

The `=.` ensures that your local changes are included. To build a published revision instead:

```bash
xk6 build --with github.com/li-zhixin/xk6-influxdbv2@main
```

## Run

Create an InfluxDB 3 database and a token with write access, then run:

```bash
K6_INFLUXDB_DATABASE=k6 \
K6_INFLUXDB_TOKEN='<your-influxdb3-token>' \
./k6 run -o xk6-influxdb=http://localhost:8181 script.js
```

The output name remains `xk6-influxdb`. The database can also be set in the output URL: `-o xk6-influxdb=http://localhost:8181/k6`.

## Configuration

Configuration priority is **defaults < JSON < environment < output URL**. The URL sets the server address and, when present, the database path. JSON keys are shown below.

| Environment variable | JSON key | Default | Description |
| --- | --- | --- | --- |
| `K6_INFLUXDB_ADDR` | `addr` | `http://localhost:8181` | InfluxDB 3 server URL. |
| `K6_INFLUXDB_DATABASE` | `database` | required | Database name, sent as the `db` query parameter. |
| `K6_INFLUXDB_TOKEN` | `token` | empty | Sent as `Authorization: Bearer <token>`. May be omitted for a server with authentication disabled. |
| `K6_INFLUXDB_PUSH_INTERVAL` | `pushInterval` | `1s` | Interval between metric flushes. |
| `K6_INFLUXDB_CONCURRENT_WRITES` | `concurrentWrites` | `4` | Maximum concurrent write requests. |
| `K6_INFLUXDB_WRITE_TIMEOUT` | `writeTimeout` | `1m` | Timeout for each write request; must be positive. |
| `K6_INFLUXDB_PRECISION` | `precision` | `1ns` | `1ns`, `1us`, `1ms`, or `1s`. Both encoded timestamps and API precision use this setting. |
| `K6_INFLUXDB_TAGS_AS_FIELDS` | `tagsAsFields` | `vu:int,iter:int,url` | Tags to convert to fields. Types: `string`, `int`, `float`, `bool`. JSON uses an array of strings. |
| `K6_INFLUXDB_INSECURE` | `insecureSkipTLSVerify` | `false` | Skip TLS certificate verification. |

Each metric name becomes an InfluxDB table; its numeric value is stored in the `value` field. Other tags remain tags except those configured as fields. Empty tag values are omitted. Keep field types consistent across writes and do not use `value` as a custom tag or in `tagsAsFields`.

Writes use `accept_partial=false` and `no_sync=false`: an invalid batch is rejected as a whole, and acknowledgements wait for WAL persistence. Write failures are logged and the first failure is returned when the output stops. Requests have a timeout and are not automatically retried. Shutdown flushes buffered samples and waits for active requests.

This is a breaking change from the earlier v2 implementation: use `K6_INFLUXDB_DATABASE` instead of `K6_INFLUXDB_BUCKET`, remove `K6_INFLUXDB_ORGANIZATION`, and point the output at an InfluxDB 3 server. No v1/v2 write endpoints are used. The line-protocol codec's Go module version (`/v2`) refers to that library's version, not the InfluxDB HTTP API.

## Local Docker example

The Compose example runs InfluxDB 3 and a locally built k6 extension:

```bash
docker compose up -d influxdb
docker compose exec influxdb influxdb3 create token --admin
```

Copy the generated token, create the database, and run the included smoke script:

```bash
export K6_INFLUXDB_TOKEN='<generated-token>'
docker compose exec -e INFLUXDB3_AUTH_TOKEN="$K6_INFLUXDB_TOKEN" influxdb influxdb3 create database k6
docker compose run --rm --build k6 run /scripts/influxdb3.js
```

Read the written samples using SQL:

```bash
docker compose exec -e INFLUXDB3_AUTH_TOKEN="$K6_INFLUXDB_TOKEN" influxdb \
  influxdb3 query --database k6 'SELECT * FROM k6_smoke ORDER BY time'
```

The previous Flux dashboards have been removed. For Grafana, configure an InfluxDB 3 data source with SQL or InfluxQL queries.

## Development

```bash
go test -race -timeout 60s ./...
go build ./...
```

Tests use local HTTP servers to check native v3 requests, precision, authentication, TLS, timeouts, and error reporting.
