# xk6-output-influxdb-v3

This fork is a k6 output extension for InfluxDB 3 Core / Enterprise only. The Go module is github.com/li-zhixin/xk6-output-influxdb-v3 and the registered output name remains xk6-influxdb.

## Architecture

register.go delegates to pkg/influxdb. Configuration combines defaults, JSON, K6_INFLUXDB_ environment variables, then the output URL. Database is required; the default server is http://localhost:8181. Bucket and organization are not supported.

Samples are buffered and periodically encoded as line protocol. A semaphore bounds concurrent writes (default 4). The native client sends POST /api/v3/write_lp with db, precision, accept_partial=false, and no_sync=false. Authentication uses a Bearer token. The line-protocol/v2 Go module is a codec version, not a v2 HTTP API client.

Tags listed in tagsAsFields move into typed fields. By default these are vu:int, iter:int, and url. Other nonempty tags remain tags. The numeric metric is the value field; that name is reserved. Conversion results are cached per tag set within a batch.

Shutdown flushes buffered samples, waits for writers, then closes idle HTTP connections. Write errors are logged and the first error is returned by Stop. Requests have a configurable timeout (default 1 minute) and no automatic retries. The buffer can grow under sustained backpressure.

The Docker Compose example runs InfluxDB 3 and k6. Legacy v2 Flux dashboards are removed; use SQL or InfluxQL for visualization.

## Development

Use go test -race -timeout 60s ./... and go build ./.... Unit tests use httptest; no live database is needed. Build the local extension with xk6 build --with github.com/li-zhixin/xk6-output-influxdb-v3=.. The trailing =. is essential to include local changes.

The linter configuration is downloaded using the k6-ci ref in .github/workflows/all.yml. Do not commit the generated configuration.
