## Instrumenting using OpenTelemetry-Jaeger
### Exporters
OpenTelemetry deploys `exporter`s to process and transport telemetry data to a backend. Jaeger is one of the [exporter options](https://github.com/open-telemetry/opentelemetry-specification/tree/main/specification/trace/sdk_exporters). 

The `ot-jaeger` tracing option enables Jaeger as the exporter for OpenTelemetry.
### Collectors
> The OpenTelemetry project facilitates the collection of telemetry data via the OpenTelemetry Collector. The OpenTelemetry Collector offers a vendor-agnostic implementation on how to receive, process, and export telemetry data. It removes the need to run, operate, and maintain multiple agents/collectors in order to support open-source observability data formats (e.g. Jaeger, Prometheus, etc.) sending to one or more open-source or commercial back-ends. In addition, the Collector gives end-users control of their data. The Collector is the default location instrumentation libraries export their telemetry data.

For more explanation and configuration please refer to: https://opentelemetry.io/docs/concepts/data-collection/.

To deploy a `collector`:
```bash
# install go: https://go.dev/doc/install
# note: please do not install go-lang using apt install, which is outdated

# build 
git clone -b release/v0.48.x https://github.com/open-telemetry/opentelemetry-collector-contrib
cd opentelemetry-collector-contrib
# you may encounter errors with outdated go
# tested with go1.17
# resolve all dependencies
go get -d ./...
# make
make otelcontribcol-linux_amd64 -j
# binary is generated in bin/
./bin/otelcontribcol_linux_amd64 --config /path/to/config/file
# a sample config file: config/sample_otel_collector_config.yaml
```
