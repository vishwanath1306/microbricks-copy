/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#pragma once
#ifndef SRC_TRACING_OPENTELEMETRY_H_
#define SRC_TRACING_OPENTELEMETRY_H_

#include "grpc_propagation.h"

// Used by the stdout tracer config
#include "opentelemetry/exporters/jaeger/jaeger_exporter.h"
#include "opentelemetry/exporters/memory/in_memory_span_exporter.h"
#include "opentelemetry/exporters/ostream/span_exporter.h"
#include "opentelemetry/sdk/trace/sampler.h"
#include "opentelemetry/sdk/trace/samplers/always_on.h"
#include "opentelemetry/sdk/trace/samplers/parent.h"
#include "opentelemetry/sdk/trace/simple_processor.h"
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/trace/noop.h"

namespace hindsightgrpc {

namespace trace_sdk = opentelemetry::sdk::trace;
using opentelemetry::sdk::resource::Resource;
using opentelemetry::trace::TracerProvider;

inline void initNoopOpenTelemetry() {
  auto provider = nostd::shared_ptr<TracerProvider>(
      new opentelemetry::trace::NoopTracerProvider());

  // Set the global trace provider
  trace_api::Provider::SetTracerProvider(provider);

  // Set the global propagator
  initGrpcPropagation();
}

inline void initTracer(std::unique_ptr<trace_sdk::SpanExporter>& exporter, bool batch_processor) {

  std::vector<std::unique_ptr<trace_sdk::SpanProcessor>> processors;

  if (batch_processor) {
    trace_sdk::BatchSpanProcessorOptions options{};
    // Default options are:
    // max_queue_size 2048
    // max_export_batch_size 512
    // schedule_delay_millis 5000
    options.max_queue_size = 65536;
    options.max_export_batch_size = 32768;
    options.schedule_delay_millis = std::chrono::milliseconds(100);
    auto processor = std::unique_ptr<trace_sdk::SpanProcessor>(
        new trace_sdk::BatchSpanProcessor(std::move(exporter), options));
    processors.push_back(std::move(processor));
  } else {
    auto processor = std::unique_ptr<trace_sdk::SpanProcessor>(
        new trace_sdk::SimpleSpanProcessor(std::move(exporter)));
    processors.push_back(std::move(processor));
  }


  auto context = std::make_shared<trace_sdk::TracerContext>(
      std::move(processors), Resource::Create({}),
      std::unique_ptr<trace_sdk::Sampler>(new trace_sdk::ParentBasedSampler(
          std::make_shared<trace_sdk::AlwaysOnSampler>())));
  auto provider = opentelemetry::nostd::shared_ptr<TracerProvider>(
      new trace_sdk::TracerProvider(context));

  trace_api::Provider::SetTracerProvider(provider);
  // Set the global propagator
  initGrpcPropagation();
}

inline void initStdoutOpenTelemetry() {
  auto exporter = std::unique_ptr<trace_sdk::SpanExporter>(
      new opentelemetry::exporter::trace::OStreamSpanExporter);
  initTracer(exporter, false);
}

inline void initLocalMemoryOpenTelemetry() {
  auto exporter = std::unique_ptr<trace_sdk::SpanExporter>(
      new opentelemetry::exporter::memory::InMemorySpanExporter);
  initTracer(exporter, false);
}

inline void initJaegerOpenTelemetry(std::string exporter_ip,
                                    int exporter_port,
                                    bool batch_exporter) {
  opentelemetry::exporter::jaeger::JaegerExporterOptions opts;
  opts.endpoint = exporter_ip;
  opts.server_port = exporter_port;
  opts.transport_format =
      opentelemetry::exporter::jaeger::TransportFormat::kThriftUdpCompact;
  auto exporter = std::unique_ptr<trace_sdk::SpanExporter>(
      new opentelemetry::exporter::jaeger::JaegerExporter(opts));
  initTracer(exporter, batch_exporter);
}

}  // namespace hindsightgrpc

#endif  // SRC_TRACING_OPENTELEMETRY_H_