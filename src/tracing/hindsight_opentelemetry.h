/*
 * Copyright 2022 Max Planck Institute for Software Systems
 */

#pragma once
#ifndef SRC_TRACING_HINDSIGHT_H_
#define SRC_TRACING_HINDSIGHT_H_

#include <iostream>

#include "hindsight_extensions.h"

#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/trace/tracer_provider.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/trace/id_generator.h"


/*
This file contains the Hindsight implementation of an OpenTelemetry tracer.

The implementation comprises the following main pieces:

(1) A TracerProvider which doesn't really do anything, but instantiates Hindsight
Tracers.
(2) A HindsightTracer which implements the core StartSpan API
(3) A HindsightSpan which is an implementation of the Span API that routes all
API calls to Hindsight.

The key design decisions of the Hindsight OpenTelemetry tracer are as follows:
* HindsightSpan simply logs events to Hindsight rather than building up events
and attributes on an in-memory span object.
* HindsightSpan includes the Hindsight breadcrumb metadata in its trace context
* HindsightSpan doesn't use Hindsight's thread-local trace state, since 
OpenTelemetry decouples the thread-local context propagation from spans.  Instead
Hindsight stores its trace state as a shared pointer in the span.  Trace state is
inherited from the top-most (local) parent span.
*/

namespace hindsightgrpc {

namespace nostd = opentelemetry::nostd;

using opentelemetry::trace::Tracer;
using opentelemetry::trace::TracerProvider;
using opentelemetry::trace::Span;
using opentelemetry::trace::SpanContext;
using opentelemetry::common::KeyValueIterable;
using opentelemetry::trace::SpanContextKeyValueIterable;
using opentelemetry::trace::StartSpanOptions;
using opentelemetry::sdk::trace::IdGenerator;


class HindsightTracer final : public Tracer, public std::enable_shared_from_this<HindsightTracer> {
public:
  HindsightTracer(nostd::string_view output) noexcept;
  ~HindsightTracer();

  nostd::shared_ptr<Span> StartSpan(
      nostd::string_view name,
      const KeyValueIterable & /*attributes*/,
      const SpanContextKeyValueIterable & /*links*/,
      const StartSpanOptions & /*options */) noexcept override;

  nostd::shared_ptr<Span> StartSpan(
      nostd::string_view name,
      const KeyValueIterable & /*attributes*/,
      const SpanContextKeyValueIterable & /*links*/,
      const StartSpanOptions & /*options */,
      std::shared_ptr<HindsightTraceState> hindsight_ts,
      SpanContext &parent_context
      ) noexcept;

  void ForceFlushWithMicroseconds(uint64_t /*timeout*/) noexcept override {}

  void CloseWithMicroseconds(uint64_t /*timeout*/) noexcept override {}

private:
  nostd::string_view output;
  IdGenerator* id_generator;
  std::string local_address;

};

class HindsightTracerProvider : public opentelemetry::trace::TracerProvider {

public:
  HindsightTracerProvider() {}
  ~HindsightTracerProvider() {}

  nostd::shared_ptr<Tracer> GetTracer(nostd::string_view library_name,
                                              nostd::string_view library_version = "",
                                              nostd::string_view schema_url      = "");

};


/*
Implementation of a Span that logs all span information to Hindsight
using an event format defined in hindsight_extensions.h
*/
class HindsightSpan final : public Span {
public:
    HindsightSpan(nostd::string_view tracer_name,
                  nostd::string_view name, 
                  const KeyValueIterable &attributes, 
                  const SpanContextKeyValueIterable &links, 
                  const StartSpanOptions &options, 
                  const trace_api::SpanContext &parent_span_context, 
                  std::unique_ptr<trace_api::SpanContext> span_context,
                  std::shared_ptr<HindsightTraceState> hindsight_ts) noexcept;

    ~HindsightSpan() override;

    void SetAttribute(nostd::string_view key, const opentelemetry::common::AttributeValue &value) noexcept override;

    void AddEvent(nostd::string_view name) noexcept override;

    void AddEvent(nostd::string_view name, opentelemetry::common::SystemTimestamp timestamp) noexcept override;

    void AddEvent(nostd::string_view name, opentelemetry::common::SystemTimestamp timestamp, const opentelemetry::common::KeyValueIterable &attributes) noexcept override;

    void SetStatus(opentelemetry::trace::StatusCode code, nostd::string_view description) noexcept override;

    void UpdateName(nostd::string_view name) noexcept override;

    void End(const opentelemetry::trace::EndSpanOptions &options={}) noexcept override;

    bool IsRecording() const noexcept override;

    opentelemetry::trace::SpanContext GetContext() const noexcept override;

    std::shared_ptr<HindsightTraceState> hs_;

private:
    uint64_t span_id;
    std::shared_ptr<HindsightTracer> tracer_;
    std::unique_ptr<opentelemetry::trace::SpanContext> span_context_;
    bool has_ended_;
};

/*
Initializes Hindsight
*/
void initHindsight(std::string service_name, std::string breadcrumb);

/*
Initializes OpenTelemetry to use the Hindsight tracer
*/
void initHindsightOpenTelemetry(std::string service_name, std::string breadcrumb);

} // namespace hindsightgrpc

#endif  // SRC_TRACING_HINDSIGHT_H_