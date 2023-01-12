/*
 * Copyright 2022 Max Planck Institute for Software Systems
 */

#pragma once
#ifndef SRC_TRACING_HINDSIGHT_EXTENSIONS_H_
#define SRC_TRACING_HINDSIGHT_EXTENSIONS_H_

#include <iostream>

extern "C" {
  #include "tracestate.h"
}

#include "opentelemetry/common/attribute_value.h"


namespace common  = opentelemetry::common;

enum class EventType {
  // Core span fields
  kSpanStart = 0,
  kSpanEnd,
  kSpanName,
  kSpanParent,

  // Generic attributes
  kAttributeKey,
  kAttributeValue,

  // Generic events
  kEvent,
  kEventAttributeKey,
  kEventAttributeValue,

  // Generic links -- not implemented yet
  kLink, // payload is the links span context
  kLinkAttributeKey,
  kLinkAttributeValue,

  // Specific span fields used by otel
  // See https://github.com/open-telemetry/opentelemetry-cpp/blob/main/sdk/src/trace/span.cc
  kStatus,
  kStatusDescription,
  kSpanKind,
  kTracer
};

// events are written to hindsight
struct Event {
  EventType type;
  uint64_t span_id; // Most events belong to a span
  uint64_t timestamp;   // optional
  size_t size; // payload size, some events have no payload  
};



/*
The typical usage of Hindsight is to store tracestate in a thread-local variable.
However, in some use cases an application prefers to manage the tracestate itself.
This is the case in OpenTelemetry - spans manage the tracestate rather than TLS.

This class contains a tracestate instance and APIs for writing data to the 
trace state.  The APIs are similar to that in the base `hindsight.h` in the 
Hindsight library, but they will write to the object's tracesstate instance
rather than TLS.

This class is NOT thread-safe -- callers should use their own synchronization
if they want to use this in a thread-safe manner.

The easiest way to use HindsightTraceState is with shared_ptr which ensures that
`hindsight_end` is called when the HindsightTraceState is destroyed.
*/
class HindsightTraceState {
public:
  HindsightTraceState(uint64_t trace_id, uint64_t parent_span_id);
  ~HindsightTraceState();

  bool Recording();

  void ReportBreadcrumb(const std::string &breadcrumb);

  void Trigger(int queue_id);

  void LogSpanStart(uint64_t span_id);

  void LogSpanEnd(uint64_t span_id);

  void LogSpanName(uint64_t span_id, const std::string &name);

  void LogSpanParent(uint64_t span_id, uint64_t parent_id);

  void LogSpanAttribute(uint64_t span_id, const std::string &key, const common::AttributeValue &value);

  void LogSpanAttributeStr(uint64_t span_id, const std::string &key, const std::string &value);

  void LogSpanEvent(uint64_t span_id, const std::string &name);

  void LogSpanEventAttribute(uint64_t span_id, const std::string &key, const common::AttributeValue &value);

  void LogSpanStatus(uint64_t span_id, int status, const std::string &description);

  void LogSpanKind(uint64_t span_id, int spankind);

  void LogTracer(uint64_t span_id, const std::string &tracer);

public:
  uint64_t trace_id;
  uint64_t parent_span_id;

private:

  TraceState ts; // The actual hindsight tracestate

  // OpenTelemetry uses a variant type for attribute values,
  // which means we have to handle all possible types
  void LogAttribute(Event &e, const common::AttributeValue &value);

  // Write an event that has no payload
  void WriteEvent(Event &e);

  // Write an event with a payload.  e.size must
  // be the payload size
  void WriteEvent(Event &e, char* payload);

};

#endif  // SRC_TRACING_HINDSIGHT_EXTENSIONS_H_