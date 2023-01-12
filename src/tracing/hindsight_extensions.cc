#include "hindsight_extensions.h"

#include <iostream>
#include "opentelemetry/nostd/variant.h"

extern "C" {
  #include "hindsight.h"
  #include "tracestate.h"
  #include "common.h"
  #include "trigger.h"
  #include "breadcrumb.h"
}

#define DEBUGHINDSIGHT(x) {}

namespace common    = opentelemetry::common;
namespace nostd = opentelemetry::nostd;

uint64_t ticks() {
    unsigned int lo, hi;

    // RDTSC copies contents of 64-bit TSC into EDX:EAX
    asm volatile("rdtsc" : "=a" (lo), "=d" (hi));
    return (unsigned long long)hi << 32 | lo;
}

HindsightTraceState::HindsightTraceState(uint64_t trace_id, uint64_t parent_span_id) : ts({false}), trace_id(trace_id), parent_span_id(parent_span_id) {
  DEBUGHINDSIGHT(
    std::cout << "New HindsightTraceState " << trace_id << std::endl;
  )
  tracestate_begin_with_sampling(&ts, mgr, trace_id, hindsight.config._head_sampling_threshold, hindsight.config._retroactive_sampling_threshold);
  if (ts.head_sampled) {
    triggers_fire(&hindsight.triggers, TRIGGER_ID_HEAD_BASED_SAMPLING, trace_id, trace_id);
  }
}

HindsightTraceState::~HindsightTraceState() {
  tracestate_end(&ts, mgr);
}

void HindsightTraceState::ReportBreadcrumb(const std::string &breadcrumb) {
  DEBUGHINDSIGHT(
    std::cout << "ReportBreadcrumb " << breadcrumb << std::endl;
  )
  // std::cout << "Reporting breadcrumb " << breadcrumb << std::endl;
  breadcrumbs_add(&hindsight.breadcrumbs, ts.header.trace_id, breadcrumb.c_str());
}

void HindsightTraceState::Trigger(int queue_id) {
  hindsight_trigger_manual(trace_id, queue_id);
}

bool HindsightTraceState::Recording() {
  return ts.recording;
}

void HindsightTraceState::LogSpanStart(uint64_t span_id) {
  DEBUGHINDSIGHT(
    std::cout << "LogSpanStart " << span_id << std::endl;
  )
  Event e{EventType::kSpanStart, span_id, ticks(), 0};
  WriteEvent(e);
}

void HindsightTraceState::LogSpanEnd(uint64_t span_id) {
  DEBUGHINDSIGHT(
    std::cout << "LogSpanEnd " << span_id << std::endl;
  )
  Event e{EventType::kSpanEnd, span_id, ticks(), 0};
  WriteEvent(e);
}

void HindsightTraceState::LogSpanName(uint64_t span_id, const std::string &name) {
  DEBUGHINDSIGHT(
    std::cout << "LogSpanName " << span_id << " " << name << std::endl;
  )
  Event e{EventType::kSpanName, span_id, 0, name.size()};
  WriteEvent(e, (char*) name.c_str());
}

void HindsightTraceState::LogSpanParent(uint64_t span_id, uint64_t parent_id) {
  DEBUGHINDSIGHT(
    std::cout << "LogSpanParent " << span_id << " " << parent_id << std::endl;
  )
  Event e{EventType::kSpanParent, span_id, 0, sizeof(uint64_t)};
  WriteEvent(e, (char*) &parent_id);
}

void HindsightTraceState::LogSpanAttribute(uint64_t span_id, const std::string &key, const common::AttributeValue &value) {
  DEBUGHINDSIGHT(
    std::cout << "LogSpanAttribute " << span_id << " " << key << std::endl;
  )

  if (key == "Breadcrumb" && nostd::holds_alternative<nostd::string_view>(value)) {
    const nostd::string_view &breadcrumb = nostd::get<nostd::string_view>(value);
    char copy[breadcrumb.size()+1];
    strcpy(copy, (char*) breadcrumb.data());
    ReportBreadcrumb(std::string(copy, breadcrumb.size()));
  }

  if (key == "Trigger" && nostd::holds_alternative<int>(value)) {
    const int &queue_id = nostd::get<int>(value);
    Trigger(queue_id);
  }

  Event ek{EventType::kAttributeKey, span_id, 0, key.size()};
  WriteEvent(ek, (char*) key.c_str());

  Event ev{EventType::kAttributeValue, span_id, 0, 0};
  LogAttribute(ev, value);
}

void HindsightTraceState::LogSpanAttributeStr(uint64_t span_id, const std::string &key, const std::string &value) {
  DEBUGHINDSIGHT(
    std::cout << "LogSpanAttributeStr " << span_id << " " << key << " " << value << std::endl;
  )

  if (key == "Breadcrumb") {
    ReportBreadcrumb(value);
  }

  Event ek{EventType::kAttributeKey, span_id, 0, key.size()};
  WriteEvent(ek, (char*) key.c_str());

  Event ev{EventType::kAttributeValue, span_id, 0, value.size()};
  WriteEvent(ev, (char*) value.c_str());
}

void HindsightTraceState::LogSpanEvent(uint64_t span_id, const std::string &name) {
  DEBUGHINDSIGHT(
    std::cout << "LogSpanEvent " << span_id << " " << name << std::endl;
  )
  Event e{EventType::kEvent, span_id, ticks(), name.size()};
  WriteEvent(e, (char*) name.c_str());
}

void HindsightTraceState::LogSpanEventAttribute(uint64_t span_id, const std::string &key, const common::AttributeValue &value) {
  DEBUGHINDSIGHT(
    std::cout << "LogSpanEventAttribute " << span_id << " " << key << std::endl;
  )
  Event ek{EventType::kEventAttributeKey, span_id, 0, key.size()};
  WriteEvent(ek, (char*) key.c_str());

  Event ev{EventType::kEventAttributeValue, span_id, 0, 0};
  LogAttribute(ev, value);
}

void HindsightTraceState::LogSpanStatus(uint64_t span_id, int status, const std::string &description) {
  DEBUGHINDSIGHT(
    std::cout << "LogSpanStatus " << span_id << " " << status << " " << description << std::endl;
  )
  Event es{EventType::kStatus, span_id, ticks(), sizeof(status)};
  WriteEvent(es, (char*) &status);

  Event ed{EventType::kStatusDescription, span_id, 0, description.size()};
  WriteEvent(ed, (char*) description.c_str());
}  

void HindsightTraceState::LogSpanKind(uint64_t span_id, int spankind) {
  DEBUGHINDSIGHT(
    std::cout << "LogSpanKind " << span_id << " " << spankind << std::endl;
  )
  Event e{EventType::kSpanKind, span_id, 0, sizeof(int)};
  WriteEvent(e, (char*) &spankind);
}

void HindsightTraceState::LogTracer(uint64_t span_id, const std::string &tracer) {
  DEBUGHINDSIGHT(
    std::cout << "LogTracer " << span_id << " " << tracer << std::endl;
  )
  Event e{EventType::kTracer, span_id, 0, tracer.size()};
  WriteEvent(e, (char*) tracer.c_str());
}  

// OpenTelemetry uses a variant type for attribute values,
// which means we have to handle all possible types
void HindsightTraceState::LogAttribute(Event &e, const common::AttributeValue &value) {
  if (nostd::holds_alternative<bool>(value)) {
    const bool &v = nostd::get<bool>(value);
    e.size = sizeof(bool);
    WriteEvent(e, (char*) &v);
  } else if (nostd::holds_alternative<int>(value)) {
    const int &v = nostd::get<int>(value);
    e.size = sizeof(int);
    WriteEvent(e, (char*) &v);
  } else if (nostd::holds_alternative<int64_t>(value)) {
    const int64_t &v = nostd::get<int64_t>(value);
    e.size = sizeof(int64_t);
    WriteEvent(e, (char*) &v);
  } else if (nostd::holds_alternative<unsigned int>(value)) {
    const unsigned int &v = nostd::get<unsigned int>(value);
    e.size = sizeof(unsigned int);
    WriteEvent(e, (char*) &v);
  } else if (nostd::holds_alternative<uint64_t>(value)) {
    const uint64_t &v = nostd::get<uint64_t>(value);
    e.size = sizeof(uint64_t);
    WriteEvent(e, (char*) &v);
  } else if (nostd::holds_alternative<double>(value)) {
    const double &v = nostd::get<double>(value);
    e.size = sizeof(double);
    WriteEvent(e, (char*) &v);
  } else if (nostd::holds_alternative<const char *>(value)) {
    const char* v = nostd::get<const char *>(value);
    std::string stringv = std::string(v);
    e.size = stringv.size();
    WriteEvent(e, (char*) v);
  } else if (nostd::holds_alternative<nostd::string_view>(value)) {
    const nostd::string_view &v = nostd::get<nostd::string_view>(value);
    e.size = v.size();
    WriteEvent(e, (char*) v.data());
  } else if (nostd::holds_alternative<nostd::span<const uint8_t>>(value)) {
    const nostd::span<const uint8_t> &v = nostd::get<nostd::span<const uint8_t>>(value);
    e.size = v.size() * sizeof(const uint8_t);
    WriteEvent(e, (char*) v.data());
  } else if (nostd::holds_alternative<nostd::span<const bool>>(value)) {
    const nostd::span<const bool> &v = nostd::get<nostd::span<const bool>>(value);
    e.size = v.size() * sizeof(const bool);
    WriteEvent(e, (char*) v.data());
  } else if (nostd::holds_alternative<nostd::span<const int>>(value)) {
    const nostd::span<const int> &v = nostd::get<nostd::span<const int>>(value);
    e.size = v.size() * sizeof(const int);
    WriteEvent(e, (char*) v.data());
  } else if (nostd::holds_alternative<nostd::span<const int64_t>>(value)) {
    const nostd::span<const int64_t> &v = nostd::get<nostd::span<const int64_t>>(value);
    e.size = v.size() * sizeof(const int64_t);
    WriteEvent(e, (char*) v.data());
  } else if (nostd::holds_alternative<nostd::span<const unsigned int>>(value)) {
    const nostd::span<const unsigned int> &v = nostd::get<nostd::span<const unsigned int>>(value);
    e.size = v.size() * sizeof(const unsigned int);
    WriteEvent(e, (char*) v.data());
  } else if (nostd::holds_alternative<nostd::span<const uint64_t>>(value)) {
    const nostd::span<const uint64_t> &v = nostd::get<nostd::span<const uint64_t>>(value);
    e.size = v.size() * sizeof(const uint64_t);
    WriteEvent(e, (char*) v.data());
  } else if (nostd::holds_alternative<nostd::span<const double>>(value)) {
    const nostd::span<const double> &v = nostd::get<nostd::span<const double>>(value);
    e.size = v.size() * sizeof(const double);
    WriteEvent(e, (char*) v.data());
  } else if (nostd::holds_alternative<nostd::span<const nostd::string_view>>(value)) {
    for (const auto &v : nostd::get<nostd::span<const nostd::string_view>>(value)) {
      e.size = v.size();
      WriteEvent(e, (char*) v.data());
    }
  }
}

// Write an event that has no payload
void HindsightTraceState::WriteEvent(Event &e) {
  char* eptr = (char*) &e;
  size_t esz = sizeof(Event);

  if (!tracestate_try_write(&ts, eptr, esz)) {
    tracestate_write(&ts, mgr, eptr, esz);
  }
}

// Write an event with a payload.  e.size must
// be the payload size
void HindsightTraceState::WriteEvent(Event &e, char* payload) {
  WriteEvent(e);

  if (e.size > 0) {
    if (!tracestate_try_write(&ts, payload, e.size)) {
      tracestate_write(&ts, mgr, payload, e.size);
    }
  }
}