/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#include "hindsight_opentelemetry.h"
#include "grpc_propagation.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/trace/context.h"
#include "opentelemetry/trace/trace_state.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/noop.h"
#include "opentelemetry/sdk/trace/random_id_generator.h"

extern "C" {
  #include "hindsight.h"
}

namespace hindsightgrpc {

namespace nostd = opentelemetry::nostd;

using opentelemetry::context::Context;
using opentelemetry::trace::TraceState;
using opentelemetry::trace::Tracer;
using opentelemetry::trace::NoopSpan;
using opentelemetry::trace::TracerProvider;
using opentelemetry::trace::Span;
using opentelemetry::trace::SpanContext;
using opentelemetry::common::KeyValueIterable;
using opentelemetry::trace::SpanContextKeyValueIterable;
using opentelemetry::trace::StartSpanOptions;
using opentelemetry::sdk::trace::RandomIdGenerator;

void initHindsight(std::string service_name, std::string breadcrumb) {

  // Manually set the breadcrumb in the hindsight config
  HindsightConfig cfg = hindsight_load_config(service_name.c_str());
  cfg.address = (char*) malloc(sizeof(char) * (breadcrumb.size() + 1));
  breadcrumb.copy(cfg.address, breadcrumb.size());
  cfg.address[breadcrumb.size()] = '\0';
  hindsight_init_with_config(service_name.c_str(), cfg);
}

/*
Initializes OpenTelemetry to use the Hindsight tracer
*/
void initHindsightOpenTelemetry(std::string service_name, std::string breadcrumb) {
  std::cout << "Initializing Hindsight OpenTelemetry" << std::endl;

  initHindsight(service_name, breadcrumb);
  
  // Hindsight provides the tracer
  auto provider = nostd::shared_ptr<TracerProvider>(new HindsightTracerProvider());
  opentelemetry::trace::Provider::SetTracerProvider(provider);

  // Set the global propagator
  initGrpcPropagation();

  // for (int i = 0; i < 10; i++) {
  //   HindsightTraceState s = HindsightTraceState(i+10);
  //   for (int span_id = 10; span_id < 15; span_id++) {
  //     s.LogSpanStart(span_id);
  //     s.LogSpanName(span_id, "hello world");
  //     s.LogSpanParent(span_id, 0);
  //     s.LogSpanAttribute(span_id, "key", "value");
  //     for (int j = 0; j < 5; j++) {
  //       s.LogSpanEvent(span_id, "interesting event");
  //       s.LogSpanEventAttribute(span_id, "event key", "event value");
  //     }
  //     s.LogSpanStatus(span_id, 0, "good");
  //     s.LogSpanKind(span_id, 0);
  //     s.LogSpanEnd(span_id);
  //   }
  // }
}

nostd::shared_ptr<Tracer> HindsightTracerProvider::GetTracer(nostd::string_view library_name,
                                            nostd::string_view library_version,
                                            nostd::string_view schema_url) {
  auto p = std::make_shared<HindsightTracer>(library_name);
  return nostd::shared_ptr<Tracer>(p);
}

HindsightTracer::HindsightTracer(nostd::string_view output) noexcept 
  : output(output), id_generator(new RandomIdGenerator()), local_address(std::string(hindsight_get_local_address())) {
    std::cout << "HindsightTracer using " << local_address << " for Hindsight breadcrumb" << std::endl;
  }

HindsightTracer::~HindsightTracer() {}

nostd::shared_ptr<Span> HindsightTracer::StartSpan(nostd::string_view name,
    const KeyValueIterable &attributes,
    const SpanContextKeyValueIterable &links,
    const StartSpanOptions &options) noexcept {

  if (nostd::holds_alternative<SpanContext>(options.parent)){
    auto span_context = nostd::get<SpanContext>(options.parent);
    if (span_context.IsValid()) {
      // We have an existing parent context, but it doesn't have a hindsight state
      return StartSpan(name, attributes, links, options, nullptr, span_context);
    }
  } else if (nostd::holds_alternative<Context>(options.parent)) {
    auto context = nostd::get<Context>(options.parent);
    auto span = opentelemetry::trace::GetSpan(context);
    
    if (auto hindsight_span = dynamic_cast<HindsightSpan*>(span.get())) {
      // We have a parent hindsight span
      auto parent_context = span->GetContext();
      return StartSpan(name, attributes, links, options, hindsight_span->hs_, parent_context);
    }

    auto span_context = span->GetContext();
    if (span_context.IsValid()) {
      // We have a valid non-hindsight parent
      return StartSpan(name, attributes, links, options, nullptr, span_context);
    }
  }


  nostd::shared_ptr<Span> current_span = GetCurrentSpan();
  auto current_span_context = current_span->GetContext();
  if (auto hindsight_span = dynamic_cast<HindsightSpan*>(current_span.get())) {
    // The current span is a hindsight span - use its tracestate
    return StartSpan(name, attributes, links, options, hindsight_span->hs_, current_span_context);
  } else {
    return StartSpan(name, attributes, links, options, nullptr, current_span_context);
  }
}

nostd::shared_ptr<Span> HindsightTracer::StartSpan(
      nostd::string_view name,
      const KeyValueIterable &attributes,
      const SpanContextKeyValueIterable &links,
      const StartSpanOptions &options,
      std::shared_ptr<HindsightTraceState> hindsight_ts,
      SpanContext &parent_context
      ) noexcept {

  trace_api::TraceId trace_id;
  trace_api::SpanId span_id = id_generator->GenerateSpanId();
  bool is_parent_span_valid = false;

  // std::cout << "Start span with parent: " << parent_context.trace_state()->ToHeader() << std::endl;

  if (parent_context.IsValid()) {
    trace_id             = parent_context.trace_id();
    is_parent_span_valid = true;
  } else {
    trace_id = id_generator->GenerateTraceId();
  }

  if (hindsight_ts == nullptr) {
    uint64_t tid = *((uint64_t*) trace_id.Id().data());
    uint64_t sid = *((uint64_t*) span_id.Id().data());
    hindsight_ts = std::make_shared<HindsightTraceState>(tid, sid);

    /* (jcmace) Disabling the code below but leaving it here.
    OpenTelemetry's trace_state is horrendously inefficient,
    using regex for checking if keys are valid.  We do breadcrumbs
    directly in server.cc instead. */
    if (false && is_parent_span_valid) {
      // The parent is a remote parent -- deal with its breadcrumb
      std::string breadcrumb;
      auto trace_state = parent_context.trace_state();
      bool has_breadcrumb = trace_state->Get("breadcrumb", breadcrumb);

      if (has_breadcrumb && breadcrumb != local_address) {
        hindsight_ts->ReportBreadcrumb(breadcrumb);
      }
    }
  }

  /* (jcmace) Disabling trace_state propagation of
  breadcrumbs due to OpenTelemetry's inefficient
  tracestate implementation.  We do breadcrumbs
  directly in server.cc instead */
  // auto trace_state = TraceState::GetDefault()->Set("breadcrumb", local_address);
  auto trace_state = TraceState::GetDefault();

  if (hindsight_ts->Recording()) {
    auto trace_flags = trace_api::TraceFlags{trace_api::TraceFlags::kIsSampled};

    auto span_context = std::unique_ptr<SpanContext>(
      new SpanContext(trace_id, span_id, trace_flags, false, trace_state));

    
    uint64_t sid = *((uint64_t*) span_id.Id().data());
    return nostd::shared_ptr<Span>{
      new (std::nothrow) HindsightSpan(output, name, attributes, links, options,
        parent_context, std::move(span_context), hindsight_ts)
    };
  } else {
    auto trace_flags = trace_api::TraceFlags{};

    auto span_context = std::unique_ptr<SpanContext>(
      new SpanContext(trace_id, span_id, trace_flags, false, trace_state));

    return nostd::shared_ptr<Span>{
      new (std::nothrow) NoopSpan(this->shared_from_this(), std::move(span_context))
    };
  }
}

HindsightSpan::HindsightSpan(nostd::string_view tracer_name,
              nostd::string_view name, 
              const KeyValueIterable &attributes, 
              const SpanContextKeyValueIterable &links, 
              const StartSpanOptions &options, 
              const trace_api::SpanContext &parent_span_context, 
              std::unique_ptr<trace_api::SpanContext> span_context,
              std::shared_ptr<HindsightTraceState> hindsight_ts) noexcept
    : 
              has_ended_(false),
              hs_(hindsight_ts),
              span_context_(std::move(span_context)) {
  span_id = *((uint64_t*) span_context_->span_id().Id().data());
  uint64_t parent_span_id = *((uint64_t*) parent_span_context.span_id().Id().data());

  if (hs_ == nullptr) {
    return;
  }

  hs_->LogSpanStart(span_id);
  hs_->LogSpanName(span_id, static_cast<std::string>(name));
  hs_->LogTracer(span_id, static_cast<std::string>(tracer_name));
  hs_->LogSpanParent(span_id, parent_span_id);

  attributes.ForEachKeyValue([&](nostd::string_view key, common::AttributeValue value) noexcept {
    hs_->LogSpanAttribute(span_id, static_cast<std::string>(key), value);
    return true;
  });

  hs_->LogSpanKind(span_id, (int) options.kind);

  // TODO: links not currently implemented
  // For example, see:
  //  https://github.com/open-telemetry/opentelemetry-cpp/blob/main/sdk/src/trace/span.cc 
}

HindsightSpan::~HindsightSpan() {
  End();
}

void HindsightSpan::SetAttribute(nostd::string_view key, const opentelemetry::common::AttributeValue &value) noexcept {
  if (hs_ == nullptr) return;
  hs_->LogSpanAttribute(span_id, static_cast<std::string>(key), value);
}

void HindsightSpan::AddEvent(nostd::string_view name) noexcept {
  if (hs_ == nullptr) return;
  hs_->LogSpanEvent(span_id, static_cast<std::string>(name));
}

void HindsightSpan::AddEvent(nostd::string_view name, opentelemetry::common::SystemTimestamp timestamp) noexcept {
  if (hs_ == nullptr) return;
  hs_->LogSpanEvent(span_id, static_cast<std::string>(name));
}

void HindsightSpan::AddEvent(nostd::string_view name, opentelemetry::common::SystemTimestamp timestamp, const opentelemetry::common::KeyValueIterable &attributes) noexcept {
  if (hs_ == nullptr) return;
  hs_->LogSpanEvent(span_id, static_cast<std::string>(name));
  attributes.ForEachKeyValue([&](nostd::string_view key, common::AttributeValue value) noexcept {
    hs_->LogSpanEventAttribute(span_id, static_cast<std::string>(key), value);
    return true;
  });
}

void HindsightSpan::SetStatus(opentelemetry::trace::StatusCode code, nostd::string_view description) noexcept {
  if (hs_ == nullptr) return;
  hs_->LogSpanStatus(span_id, (int) code, static_cast<std::string>(description));
}

void HindsightSpan::UpdateName(nostd::string_view name) noexcept {
  if (hs_ == nullptr) return;
  hs_->LogSpanName(span_id, static_cast<std::string>(name));
}

void HindsightSpan::End(const opentelemetry::trace::EndSpanOptions &options) noexcept {
  if (has_ended_ == true) {
    return;
  }
  has_ended_ = true;
  if (hs_ == nullptr) return;
  hs_->LogSpanEnd(span_id);
}

bool HindsightSpan::IsRecording() const noexcept {
  return hs_ != nullptr;
}

opentelemetry::trace::SpanContext HindsightSpan::GetContext() const noexcept {
  return *span_context_.get();
}

}  // namespace hindsightgrpc
