/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#include "grpc_propagation.h"

#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"

namespace hindsightgrpc {

namespace nostd = opentelemetry::nostd;
namespace propagation = opentelemetry::context::propagation;

using grpc::ClientContext;
using grpc::ServerContext;
using opentelemetry::context::propagation::TextMapCarrier;
using opentelemetry::context::propagation::GlobalTextMapPropagator;
using opentelemetry::context::propagation::TextMapPropagator;
using opentelemetry::trace::propagation::HttpTraceContext;

GrpcClientCarrier::GrpcClientCarrier(ClientContext *context) : context_(context) {}

nostd::string_view GrpcClientCarrier::Get(nostd::string_view key) const noexcept {
  return "";
}

void GrpcClientCarrier::Set(nostd::string_view key, nostd::string_view value) noexcept {
  context_->AddMetadata(key.data(), value.data());
}

GrpcServerCarrier::GrpcServerCarrier(ServerContext *context) : context_(context) {}

nostd::string_view GrpcServerCarrier::Get(nostd::string_view key) const noexcept {
  auto it = context_->client_metadata().find(key.data());
  if (it != context_->client_metadata().end()) {
    return it->second.data();
  }
  return "";
}

void GrpcServerCarrier::Set(nostd::string_view key, nostd::string_view value) noexcept {
  // Not required for server
}

void initGrpcPropagation() {
  // Set the global propagator
  propagation::GlobalTextMapPropagator::SetGlobalPropagator(
          nostd::shared_ptr<TextMapPropagator>(new HttpTraceContext()));
}

} // namespace hindsightgrpc