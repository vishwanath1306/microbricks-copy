/*
 * Copyright 2022 Max Planck Institute for Software Systems *
 */

#pragma once
#ifndef SRC_TRACING_GRPC_PROPAGATION_H_
#define SRC_TRACING_GRPC_PROPAGATION_H_

#include <grpcpp/grpcpp.h>
#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/nostd/string_view.h"

namespace hindsightgrpc {

namespace nostd = opentelemetry::nostd;
namespace propagation = opentelemetry::context::propagation;

using grpc::ClientContext;
using grpc::ServerContext;
using opentelemetry::context::propagation::TextMapCarrier;


void initGrpcPropagation();

/* Propagates string KV pairs inside gRPC's context.  Client-side part. */
class GrpcClientCarrier : public TextMapCarrier {
public:
  GrpcClientCarrier(ClientContext *context);
  GrpcClientCarrier() = default;
  
  nostd::string_view Get(nostd::string_view key) const noexcept override;
  virtual void Set(nostd::string_view key, nostd::string_view value) noexcept override;

  ClientContext *context_;
};


/* Propagates string KV pairs inside gRPC's context.  Server-side part. */
class GrpcServerCarrier : public TextMapCarrier {
public:
  GrpcServerCarrier(ServerContext *context);
  GrpcServerCarrier() = default;

  nostd::string_view Get(nostd::string_view key) const noexcept override;

  virtual void Set(nostd::string_view key, nostd::string_view value) noexcept override;

  ServerContext *context_;
};


} // namespace hindsightgrpc

#endif  // SRC_TRACING_GRPC_PROPAGATION_H_