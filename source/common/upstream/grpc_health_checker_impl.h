#pragma once

#include "common/upstream/health_checker_impl.h"

namespace Envoy {
namespace Upstream {

/**
 * Grpc health checker implementation.
 */
class GrpcHealthCheckerImpl : public HealthCheckerImplBase {
public:
  GrpcHealthCheckerImpl(const Cluster& cluster, const envoy::api::v2::HealthCheck& config,
                        Event::Dispatcher& dispatcher, Runtime::Loader& runtime,
                        Runtime::RandomGenerator& random);
  ~GrpcHealthCheckerImpl() override;

private:
  struct Session;

  ActiveHealthCheckSessionPtr makeSession(HostSharedPtr host) override;

  const std::string service_name_;
};

} // namespace Upstream
} // namespace Envoy
