#include "common/upstream/grpc_health_checker_impl.h"

namespace Envoy {
namespace Upstream {

struct GrpcHealthCheckerImpl::Session : public ActiveHealthCheckSession {
public:
  Session(HealthCheckerImplBase& parent, HostSharedPtr host)
      : ActiveHealthCheckSession(parent, host) {
  }

  ~Session() override {
  }

private:
  void onInterval() override {
    // send a ping
  }

  void onTimeout() override {
    // cancel the outstanding ping
  }
};

GrpcHealthCheckerImpl::GrpcHealthCheckerImpl(const Cluster& cluster,
                                             const envoy::api::v2::HealthCheck& config,
                                             Event::Dispatcher& dispatcher,
                                             Runtime::Loader& runtime,
                                             Runtime::RandomGenerator& random)
    : HealthCheckerImplBase(cluster, config, dispatcher, runtime, random) {}

GrpcHealthCheckerImpl::~GrpcHealthCheckerImpl() {
}

GrpcHealthCheckerImpl::ActiveHealthCheckSessionPtr GrpcHealthCheckerImpl::makeSession(HostSharedPtr host) {
  return ActiveHealthCheckSessionPtr{new Session(*this, host)};
}

} // namespace Upstream
} // namespace Envoy
