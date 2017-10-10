#include "common/upstream/grpc_health_checker_impl.h"

#include "common/grpc/async_client_impl.h"
#include "common/upstream/health.pb.h"

namespace Envoy {
namespace Upstream {

using RequestType = grpc::health::v1::HealthCheckRequest;
using ResponseType = grpc::health::v1::HealthCheckResponse;
using StreamType = Grpc::AsyncStream<RequestType>;

struct GrpcHealthCheckerImpl::Session : public ActiveHealthCheckSession,
                                        private Grpc::AsyncStreamCallbacks<ResponseType> {
public:
  Session(GrpcHealthCheckerImpl& parent, HostSharedPtr host)
      : ActiveHealthCheckSession(parent, host),
        request_([&]() {
            RequestType request;
            request.set_service(parent.service_name_);
            return request;
          }()) {
  }

  ~Session() override {
    if (stream_) {
      stream_->resetStream();
    }
  }

private:
  // ActiveHealthCheckSession
  void onInterval() override {
    if (!stream_) {
      // TODO(crafferty): create stream
      expect_close_ = false;
    }
    stream_->sendMessage(request_, false);
  }

  void onTimeout() override {
    resetStream();
  }

  // AsyncStreamCallbacks
  void onReceiveMessage(std::unique_ptr<ResponseType>&& message) override {
    if (message && message->status() == ResponseType::SERVING) {
      handleSuccess();
    } else {
      handleFailure(FailureType::Active);
    }
  }

  void onRemoteClose(Grpc::Status::GrpcStatus, const std::string&) override {
    if (expect_close_) {
      return;
    }
    handleFailure(FailureType::Network);
  }

  void onCreateInitialMetadata(Http::HeaderMap&) override {}
  void onReceiveInitialMetadata(Http::HeaderMapPtr&&) override {}
  void onReceiveTrailingMetadata(Http::HeaderMapPtr&&) override {}

  // internals
  void resetStream() {
    if (stream_) {
      expect_close_ = true;
      stream_->resetStream();
      stream_ = nullptr;
    }
  }

  const RequestType request_;
  StreamType* stream_{};
  bool expect_close_{};
};

GrpcHealthCheckerImpl::GrpcHealthCheckerImpl(const Cluster& cluster,
                                             const envoy::api::v2::HealthCheck& config,
                                             Event::Dispatcher& dispatcher,
                                             Runtime::Loader& runtime,
                                             Runtime::RandomGenerator& random)
    : HealthCheckerImplBase(cluster, config, dispatcher, runtime, random),
      service_name_(config.grpc_health_check().service_name()) {}

GrpcHealthCheckerImpl::~GrpcHealthCheckerImpl() {
}

GrpcHealthCheckerImpl::ActiveHealthCheckSessionPtr GrpcHealthCheckerImpl::makeSession(HostSharedPtr host) {
  return ActiveHealthCheckSessionPtr{new Session(*this, host)};
}

} // namespace Upstream
} // namespace Envoy
