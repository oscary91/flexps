#pragma once

#include <cinttypes>
#include <funtional>
#include <map>
#include <vector>

#include "driver/worker_spec.hpp"

namespace flexps {

class MLTask {
 public:
  void SetLambda(const std::function<void(const Info&)>& func) {
    func_ = func;
  }
  void RunLambda(const Info& info) const {
    func_(info);
  }
  void SetWorkerSpec(const WorkerSpec& worker_spec) {
    worker_spec_ = worker_spec;
  }
  const WorkerSpec& GetWorkerSpec() const {
    return worker_spec_;
  }
 private:
  std::function<void(const Info&)> func_;
  WorkerSpec worker_spec_;
};

}  // namespace flexps