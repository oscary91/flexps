#pragma once

#include <condition_variable>
#include <map>
#include <mutex>
#include <vector>

#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "worker/app_blocker.hpp"
#include "worker/process_cache/process_storage.hpp"
#include "worker/process_cache/process_cache.hpp"
#include "worker/kvpairs.hpp"
#include "glog/logging.h"

namespace flexps {

/*
 * Thread-safe.
 *
 * App threads block on AppBlocker by WaitRequest().
 * WorkerThread calls AddResponse() and run the callbacks.
 * Add response will work on sending and receiving message
 *
 * Should register sendqueue to server before sending message

 */
class CachedBlocker : public AppBlocker{
 public:
  CachedBlocker() : chunk_request_mgr_(), ps_cache_() {};

  virtual void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) override;
  void RegisterSenderqueue(ThreadsafeQueue<Message>* const send_queue);
  void UpdateProcessCache(Message& msg, uint32_t model_id);

 private:
  SSPChunkRequestMgr chunk_request_mgr_;
  ProcessStorage<char> ps_cache_;
  ThreadsafeQueue<Message>* send_queue_;

  // app_thread_id, model_id, clock
  std::map<uint32_t, std::map<uint32_t, int32_t>> clock_recorder_; 
};

}  // namespace flexps

