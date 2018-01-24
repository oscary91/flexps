#pragma once

#include <condition_variable>
#include <map>
#include <mutex>
#include <vector>

#include "base/threadsafe_queue.hpp"
#include "base/message.hpp"
#include "worker/abstract_callback_runner.hpp"
#include "worker/abstract_receiver.hpp"
#include "worker/process_storage.hpp"

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

  virtual void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) override;
  void RegisterSenderqueue(ThreadsafeQueue<Message>* const send_queue);

 private:
  SSPChunkRequestMgr chunk_requst_mgr_;
  ProcessStorage ps_cache_;
  ThreadsafeQueue<Message>* send_queue_;
};

}  // namespace flexps