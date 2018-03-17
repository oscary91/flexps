#include "worker/process_cache/cached_blocker.hpp"

#include "glog/logging.h"

namespace flexps {

void CachedBlocker::UpdateProcessCache(Message& msg, uint32_t model_id){
  KVPairs<float> kvs;
  kvs.keys = msg.data[0];
  kvs.vals = msg.data[1];
  int clock = msg.meta.version;
  
  auto chunk_size = kvs.vals.size() / kvs.keys.size();
  
  int idx = 0;
  for (auto chunk_id: kvs.keys){
    std::vector<float> chunk(chunk_size);
    memcpy(chunk.data(), kvs.vals.data()+(idx * chunk_size), chunk_size*sizeof(float));
    process_storage_->Insert(chunk_id, chunk, clock); 
    idx++;
        
    std::vector<int> app_thread_ids;
    chunk_request_mgr_.InformReply(model_id, chunk_id, clock, &app_thread_ids);
//    for (auto app_id: app_thread_ids)
//       LOG(INFO) << "app_id: " << app_id;
    for (auto app_id: app_thread_ids)
    {
      bool recv_finish = false; 
      {
        std::lock_guard<std::mutex> lk(mu_);
        recv_finish =
            chunk_tracker_[app_id][model_id].first == chunk_tracker_[app_id][model_id].second + 1 ? true : false;
//        LOG(INFO) << "first: " << chunk_tracker_[app_id][model_id].first;
//        LOG(INFO) << "second: " << chunk_tracker_[app_id][model_id].second;
      }
      if (recv_finish) {
        recv_finish_handle_[app_id][model_id]();
      }
      {
        std::lock_guard<std::mutex> lk(mu_);
        chunk_tracker_[app_id][model_id].second += 1;
        if (recv_finish) {
          cond_.notify_all();
        }
      }
    } 
  }
}

int32_t CachedBlocker::GetClock(const uint32_t app_thread_id, const uint32_t model_id)
{
  std::lock_guard<std::mutex> clock_lk(clock_mu_);
  auto it1 = clock_recorder_.find(app_thread_id);
  if (it1 == clock_recorder_.end())
    return -1;
  auto it2 = it1->second.find(model_id);
  if (it2 == it1->second.end())
    return -1;
  return clock_recorder_[app_thread_id][model_id];
}

void CachedBlocker::RegisterProcessStorage(ProcessStorage<float>* const process_storage) {
  process_storage_ = process_storage;
}

void CachedBlocker::RegisterSenderqueue(ThreadsafeQueue<Message>* const send_queue) {
  send_queue_ = send_queue;
}

void CachedBlocker::NewChunkRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_chunks) {
  std::lock_guard<std::mutex> lk(mu_);
  SanityCheck(app_thread_id, model_id);
  chunk_tracker_[app_thread_id][model_id] = {expected_chunks, 0};
}

void CachedBlocker::WaitChunkRequest(uint32_t app_thread_id, uint32_t model_id) {
  std::unique_lock<std::mutex> lk(mu_);
  SanityCheck(app_thread_id, model_id);
  cond_.wait(lk, [this, app_thread_id, model_id] {
    return chunk_tracker_[app_thread_id][model_id].first == chunk_tracker_[app_thread_id][model_id].second;
  });
}

void CachedBlocker::AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) {
  switch (msg.meta.flag)
  {
    case Flag::kGetReply:
    {
      bool recv_finish = false;
      {
        std::lock_guard<std::mutex> lk(mu_);
        recv_finish =
            tracker_[app_thread_id][model_id].first == tracker_[app_thread_id][model_id].second + 1 ? true : false;
      }
      recv_handle_[app_thread_id][model_id](msg);

      if (recv_finish) {
        recv_finish_handle_[app_thread_id][model_id]();
      }

      {
        std::lock_guard<std::mutex> lk(mu_);
        tracker_[app_thread_id][model_id].second += 1;
        if (recv_finish) {
          cond_.notify_all();
        }
      }
      break;
    }
    case Flag::kGetChunkReply:
    {
      UpdateProcessCache(msg, model_id);
      break;
    }
    case Flag::kGetChunk:
    {
      KVPairs<float> kvs;
      kvs.keys = msg.data[0];
      // TODO compute the clock in workerside
      int32_t clock = GetClock(app_thread_id, model_id); 
      third_party::SArray<Key> chunks_not_in_cache = process_storage_->FindChunkToUpdate(kvs.keys, clock);
      LOG(INFO) << "thread id: " << app_thread_id << " model id: " << model_id
                 << " clock: " << clock << " chunks_not_in_cache.size(): " << chunks_not_in_cache.size();
      if (chunks_not_in_cache.size() == 0)
      {
          // All requests have been satisfied by process cache
          LOG(INFO) << "All requests have been satisfied by process cache";
          std::lock_guard<std::mutex> lk(mu_);
          chunk_tracker_[app_thread_id][model_id].second = chunk_tracker_[app_thread_id][model_id].first;
          recv_finish_handle_[app_thread_id][model_id]();
          cond_.notify_all();
      }
      else
      {
          // Some requests have not been satisfied by process cache
          LOG(INFO) << "Some requests have not been satisfied by process cache";
          std::lock_guard<std::mutex> lk(mu_); 
          chunk_tracker_[app_thread_id][model_id].second += kvs.keys.size() - chunks_not_in_cache.size();
          ChunkRequestInfo chunk_request;
          chunk_request.app_thread_id = app_thread_id;
          chunk_request.clock = clock;
          chunk_request.sent = false;
          third_party::SArray<Key> chunks_to_request;
          for (auto chunk_id : chunks_not_in_cache) {
            bool send = chunk_request_mgr_.AddChunkRequest(chunk_request, model_id, chunk_id);
            if (send)
              chunks_to_request.push_back(chunk_id);
          }
          Message msg_to_send;
          msg_to_send.meta.sender = msg.meta.sender;
          msg_to_send.meta.recver = msg.meta.recver;
          msg_to_send.meta.model_id = msg.meta.model_id;
          msg_to_send.meta.flag = msg.meta.flag;
          msg_to_send.AddData(chunks_to_request);
          send_queue_->Push(msg_to_send);
      }
      break;
    }
    case Flag::kClock:
    {
      std::lock_guard<std::mutex> clock_lk(clock_mu_);
      clock_recorder_[msg.meta.sender][model_id]++;
      send_queue_->Push(std::move(msg));
      break;
    }
    default:
    {
      send_queue_->Push(std::move(msg));
      break;
    }
  }

}

template <typename C>
int32_t CachedBlocker::FindProcessStorage(const third_party::SArray<Key>& keys, std::vector<C*>& vals, size_t chunk_size){
  int start = 0;
  int num_chunk_found = 0;
  for (int i = 0 ; i <  keys.size(); i++){
    third_party::SArray<C> chunk_vals = process_storage_->Find(keys[i]);
    if (chunk_vals.size() != 0)
      num_chunk_found++;
    memcpy(vals[i]->data(), chunk_vals.data()+start, chunk_size*sizeof(C));
    start += chunk_size;
  }
  
  CHECK_EQ(num_chunk_found, keys.size());
  return keys.size();
}

}  // namespace flexps
