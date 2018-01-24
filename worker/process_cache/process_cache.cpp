#include <process_cache.hpp>
#include <utility>
#include <glog/logging.h>
#include <list>
#include <vector>

namespace flexps {

bool SSPChunkRequestMgr::AddChunkRequest(ChunkRequestInfo &request, int32_t model_id, int32_t chunk_id) {
  request.sent = true;
  {
    std::pair<int32_t, int32_t> request_key(model_id, chunk_id);
    if (pending_row_requests_.count(request_key) == 0) {
      pending_row_requests_.insert(std::make_pair(request_key,
        std::list<ChunkRequestInfo>()));
      VLOG(0) << "pending row requests does not have this model_id = "
	      << model_id << " chunk_id = " << chunk_id;
    }
    std::list<ChunkRequestInfo> &request_list
      = pending_row_requests_[request_key];
    bool request_added = false;
    // Requests are sorted in increasing order of clock number.
    // When a request is to be inserted, start from the end as the requst's
    // clock is more likely to be larger.
    for (auto iter = request_list.end(); iter != request_list.begin(); iter--) {
      auto iter_prev = std::prev(iter);
      int32_t clock = request.clock;
      if (clock >= iter_prev->clock) {
	      LOG(INFO) << "I'm requesting clock is " << clock
		    << " There's a previous request requesting clock "
		    << iter_prev->clock;
        // insert before iter
	      request.sent = false;
        request_list.insert(iter, request);
        request_added = true;
        break;
      }
    }
    if (!request_added)
      request_list.push_front(request);
  }


  return request.sent;
}

int32_t SSPChunkRequestMgr::InformReply(int32_t model_id, int32_t row_id,
  int32_t clock, std::vector<int32_t> *app_thread_ids) {
  (*app_thread_ids).clear();
  std::pair<int32_t, int32_t> request_key(model_id, row_id);
  std::list<ChunkRequestInfo> &request_list = pending_row_requests_[request_key];
  int32_t clock_to_request = -1;

  while (!request_list.empty()) {
    ChunkRequestInfo &request = request_list.front();
    if (request.clock <= clock) {
      app_thread_ids->push_back(request.app_thread_id);
      request_list.pop_front();
    } 
    else {
      if (!request.sent) {
	      clock_to_request = request.clock;
	      request.sent = true;
      }
      break;
    }
  }
  // if there's no request in that list, I can remove the empty list
  if (request_list.empty())
    pending_row_requests_.erase(request_key);
  return clock_to_request;
}

}  // namespace petuum