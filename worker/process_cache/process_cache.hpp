#pragma once

#include <map>
#include <vector>
#include <list>
#include <utility>
#include <boost/noncopyable.hpp>
#include <glog/logging.h>

//#include <petuum_ps/thread/bg_oplog.hpp>

namespace flexps {

struct ChunkRequestInfo {
public:
  int32_t app_thread_id;
  int32_t clock;
  bool sent;

  ChunkRequestInfo() :
    app_thread_id(0),
    clock(0),
    sent(0) {};

  ChunkRequestInfo(const ChunkRequestInfo & other):
    app_thread_id(other.app_thread_id),
    clock(other.clock),
    sent(other.sent) {};
  ChunkRequestInfo & operator=(const ChunkRequestInfo & other) {
    app_thread_id = other.app_thread_id;
    clock = other.clock;
    return *this;
  }
};




class SSPChunkRequestMgr {
public:
  SSPChunkRequestMgr() {};

  ~SSPChunkRequestMgr() {};


  bool AddChunkRequest(ChunkRequestInfo &request, int32_t model_id, int32_t chunk_id);

  // Get a list of app thread ids that can be satisfied with this reply.
  // Corresponding row requests are removed upon returning.
  // If all row requests prior to some version are removed, those OpLogs are
  // removed as well.
  int32_t InformReply(int32_t model_id, int32_t chunk_id, int32_t clock, std::vector<int32_t> *app_thread_ids);



private:


  // map <model_id, chunk_id> to a list of requests
  // The list is in increasing order of clock.
  std::map<std::pair<int32_t, int32_t>,
    std::list<ChunkRequestInfo> > pending_row_requests_;


};

}  // namespace flexps
