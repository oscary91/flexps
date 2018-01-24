#pragma once

#include <functional>
#include <boost/noncopyable.hpp>
#include <base/third_party/sarray.h>

namespace flexps {

template <typename Val>
class ProcessStorage {
public:
  struct ChunkInfo {
    third_party::SArray<Val> chunk; 
    int clock;
  }
  ProcessStorage();

  ~ProcessStorage();

  template <typename Val>
  std::vector<Val> Find(int32_t chunk_id);
  bool Insert(int32_t chunk_id, std::vector<Val>& chunk);
  std::vector<int32_t> FindChunkToUpdate(std::vector<int> chunk_ids, int min_clock)
private:
  //chunk_id -> clock
  std::map<int, int> chunks_clock;
  std::map<int, ChunkInfo> cached_kvs;
  uint32_t chunk_size_;
};

}  // namespace flexps